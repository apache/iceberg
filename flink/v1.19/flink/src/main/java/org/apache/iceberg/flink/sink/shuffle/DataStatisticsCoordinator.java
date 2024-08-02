/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.flink.sink.shuffle;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataStatisticsCoordinator receives {@link StatisticsEvent} from {@link DataStatisticsOperator}
 * every subtask and then merge them together. Once aggregation for all subtasks data statistics
 * completes, DataStatisticsCoordinator will send the aggregated data statistics back to {@link
 * DataStatisticsOperator}. In the end a custom partitioner will distribute traffic based on the
 * aggregated data statistics to improve data clustering.
 */
@Internal
class DataStatisticsCoordinator implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

  private final String operatorName;
  private final OperatorCoordinator.Context context;
  private final Schema schema;
  private final SortOrder sortOrder;
  private final Comparator<StructLike> comparator;
  private final int downstreamParallelism;
  private final StatisticsType statisticsType;
  private final double closeFileCostWeightPercentage;

  private final ExecutorService coordinatorExecutor;
  private final SubtaskGateways subtaskGateways;
  private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
  private final TypeSerializer<CompletedStatistics> completedStatisticsSerializer;
  private final TypeSerializer<GlobalStatistics> globalStatisticsSerializer;

  private transient boolean started;
  private transient AggregatedStatisticsTracker aggregatedStatisticsTracker;
  private transient CompletedStatistics completedStatistics;
  private transient GlobalStatistics globalStatistics;

  DataStatisticsCoordinator(
      String operatorName,
      OperatorCoordinator.Context context,
      Schema schema,
      SortOrder sortOrder,
      int downstreamParallelism,
      StatisticsType statisticsType,
      double closeFileCostWeightPercentage) {
    this.operatorName = operatorName;
    this.context = context;
    this.schema = schema;
    this.sortOrder = sortOrder;
    this.comparator = Comparators.forType(SortKeyUtil.sortKeySchema(schema, sortOrder).asStruct());
    this.downstreamParallelism = downstreamParallelism;
    this.statisticsType = statisticsType;
    this.closeFileCostWeightPercentage = closeFileCostWeightPercentage;

    this.coordinatorThreadFactory =
        new CoordinatorExecutorThreadFactory(
            "DataStatisticsCoordinator-" + operatorName, context.getUserCodeClassloader());
    this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
    this.subtaskGateways = new SubtaskGateways(operatorName, context.currentParallelism());
    SortKeySerializer sortKeySerializer = new SortKeySerializer(schema, sortOrder);
    this.completedStatisticsSerializer = new CompletedStatisticsSerializer(sortKeySerializer);
    this.globalStatisticsSerializer = new GlobalStatisticsSerializer(sortKeySerializer);
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting data statistics coordinator: {}.", operatorName);
    this.started = true;

    // statistics are restored already in resetToCheckpoint() before start() called
    this.aggregatedStatisticsTracker =
        new AggregatedStatisticsTracker(
            operatorName,
            context.currentParallelism(),
            schema,
            sortOrder,
            downstreamParallelism,
            statisticsType,
            SketchUtil.COORDINATOR_SKETCH_SWITCH_THRESHOLD,
            completedStatistics);
  }

  @Override
  public void close() throws Exception {
    coordinatorExecutor.shutdown();
    this.aggregatedStatisticsTracker = null;
    this.started = false;
    LOG.info("Closed data statistics coordinator: {}.", operatorName);
  }

  @VisibleForTesting
  void callInCoordinatorThread(Callable<Void> callable, String errorMessage) {
    ensureStarted();
    // Ensure the task is done by the coordinator executor.
    if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
      try {
        Callable<Void> guardedCallable =
            () -> {
              try {
                return callable.call();
              } catch (Throwable t) {
                LOG.error(
                    "Uncaught Exception in data statistics coordinator: {} executor",
                    operatorName,
                    t);
                ExceptionUtils.rethrowException(t);
                return null;
              }
            };

        coordinatorExecutor.submit(guardedCallable).get();
      } catch (InterruptedException | ExecutionException e) {
        throw new FlinkRuntimeException(errorMessage, e);
      }
    } else {
      try {
        callable.call();
      } catch (Throwable t) {
        LOG.error(
            "Uncaught Exception in data statistics coordinator: {} executor", operatorName, t);
        throw new FlinkRuntimeException(errorMessage, t);
      }
    }
  }

  public void runInCoordinatorThread(Runnable runnable) {
    this.coordinatorExecutor.execute(
        new ThrowableCatchingRunnable(
            throwable ->
                this.coordinatorThreadFactory.uncaughtException(Thread.currentThread(), throwable),
            runnable));
  }

  private void runInCoordinatorThread(ThrowingRunnable<Throwable> action, String actionString) {
    ensureStarted();
    runInCoordinatorThread(
        () -> {
          try {
            action.run();
          } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            LOG.error(
                "Uncaught exception in the data statistics coordinator: {} while {}. Triggering job failover",
                operatorName,
                actionString,
                t);
            context.failJob(t);
          }
        });
  }

  private void ensureStarted() {
    Preconditions.checkState(started, "The coordinator of %s has not started yet.", operatorName);
  }

  private void handleDataStatisticRequest(int subtask, StatisticsEvent event) {
    CompletedStatistics maybeCompletedStatistics =
        aggregatedStatisticsTracker.updateAndCheckCompletion(subtask, event);

    if (maybeCompletedStatistics != null) {
      if (maybeCompletedStatistics.isEmpty()) {
        LOG.info(
            "Skip aggregated statistics for checkpoint {} as it is empty.", event.checkpointId());
      } else {
        LOG.info("Completed statistics aggregation for checkpoint {}", event.checkpointId());
        // completedStatistics contains the complete samples, which is needed to compute
        // the range bounds in globalStatistics if downstreamParallelism changed.
        this.completedStatistics = maybeCompletedStatistics;
        // globalStatistics only contains assignment calculated based on Map or Sketch statistics
        this.globalStatistics =
            globalStatistics(
                maybeCompletedStatistics,
                downstreamParallelism,
                comparator,
                closeFileCostWeightPercentage);
        sendGlobalStatisticsToSubtasks(globalStatistics);
      }
    }
  }

  private static GlobalStatistics globalStatistics(
      CompletedStatistics completedStatistics,
      int downstreamParallelism,
      Comparator<StructLike> comparator,
      double closeFileCostWeightPercentage) {
    if (completedStatistics.type() == StatisticsType.Sketch) {
      // range bound is a much smaller array compared to the complete samples.
      // It helps reduce the amount of data transfer from coordinator to operator subtasks.
      return GlobalStatistics.fromRangeBounds(
          completedStatistics.checkpointId(),
          SketchUtil.rangeBounds(
              downstreamParallelism, comparator, completedStatistics.keySamples()));
    } else {
      return GlobalStatistics.fromMapAssignment(
          completedStatistics.checkpointId(),
          MapAssignment.fromKeyFrequency(
              downstreamParallelism,
              completedStatistics.keyFrequency(),
              closeFileCostWeightPercentage,
              comparator));
    }
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void sendGlobalStatisticsToSubtasks(GlobalStatistics statistics) {
    runInCoordinatorThread(
        () -> {
          LOG.info(
              "Broadcast latest global statistics from checkpoint {} to all subtasks",
              statistics.checkpointId());
          // applyImmediately is set to false so that operator subtasks can
          // apply the change at checkpoint boundary
          StatisticsEvent statisticsEvent =
              StatisticsEvent.createGlobalStatisticsEvent(
                  statistics, globalStatisticsSerializer, false);
          for (int i = 0; i < context.currentParallelism(); ++i) {
            // Ignore future return value for potential error (e.g. subtask down).
            // Upon restart, subtasks send request to coordinator to refresh statistics
            // if there is any difference
            subtaskGateways.getSubtaskGateway(i).sendEvent(statisticsEvent);
          }
        },
        String.format(
            "Failed to send operator %s coordinator global data statistics for checkpoint %d",
            operatorName, statistics.checkpointId()));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void handleRequestGlobalStatisticsEvent(int subtask, RequestGlobalStatisticsEvent event) {
    if (globalStatistics != null) {
      runInCoordinatorThread(
          () -> {
            if (event.signature() != null && event.signature() != globalStatistics.hashCode()) {
              LOG.debug(
                  "Skip responding to statistics request from subtask {}, as hashCode matches or not included in the request",
                  subtask);
            } else {
              LOG.info(
                  "Send latest global statistics from checkpoint {} to subtask {}",
                  globalStatistics.checkpointId(),
                  subtask);
              StatisticsEvent statisticsEvent =
                  StatisticsEvent.createGlobalStatisticsEvent(
                      globalStatistics, globalStatisticsSerializer, true);
              subtaskGateways.getSubtaskGateway(subtask).sendEvent(statisticsEvent);
            }
          },
          String.format(
              "Failed to send operator %s coordinator global data statistics to requesting subtask %d for checkpoint %d",
              operatorName, subtask, globalStatistics.checkpointId()));
    } else {
      LOG.info(
          "Ignore global statistics request from subtask {} as statistics not available", subtask);
    }
  }

  @Override
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Handling event from subtask {} (#{}) of {}: {}",
              subtask,
              attemptNumber,
              operatorName,
              event);
          if (event instanceof StatisticsEvent) {
            handleDataStatisticRequest(subtask, ((StatisticsEvent) event));
          } else if (event instanceof RequestGlobalStatisticsEvent) {
            handleRequestGlobalStatisticsEvent(subtask, (RequestGlobalStatisticsEvent) event);
          } else {
            throw new IllegalArgumentException(
                "Invalid operator event type: " + event.getClass().getCanonicalName());
          }
        },
        String.format(
            "handling operator event %s from subtask %d (#%d)",
            event.getClass(), subtask, attemptNumber));
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Snapshotting data statistics coordinator {} for checkpoint {}",
              operatorName,
              checkpointId);
          if (completedStatistics == null) {
            // null checkpoint result is not allowed, hence supply an empty byte array
            resultFuture.complete(new byte[0]);
          } else {
            resultFuture.complete(
                StatisticsUtil.serializeCompletedStatistics(
                    completedStatistics, completedStatisticsSerializer));
          }
        },
        String.format("taking checkpoint %d", checkpointId));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
    Preconditions.checkState(
        !started, "The coordinator %s can only be reset if it was not yet started", operatorName);
    if (checkpointData == null || checkpointData.length == 0) {
      LOG.info(
          "Data statistic coordinator {} has nothing to restore from checkpoint {}",
          operatorName,
          checkpointId);
      return;
    }

    LOG.info(
        "Restoring data statistic coordinator {} from checkpoint {}", operatorName, checkpointId);
    this.completedStatistics =
        StatisticsUtil.deserializeCompletedStatistics(
            checkpointData, completedStatisticsSerializer);
    // recompute global statistics in case downstream parallelism changed
    this.globalStatistics =
        globalStatistics(
            completedStatistics, downstreamParallelism, comparator, closeFileCostWeightPercentage);
  }

  @Override
  public void subtaskReset(int subtask, long checkpointId) {
    runInCoordinatorThread(
        () -> {
          LOG.info(
              "Operator {} subtask {} is reset to checkpoint {}",
              operatorName,
              subtask,
              checkpointId);
          Preconditions.checkState(
              this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.reset(subtask);
        },
        String.format("handling subtask %d recovery to checkpoint %d", subtask, checkpointId));
  }

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
    runInCoordinatorThread(
        () -> {
          LOG.info(
              "Unregistering gateway after failure for subtask {} (#{}) of data statistics {}",
              subtask,
              attemptNumber,
              operatorName);
          Preconditions.checkState(
              this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.unregisterSubtaskGateway(subtask, attemptNumber);
        },
        String.format("handling subtask %d (#%d) failure", subtask, attemptNumber));
  }

  @Override
  public void executionAttemptReady(int subtask, int attemptNumber, SubtaskGateway gateway) {
    Preconditions.checkArgument(subtask == gateway.getSubtask());
    Preconditions.checkArgument(attemptNumber == gateway.getExecution().getAttemptNumber());
    runInCoordinatorThread(
        () -> {
          Preconditions.checkState(
              this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.registerSubtaskGateway(gateway);
        },
        String.format(
            "making event gateway to subtask %d (#%d) available", subtask, attemptNumber));
  }

  @VisibleForTesting
  CompletedStatistics completedStatistics() {
    return completedStatistics;
  }

  @VisibleForTesting
  GlobalStatistics globalStatistics() {
    return globalStatistics;
  }

  private static class SubtaskGateways {
    private final String operatorName;
    private final Map<Integer, SubtaskGateway>[] gateways;

    @SuppressWarnings("unchecked")
    private SubtaskGateways(String operatorName, int parallelism) {
      this.operatorName = operatorName;
      gateways = new Map[parallelism];

      for (int i = 0; i < parallelism; ++i) {
        gateways[i] = Maps.newHashMap();
      }
    }

    private void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
      int subtaskIndex = gateway.getSubtask();
      int attemptNumber = gateway.getExecution().getAttemptNumber();
      Preconditions.checkState(
          !gateways[subtaskIndex].containsKey(attemptNumber),
          "Coordinator of %s already has a subtask gateway for %d (#%d)",
          operatorName,
          subtaskIndex,
          attemptNumber);
      LOG.debug(
          "Coordinator of {} registers gateway for subtask {} attempt {}",
          operatorName,
          subtaskIndex,
          attemptNumber);
      gateways[subtaskIndex].put(attemptNumber, gateway);
    }

    private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      LOG.debug(
          "Coordinator of {} unregisters gateway for subtask {} attempt {}",
          operatorName,
          subtaskIndex,
          attemptNumber);
      gateways[subtaskIndex].remove(attemptNumber);
    }

    private OperatorCoordinator.SubtaskGateway getSubtaskGateway(int subtaskIndex) {
      Preconditions.checkState(
          !gateways[subtaskIndex].isEmpty(),
          "Coordinator of %s subtask %d is not ready yet to receive events",
          operatorName,
          subtaskIndex);
      return Iterables.getOnlyElement(gateways[subtaskIndex].values());
    }

    private void reset(int subtaskIndex) {
      gateways[subtaskIndex].clear();
    }
  }

  private static class CoordinatorExecutorThreadFactory
      implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private final String coordinatorThreadName;
    private final ClassLoader classLoader;
    private final Thread.UncaughtExceptionHandler errorHandler;

    @javax.annotation.Nullable private Thread thread;

    CoordinatorExecutorThreadFactory(
        final String coordinatorThreadName, final ClassLoader contextClassLoader) {
      this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
    }

    @org.apache.flink.annotation.VisibleForTesting
    CoordinatorExecutorThreadFactory(
        final String coordinatorThreadName,
        final ClassLoader contextClassLoader,
        final Thread.UncaughtExceptionHandler errorHandler) {
      this.coordinatorThreadName = coordinatorThreadName;
      this.classLoader = contextClassLoader;
      this.errorHandler = errorHandler;
    }

    @Override
    public synchronized Thread newThread(@NotNull Runnable runnable) {
      thread = new Thread(runnable, coordinatorThreadName);
      thread.setContextClassLoader(classLoader);
      thread.setUncaughtExceptionHandler(this);
      return thread;
    }

    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
      errorHandler.uncaughtException(t, e);
    }

    boolean isCurrentThreadCoordinatorThread() {
      return Thread.currentThread() == thread;
    }
  }
}
