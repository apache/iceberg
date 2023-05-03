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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.function.ThrowingRunnable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DataStatisticsCoordinator receives {@link DataStatisticsEvent} from {@link
 * DataStatisticsOperator} every subtask and then merge them together. Once aggregation for all
 * subtasks data statistics completes, DataStatisticsCoordinator will send the aggregated
 * result(global data statistics) back to {@link DataStatisticsOperator}. In the end a custom
 * partitioner will distribute traffic based on the global data statistics to improve data
 * clustering.
 */
@Internal
class DataStatisticsCoordinator<K> implements OperatorCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinator.class);

  private final String operatorName;
  private final ExecutorService coordinatorExecutor;
  private final OperatorCoordinator.Context operatorCoordinatorContext;
  private final SubtaskGateways subtaskGateways;
  private final DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory
      coordinatorThreadFactory;
  private final GlobalStatisticsAggregatorTracker<K> globalStatisticsAggregatorTracker;

  private volatile boolean started;

  DataStatisticsCoordinator(
      String operatorName,
      OperatorCoordinator.Context context,
      DataStatisticsFactory<K> statisticsFactory) {
    this.operatorName = operatorName;
    this.coordinatorThreadFactory =
        new DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory(
            "DataStatisticsCoordinator-" + operatorName, context.getUserCodeClassloader());
    this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
    this.operatorCoordinatorContext = context;
    this.subtaskGateways = new SubtaskGateways(parallelism());
    this.globalStatisticsAggregatorTracker =
        new GlobalStatisticsAggregatorTracker<>(statisticsFactory, parallelism());
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting data statistics coordinator for {}.", operatorName);
    started = true;
  }

  @Override
  public void close() throws Exception {
    LOG.info("Closing data statistics coordinator for {}.", operatorName);
    coordinatorExecutor.shutdown();
    try {
      if (!coordinatorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
        LOG.warn(
            "Fail to shut down data statistics coordinator context gracefully. Shutting down now");
        coordinatorExecutor.shutdownNow();
        if (!coordinatorExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
          LOG.warn("Fail to terminate data statistics coordinator context");
          return;
        }
      }
      LOG.info("Data statistics coordinator context closed.");
    } catch (InterruptedException e) {
      coordinatorExecutor.shutdownNow();
      Thread.currentThread().interrupt();
      LOG.error("Errors occurred while closing the data statistics coordinator context", e);
    }

    LOG.info("Data statistics coordinator for {} closed.", operatorName);
  }

  void callInCoordinatorThread(Callable<Void> callable, String errorMessage) {
    // Ensure the task is done by the coordinator executor.
    if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
      try {
        final Callable<Void> guardedCallable =
            () -> {
              try {
                return callable.call();
              } catch (Throwable t) {
                LOG.error("Uncaught Exception in DataStatistics Coordinator Executor", t);
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
        LOG.error("Uncaught Exception in DataStatistics coordinator executor", t);
        throw new FlinkRuntimeException(errorMessage, t);
      }
    }
  }

  public void runInCoordinatorThread(Runnable runnable) {
    this.coordinatorExecutor.execute(
        new ThrowableCatchingRunnable(
            (throwable) -> {
              this.coordinatorThreadFactory.uncaughtException(Thread.currentThread(), throwable);
            },
            runnable));
  }

  private void runInCoordinatorThread(
      ThrowingRunnable<Throwable> action, String actionName, Object... actionNameFormatParameters) {
    ensureStarted();
    runInCoordinatorThread(
        () -> {
          try {
            action.run();
          } catch (Throwable t) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
            String actionString = String.format(actionName, actionNameFormatParameters);
            LOG.error(
                "Uncaught exception in the data statistics {} while {}. Triggering job failover.",
                operatorName,
                actionString,
                t);
            operatorCoordinatorContext.failJob(t);
          }
        });
  }

  private void ensureStarted() {
    Preconditions.checkState(started, "The coordinator has not started yet.");
  }

  private int parallelism() {
    return operatorCoordinatorContext.currentParallelism();
  }

  private void handleDataStatisticRequest(int subtask, DataStatisticsEvent<K> event) {
    if (globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
        subtask, event)) {
      GlobalStatisticsAggregator<K> lastCompletedAggregator =
          globalStatisticsAggregatorTracker.lastCompletedAggregator();
      sendDataStatisticsToSubtasks(
          lastCompletedAggregator.checkpointId(), lastCompletedAggregator.dataStatistics());
    }
  }

  private void sendDataStatisticsToSubtasks(
      long checkpointId, DataStatistics<K> globalDataStatistics) {
    callInCoordinatorThread(
        () -> {
          DataStatisticsEvent<K> dataStatisticsEvent =
              new DataStatisticsEvent<>(checkpointId, globalDataStatistics);
          int parallelism = parallelism();
          for (int i = 0; i < parallelism; ++i) {
            subtaskGateways.getOnlyGatewayAndCheckReady(i).sendEvent(dataStatisticsEvent);
          }
          return null;
        },
        String.format("Failed to send global data statistics for checkpoint %d", checkpointId));
  }

  @Override
  @SuppressWarnings("unchecked")
  public void handleEventFromOperator(int subtask, int attemptNumber, OperatorEvent event) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Handling event from subtask {} (#{}) of {}: {}",
              subtask,
              attemptNumber,
              operatorName,
              event);
          Preconditions.checkArgument(event instanceof DataStatisticsEvent);
          handleDataStatisticRequest(subtask, ((DataStatisticsEvent<K>) event));
        },
        "handling operator event %s from subtask %d (#%d)",
        event.getClass(),
        subtask,
        attemptNumber);
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    runInCoordinatorThread(
        () -> {
          LOG.debug(
              "Taking a state snapshot on data statistics coordinator {} for checkpoint {}",
              operatorName,
              checkpointId);
          try {
            byte[] serializedDataDistributionWeight =
                InstantiationUtil.serializeObject(
                    globalStatisticsAggregatorTracker.lastCompletedAggregator());
            resultFuture.complete(serializedDataDistributionWeight);
          } catch (Throwable e) {
            ExceptionUtils.rethrowIfFatalErrorOrOOM(e);
            resultFuture.completeExceptionally(
                new CompletionException(
                    String.format("Failed to checkpoint data statistics for %s", operatorName), e));
          }
        },
        "taking checkpoint %d",
        checkpointId);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
      throws Exception {
    if (started) {
      throw new IllegalStateException(
          "The coordinator can only be reset if it was not yet started");
    }

    if (checkpointData == null) {
      return;
    }

    LOG.info(
        "Restoring data statistic coordinator {} from checkpoint {}.", operatorName, checkpointId);
    globalStatisticsAggregatorTracker.setLastCompletedAggregator(
        InstantiationUtil.deserializeObject(
            checkpointData, GlobalStatisticsAggregator.class.getClassLoader()));
  }

  @Override
  public void subtaskReset(int subtask, long checkpointId) {
    runInCoordinatorThread(
        () -> {
          LOG.info(
              "Resetting subtask {} to checkpoint {} for data statistics {}.",
              subtask,
              checkpointId,
              operatorName);
          Preconditions.checkState(
              this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.reset(subtask);
        },
        "handling subtask %d recovery to checkpoint %d",
        subtask,
        checkpointId);
  }

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, @Nullable Throwable reason) {
    runInCoordinatorThread(
        () -> {
          LOG.info(
              "Unregistering gateway after failure for subtask {} (#{}) of data statistic {}.",
              subtask,
              attemptNumber,
              operatorName);
          Preconditions.checkState(
              this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.unregisterSubtaskGateway(subtask, attemptNumber);
        },
        "handling subtask %d (#%d) failure",
        subtask,
        attemptNumber);
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
        "making event gateway to subtask %d (#%d) available",
        subtask,
        attemptNumber);
  }

  @VisibleForTesting
  GlobalStatisticsAggregatorTracker<K> globalStatisticsAggregatorTracker() {
    return globalStatisticsAggregatorTracker;
  }

  private static class SubtaskGateways {
    private final Map<Integer, SubtaskGateway>[] gateways;

    private SubtaskGateways(int parallelism) {
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
          "Already have a subtask gateway for %d (#%d).",
          subtaskIndex,
          attemptNumber);
      LOG.debug("Register gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
      gateways[subtaskIndex].put(attemptNumber, gateway);
    }

    private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      LOG.debug("Unregister gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
      gateways[subtaskIndex].remove(attemptNumber);
    }

    private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndCheckReady(int subtaskIndex) {
      Preconditions.checkState(
          gateways[subtaskIndex].size() > 0,
          "Subtask %d is not ready yet to receive events.",
          subtaskIndex);
      return Iterables.getOnlyElement(gateways[subtaskIndex].values());
    }

    private void reset(int subtaskIndex) {
      gateways[subtaskIndex].clear();
    }
  }
}
