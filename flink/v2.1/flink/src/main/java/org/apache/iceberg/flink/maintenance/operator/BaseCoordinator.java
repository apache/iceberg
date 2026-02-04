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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base coordinator for table maintenance operators. Provides common functionality for thread
 * management, subtask gateway management, and checkpoint handling.
 */
@Internal
public abstract class BaseCoordinator implements OperatorCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCoordinator.class);

  private final String operatorName;
  private final Context context;

  private final ExecutorService coordinatorExecutor;
  private boolean started;
  private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
  private final SubtaskGateways subtaskGateways;
  protected static final Map<String, Consumer<LockReleasedEvent>> LOCK_RELEASE_CONSUMERS =
      Maps.newConcurrentMap();
  protected static final List<LockReleasedEvent> PENDING_RELEASE_EVENTS =
      new CopyOnWriteArrayList<>();

  protected BaseCoordinator(String operatorName, Context context) {
    this.operatorName = operatorName;
    this.context = context;

    this.coordinatorThreadFactory =
        new CoordinatorExecutorThreadFactory(
            "Coordinator-" + operatorName, context.getUserCodeClassloader());
    this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
    this.subtaskGateways = new SubtaskGateways(operatorName, context.currentParallelism());
    LOG.info("Created coordinator: {}", operatorName);
  }

  @VisibleForTesting
  void handleReleaseLock(LockReleasedEvent lockReleasedEvent) {
    if (LOCK_RELEASE_CONSUMERS.containsKey(lockReleasedEvent.lockId())) {
      LOCK_RELEASE_CONSUMERS.get(lockReleasedEvent.lockId()).accept(lockReleasedEvent);
      LOG.info(
          "Send release event for lock id {}, timestamp: {}",
          lockReleasedEvent.lockId(),
          lockReleasedEvent.timestamp());
    } else {
      PENDING_RELEASE_EVENTS.add(lockReleasedEvent);
      LOG.info(
          "No consumer for lock id {}, timestamp: {}",
          lockReleasedEvent.lockId(),
          lockReleasedEvent.timestamp());
    }
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting coordinator: {}", operatorName);
    this.started = true;
  }

  @Override
  public void close() throws Exception {
    coordinatorExecutor.shutdown();
    this.started = false;
    LOCK_RELEASE_CONSUMERS.clear();
    PENDING_RELEASE_EVENTS.clear();
    LOG.info("Closed coordinator: {}", operatorName);
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void subtaskReset(int subtask, long checkpointId) {
    runInCoordinatorThread(
        () -> {
          LOG.info("Subtask {} is reset to checkpoint {}", subtask, checkpointId);
          Preconditions.checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.reset(subtask);
        },
        String.format(
            Locale.ROOT, "handling subtask %d recovery to checkpoint %d", subtask, checkpointId));
  }

  @Override
  public void executionAttemptFailed(int subtask, int attemptNumber, Throwable reason) {
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
        String.format(Locale.ROOT, "handling subtask %d (#%d) failure", subtask, attemptNumber));
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
            Locale.ROOT,
            "making event gateway to subtask %d (#%d) available",
            subtask,
            attemptNumber));
  }

  protected CoordinatorExecutorThreadFactory coordinatorThreadFactory() {
    return coordinatorThreadFactory;
  }

  protected SubtaskGateways subtaskGateways() {
    return subtaskGateways;
  }

  protected String operatorName() {
    return operatorName;
  }

  protected void runInCoordinatorThread(Runnable runnable, String actionString) {
    ensureStarted();
    coordinatorExecutor.execute(
        () -> {
          try {
            runnable.run();
          } catch (Throwable t) {
            LOG.error(
                "Uncaught exception in coordinator while {}: {}", actionString, t.getMessage(), t);
            context.failJob(t);
          }
        });
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
                LOG.error("Uncaught Exception in coordinator {} executor", operatorName, t);
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
        LOG.error("Uncaught Exception in coordinator {} executor", operatorName, t);
        throw new FlinkRuntimeException(errorMessage, t);
      }
    }
  }

  private void ensureStarted() {
    Preconditions.checkState(started, "The coordinator has not started yet.");
  }

  /** Inner class to manage subtask gateways. */
  protected static class SubtaskGateways {
    private final String operatorName;
    private final Map<Integer, SubtaskGateway>[] gateways;

    @SuppressWarnings("unchecked")
    protected SubtaskGateways(String operatorName, int parallelism) {
      this.operatorName = operatorName;
      gateways = new Map[parallelism];

      for (int i = 0; i < parallelism; ++i) {
        gateways[i] = new java.util.HashMap<>();
      }
    }

    protected void registerSubtaskGateway(SubtaskGateway gateway) {
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
      LOG.debug("Registered gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
    }

    protected void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      gateways[subtaskIndex].remove(attemptNumber);
      LOG.debug("Unregistered gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
    }

    protected SubtaskGateway getSubtaskGateway(int subtaskIndex) {
      Preconditions.checkState(
          !gateways[subtaskIndex].isEmpty(),
          "Coordinator subtask %d is not ready yet to receive events",
          subtaskIndex);
      return gateways[subtaskIndex].values().iterator().next();
    }

    protected void reset(int subtaskIndex) {
      gateways[subtaskIndex].clear();
    }
  }

  /** Custom thread factory for the coordinator executor. */
  protected static class CoordinatorExecutorThreadFactory
      implements java.util.concurrent.ThreadFactory, Thread.UncaughtExceptionHandler {

    private final String coordinatorThreadName;
    private final ClassLoader classLoader;
    private final Thread.UncaughtExceptionHandler errorHandler;

    private Thread thread;

    CoordinatorExecutorThreadFactory(String coordinatorThreadName, ClassLoader contextClassLoader) {
      this(coordinatorThreadName, contextClassLoader, FatalExitExceptionHandler.INSTANCE);
    }

    CoordinatorExecutorThreadFactory(
        String coordinatorThreadName,
        ClassLoader contextClassLoader,
        Thread.UncaughtExceptionHandler errorHandler) {
      this.coordinatorThreadName = coordinatorThreadName;
      this.classLoader = contextClassLoader;
      this.errorHandler = errorHandler;
    }

    @Override
    public synchronized Thread newThread(@Nonnull Runnable runnable) {
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

  @VisibleForTesting
  List<LockReleasedEvent> pendingReleaseEvents() {
    return PENDING_RELEASE_EVENTS;
  }
}
