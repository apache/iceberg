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

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
@Internal
public class TableMaintenanceCoordinator implements OperatorCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(TableMaintenanceCoordinator.class);

  private final String operatorName;
  private final Context context;

  private final ExecutorService coordinatorExecutor;
  private final CoordinatorExecutorThreadFactory coordinatorThreadFactory;
  private transient boolean started;
  private final transient SubtaskGateways subtaskGateways;

  private static final Map<String, Long> LOCK_HELD_MAP = Maps.newConcurrentMap();

  public TableMaintenanceCoordinator(String operatorName, Context context) {
    this.operatorName = operatorName;
    this.context = context;

    this.coordinatorThreadFactory =
        new CoordinatorExecutorThreadFactory(
            "TableMaintenanceCoordinator-" + operatorName, context.getUserCodeClassloader());
    this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
    this.subtaskGateways = new SubtaskGateways(operatorName, context.currentParallelism());
    LOG.info("Created TableMaintenanceCoordinator: {}", operatorName);
  }

  @Override
  public void start() throws Exception {
    LOG.info("Starting TableMaintenanceCoordinator: {}", operatorName);
    this.started = true;
  }

  @Override
  public void close() throws Exception {
    coordinatorExecutor.shutdown();
    this.started = false;
    LOG.info("Closed TableMaintenanceCoordinator: {}", operatorName);
    LOCK_HELD_MAP.clear();
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

          if (event instanceof LockReleasedEvent) {
            handleReleaseLock((LockReleasedEvent) event);
          } else if (event instanceof LockAcquiredEvent) {
            handleLockAcquired((LockAcquiredEvent) event, subtask);
          } else {
            throw new IllegalArgumentException(
                "Invalid operator event type: " + event.getClass().getCanonicalName());
          }
        },
        String.format(
            Locale.ROOT,
            "handling operator event %s from subtask %d (#%d)",
            event.getClass(),
            subtask,
            attemptNumber));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void handleLockAcquired(LockAcquiredEvent event, int subtask) {
    runInCoordinatorThread(
        () -> {
          String lockId = event.lockId();
          boolean isHeld = false;
          if (!LOCK_HELD_MAP.containsKey(lockId)) {
            LOCK_HELD_MAP.put(lockId, event.timestamp());
            isHeld = true;
          } else {
            Long triggerTimestamp = LOCK_HELD_MAP.get(event.lockId());
            if (event.timestamp() == Long.MAX_VALUE) {
              LOCK_HELD_MAP.put(lockId, event.timestamp());
              isHeld = true;
              LOG.info(
                  "Add lock for lock id {}, timestamp: {}, trigger timestamp: {}",
                  event.lockId(),
                  event.timestamp(),
                  triggerTimestamp);
            } else {
              LOG.info(
                  "Lock is not held for lock id {}, timestamp: {}, trigger timestamp: {}",
                  event.lockId(),
                  event.timestamp(),
                  triggerTimestamp);
            }
          }

          LockAcquireResultEvent lockAcquireResultEvent =
              new LockAcquireResultEvent(isHeld, lockId, event.timestamp());
          subtaskGateways.getSubtaskGateway(subtask).sendEvent(lockAcquireResultEvent);
        },
        String.format(
            Locale.ROOT,
            "Failed to send operator %s coordinator to requesting subtask %d ",
            operatorName,
            subtask));
  }

  /** Release the lock and optionally trigger the next pending task. */
  private void handleReleaseLock(LockReleasedEvent lockReleasedEvent) {
    if (LOCK_HELD_MAP.containsKey(lockReleasedEvent.lockId())) {
      LOCK_HELD_MAP.remove(lockReleasedEvent.lockId());
      LOG.info(
          "Release lock for lock id {}, timestamp: {}",
          lockReleasedEvent.lockId(),
          lockReleasedEvent.timestamp());
    }
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) {
    // We don’t need to track how many locks are currently held, because when recovering from state,
    // a `recover lock` will be issued to ensure all tasks finish running and then release all
    // locks.
    // The `TriggerManagerOperator` already keeps the `TableChange` state and related information,
    // so there’s no need to store additional state here.
    runInCoordinatorThread(
        () -> {
          resultFuture.complete(new byte[0]);
        },
        String.format(Locale.ROOT, "taking checkpoint %d", checkpointId));
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {}

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

  private void runInCoordinatorThread(Runnable runnable, String actionString) {
    ensureStarted();
    coordinatorExecutor.execute(
        () -> {
          try {
            runnable.run();
          } catch (Throwable t) {
            LOG.error(
                "Uncaught exception in TableMaintenanceCoordinator while {}: {}",
                actionString,
                t.getMessage(),
                t);
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
                LOG.error(
                    "Uncaught Exception in table maintenance coordinator: {} executor",
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
            "Uncaught Exception in table maintenance coordinator: {} executor", operatorName, t);
        throw new FlinkRuntimeException(errorMessage, t);
      }
    }
  }

  private void ensureStarted() {
    Preconditions.checkState(started, "The coordinator has not started yet.");
  }

  /** Inner class to manage subtask gateways. */
  private static class SubtaskGateways {
    private final String operatorName;
    private final Map<Integer, SubtaskGateway>[] gateways;

    @SuppressWarnings("unchecked")
    private SubtaskGateways(String operatorName, int parallelism) {
      this.operatorName = operatorName;
      gateways = new Map[parallelism];

      for (int i = 0; i < parallelism; ++i) {
        gateways[i] = new java.util.HashMap<>();
      }
    }

    private void registerSubtaskGateway(SubtaskGateway gateway) {
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

    private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      gateways[subtaskIndex].remove(attemptNumber);
      LOG.debug("Unregistered gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
    }

    private SubtaskGateway getSubtaskGateway(int subtaskIndex) {
      Preconditions.checkState(
          !gateways[subtaskIndex].isEmpty(),
          "Coordinator subtask %d is not ready yet to receive events",
          subtaskIndex);
      return gateways[subtaskIndex].values().iterator().next();
    }

    private void reset(int subtaskIndex) {
      gateways[subtaskIndex].clear();
    }
  }

  /** Custom thread factory for the coordinator executor. */
  private static class CoordinatorExecutorThreadFactory
      implements ThreadFactory, Thread.UncaughtExceptionHandler {

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
  static Map<String, Long> lockHeldMap() {
    return LOCK_HELD_MAP;
  }
}
