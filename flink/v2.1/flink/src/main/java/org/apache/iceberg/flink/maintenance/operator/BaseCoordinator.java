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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
  private static final Map<String, Consumer<LockReleaseEvent>> LOCK_RELEASE_CONSUMERS =
      Maps.newConcurrentMap();
  private static final List<LockReleaseEvent> PENDING_RELEASE_EVENTS = Lists.newArrayList();

  protected BaseCoordinator(String operatorName, Context context) {
    this.operatorName = operatorName;
    this.context = context;

    this.coordinatorThreadFactory =
        new CoordinatorExecutorThreadFactory(
            "Coordinator-" + operatorName, context.getUserCodeClassloader());
    this.coordinatorExecutor = Executors.newSingleThreadExecutor(coordinatorThreadFactory);
    Preconditions.checkState(
        context.currentParallelism() == 1, "Coordinator must run with parallelism 1");
    this.subtaskGateways = SubtaskGateways.create(operatorName);
    LOG.info("Created coordinator: {}", operatorName);
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  void registerLock(LockRegisterEvent lockRegisterEvent) {
    LOCK_RELEASE_CONSUMERS.put(
        lockRegisterEvent.lockId(),
        lock -> {
          LOG.info(
              "Send release event for lock id {}, timestamp: {} to Operator {}",
              lock.lockId(),
              lock.timestamp(),
              operatorName());
          subtaskGateways.subtaskGateway().sendEvent(lock);
        });

    synchronized (PENDING_RELEASE_EVENTS) {
      if (!PENDING_RELEASE_EVENTS.isEmpty()) {
        PENDING_RELEASE_EVENTS.forEach(this::handleReleaseLock);
        PENDING_RELEASE_EVENTS.clear();
      }
    }
  }

  void handleReleaseLock(LockReleaseEvent lockReleaseEvent) {
    synchronized (PENDING_RELEASE_EVENTS) {
      if (LOCK_RELEASE_CONSUMERS.containsKey(lockReleaseEvent.lockId())) {
        LOCK_RELEASE_CONSUMERS.get(lockReleaseEvent.lockId()).accept(lockReleaseEvent);
        LOG.info(
            "Send release event for lock id {}, timestamp: {}",
            lockReleaseEvent.lockId(),
            lockReleaseEvent.timestamp());
      } else {
        PENDING_RELEASE_EVENTS.add(lockReleaseEvent);
        LOG.info(
            "No consumer for lock id {}, timestamp: {}",
            lockReleaseEvent.lockId(),
            lockReleaseEvent.timestamp());
      }
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
    synchronized (PENDING_RELEASE_EVENTS) {
      LOCK_RELEASE_CONSUMERS.clear();
      PENDING_RELEASE_EVENTS.clear();
    }

    LOG.info("Closed coordinator: {}", operatorName);
  }

  @Override
  public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
      throws Exception {
    runInCoordinatorThread(
        () -> resultFuture.complete(new byte[0]),
        String.format(Locale.ROOT, "taking checkpoint %d", checkpointId));
  }

  @Override
  public void resetToCheckpoint(long checkpointId, byte[] checkpointData) {
    Preconditions.checkState(
        !started, "The coordinator %s can only be reset if it was not yet started", operatorName);
    LOG.info("Reset to checkpoint {}", checkpointId);
    synchronized (PENDING_RELEASE_EVENTS) {
      LOCK_RELEASE_CONSUMERS.clear();
      PENDING_RELEASE_EVENTS.clear();
    }
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) {}

  @Override
  public void subtaskReset(int subtask, long checkpointId) {
    runInCoordinatorThread(
        () -> {
          LOG.info("Subtask {} is reset to checkpoint {}", subtask, checkpointId);
          Preconditions.checkState(coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
          subtaskGateways.reset();
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
  private record SubtaskGateways(String operatorName, Map<Integer, SubtaskGateway> gateways) {

    static SubtaskGateways create(String operatorName) {
      return new SubtaskGateways(operatorName, Maps.newHashMap());
    }

    void registerSubtaskGateway(SubtaskGateway gateway) {
      int attemptNumber = gateway.getExecution().getAttemptNumber();
      Preconditions.checkState(
          !gateways.containsKey(attemptNumber),
          "Coordinator of %s already has a subtask gateway for (#%d)",
          operatorName,
          attemptNumber);
      LOG.debug("Coordinator of {} registers gateway for attempt {}", operatorName, attemptNumber);
      gateways.put(attemptNumber, gateway);
      LOG.debug("Registered gateway for  attempt {}", attemptNumber);
    }

    void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      gateways.remove(attemptNumber);
      LOG.debug("Unregistered gateway for subtask {} attempt {}", subtaskIndex, attemptNumber);
    }

    SubtaskGateway subtaskGateway() {
      Preconditions.checkState(
          !gateways.isEmpty(),
          "Coordinator of %s is not ready yet to receive events",
          operatorName);
      return Iterables.getOnlyElement(gateways.values());
    }

    void reset() {
      gateways.clear();
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
  List<LockReleaseEvent> pendingReleaseEvents() {
    return PENDING_RELEASE_EVENTS;
  }
}
