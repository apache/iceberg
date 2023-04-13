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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataStatisticsCoordinatorContext<K> implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(DataStatisticsCoordinatorContext.class);
  private final ExecutorService coordinatorExecutor;
  private final OperatorCoordinator.Context operatorCoordinatorContext;
  private final SubtaskGateways subtaskGateways;
  private final DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory
      coordinatorThreadFactory;

  DataStatisticsCoordinatorContext(
      ExecutorService coordinatorExecutor,
      DataStatisticsCoordinatorProvider.CoordinatorExecutorThreadFactory coordinatorThreadFactory,
      OperatorCoordinator.Context operatorCoordinatorContext) {
    this.coordinatorExecutor = coordinatorExecutor;
    this.coordinatorThreadFactory = coordinatorThreadFactory;
    this.operatorCoordinatorContext = operatorCoordinatorContext;
    this.subtaskGateways = new SubtaskGateways(currentParallelism());
  }

  @Override
  public void close() throws Exception {
    coordinatorExecutor.shutdown();
    coordinatorExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
  }

  void sendDataStatisticsToSubtasks(long checkpointId, DataStatistics<K> globalDataStatistics) {
    callInCoordinatorThread(
        () -> {
          DataStatisticsEvent<K> dataStatisticsEvent =
              new DataStatisticsEvent<>(checkpointId, globalDataStatistics);
          int parallelism = currentParallelism();
          for (int i = 0; i < parallelism; ++i) {
            subtaskGateways.getOnlyGatewayAndCheckReady(i).sendEvent(dataStatisticsEvent);
          }
          return null;
        },
        String.format(
            "Failed to send global data statistics %s for checkpoint %d",
            globalDataStatistics, checkpointId));
  }

  int currentParallelism() {
    return operatorCoordinatorContext.currentParallelism();
  }

  void attemptReady(OperatorCoordinator.SubtaskGateway gateway) {
    Preconditions.checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
    this.subtaskGateways.registerSubtaskGateway(gateway);
  }

  void attemptFailed(int subtaskIndex, int attemptNumber) {
    Preconditions.checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
    this.subtaskGateways.unregisterSubtaskGateway(subtaskIndex, attemptNumber);
  }

  void subtaskReset(int subtaskIndex) {
    Preconditions.checkState(this.coordinatorThreadFactory.isCurrentThreadCoordinatorThread());
    this.subtaskGateways.reset(subtaskIndex);
  }

  void failJob(Throwable cause) {
    operatorCoordinatorContext.failJob(cause);
  }

  /**
   * A helper method that delegates the callable to the coordinator thread if the current thread is
   * not the coordinator thread, otherwise call the callable right away.
   *
   * @param callable the callable to delegate.
   */
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

  private static class SubtaskGateways {
    private final Map<Integer, OperatorCoordinator.SubtaskGateway>[] gateways;

    private SubtaskGateways(int parallelism) {
      this.gateways = new Map[parallelism];

      for (int i = 0; i < parallelism; ++i) {
        this.gateways[i] = new HashMap<>();
      }
    }

    private void registerSubtaskGateway(OperatorCoordinator.SubtaskGateway gateway) {
      int subtaskIndex = gateway.getSubtask();
      int attemptNumber = gateway.getExecution().getAttemptNumber();
      Preconditions.checkState(
          !this.gateways[subtaskIndex].containsKey(attemptNumber),
          "Already have a subtask gateway for %d (#%d).",
          subtaskIndex,
          attemptNumber);
      this.gateways[subtaskIndex].put(attemptNumber, gateway);
    }

    private void unregisterSubtaskGateway(int subtaskIndex, int attemptNumber) {
      this.gateways[subtaskIndex].remove(attemptNumber);
    }

    private OperatorCoordinator.SubtaskGateway getOnlyGatewayAndCheckReady(int subtaskIndex) {
      Preconditions.checkState(
          this.gateways[subtaskIndex].size() > 0,
          "Subtask %d is not ready yet to receive events.",
          subtaskIndex);
      return Iterables.getOnlyElement(this.gateways[subtaskIndex].values());
    }

    private void reset(int subtaskIndex) {
      this.gateways[subtaskIndex].clear();
    }
  }
}
