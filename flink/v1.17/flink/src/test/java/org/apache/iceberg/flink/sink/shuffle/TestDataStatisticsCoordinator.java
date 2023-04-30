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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.util.ExceptionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinator {
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  private static final int NUM_SUBTASKS = 2;

  private EventReceivingTasks receivingTasks;
  private DataStatisticsCoordinator<String> dataStatisticsCoordinator;

  @Before
  public void before() throws Exception {
    receivingTasks = EventReceivingTasks.createForRunningTasks();
    dataStatisticsCoordinator =
        new DataStatisticsCoordinator<>(
            OPERATOR_NAME,
            new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, NUM_SUBTASKS),
            new MapDataStatisticsFactory<>());
  }

  private void tasksReady() throws Exception {
    dataStatisticsCoordinator.start();
    setAllTasksReady(NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);
  }

  @Test
  public void testThrowExceptionWhenNotStarted() {
    String failureMessage =
        "Call should fail when data statistics coordinator has not started yet.";

    Assert.assertThrows(
        failureMessage,
        IllegalStateException.class,
        () ->
            dataStatisticsCoordinator.handleEventFromOperator(
                0, 0, new DataStatisticsEvent<>(0, new MapDataStatistics<>())));
    Assert.assertThrows(
        failureMessage,
        IllegalStateException.class,
        () -> dataStatisticsCoordinator.executionAttemptFailed(0, 0, null));
    Assert.assertThrows(
        failureMessage,
        IllegalStateException.class,
        () -> dataStatisticsCoordinator.checkpointCoordinator(0, null));
  }

  @Test
  public void testDataStatisticsEventHandling() throws Exception {
    tasksReady();

    MapDataStatistics<String> checkpoint1Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask0DataStatistic.add("a");
    checkpoint1Subtask0DataStatistic.add("b");
    checkpoint1Subtask0DataStatistic.add("b");
    checkpoint1Subtask0DataStatistic.add("c");
    checkpoint1Subtask0DataStatistic.add("c");
    checkpoint1Subtask0DataStatistic.add("c");
    DataStatisticsEvent<String> checkpoint1Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic);
    MapDataStatistics<String> checkpoint1Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask1DataStatistic.add("a");
    checkpoint1Subtask1DataStatistic.add("b");
    checkpoint1Subtask1DataStatistic.add("c");
    checkpoint1Subtask1DataStatistic.add("c");
    DataStatisticsEvent<String> checkpoint1Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask1DataStatistic);
    // Handle events from operators for checkpoint 1
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint1Subtask1DataStatisticEvent);

    waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify global data statistics is the aggregation of all subtasks data statistics
    MapDataStatistics<String> globalDataStatistics =
        (MapDataStatistics<String>)
            dataStatisticsCoordinator
                .globalStatisticsAggregatorTracker()
                .lastCompletedAggregator()
                .dataStatistics();
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("a")
            + (long) checkpoint1Subtask1DataStatistic.mapStatistics().get("a"),
        (long) globalDataStatistics.mapStatistics().get("a"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("b")
            + (long) checkpoint1Subtask1DataStatistic.mapStatistics().get("b"),
        (long) globalDataStatistics.mapStatistics().get("b"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("c")
            + (long) checkpoint1Subtask1DataStatistic.mapStatistics().get("c"),
        (long) globalDataStatistics.mapStatistics().get("c"));
  }

  static void setAllTasksReady(
      int subtasks,
      DataStatisticsCoordinator<String> dataStatisticsCoordinator,
      EventReceivingTasks receivingTasks) {
    for (int i = 0; i < subtasks; i++) {
      dataStatisticsCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  static void waitForCoordinatorToProcessActions(DataStatisticsCoordinator<String> coordinator) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    coordinator.callInCoordinatorThread(
        () -> {
          future.complete(null);
          return null;
        },
        "Coordinator fails to process action");

    try {
      future.get();
    } catch (InterruptedException e) {
      throw new AssertionError("test interrupted");
    } catch (ExecutionException e) {
      ExceptionUtils.rethrow(ExceptionUtils.stripExecutionException(e));
    }
  }

  static byte[] waitForCheckpoint(long checkpointId, DataStatisticsCoordinator<String> coordinator)
      throws InterruptedException, ExecutionException {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(checkpointId, future);
    return future.get();
  }
}
