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

    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    // Verify global data statistics is the aggregation of all subtasks data statistics
    MapDataStatistics<String> globalDataStatistics =
        (MapDataStatistics<String>)
            dataStatisticsCoordinator.completeAggregatedDataStatistics().dataStatistics();
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.dataStatistics().get("a")
            + (long) checkpoint1Subtask1DataStatistic.dataStatistics().get("a"),
        (long) globalDataStatistics.dataStatistics().get("a"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.dataStatistics().get("b")
            + (long) checkpoint1Subtask1DataStatistic.dataStatistics().get("b"),
        (long) globalDataStatistics.dataStatistics().get("b"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.dataStatistics().get("c")
            + (long) checkpoint1Subtask1DataStatistic.dataStatistics().get("c"),
        (long) globalDataStatistics.dataStatistics().get("c"));
  }

  @Test
  public void testCleanupExpiredEntryInAggregateDataStatisticsMap() throws Exception {
    tasksReady();
    MapDataStatistics<String> checkpoint1Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint1Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    Assert.assertEquals(
        1, dataStatisticsCoordinator.incompleteAggregatedDataStatistics().checkpointId());
    Assert.assertNull(dataStatisticsCoordinator.completeAggregatedDataStatistics());

    MapDataStatistics<String> checkpoint2Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint2Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint2Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(2, checkpoint2Subtask0DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint2Subtask0DataStatisticEvent);
    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    // Checkpoint 2 is newer than checkpoint1, thus drop pending data statistics for checkpoint1
    Assert.assertEquals(
        2, dataStatisticsCoordinator.incompleteAggregatedDataStatistics().checkpointId());
    Assert.assertNull(dataStatisticsCoordinator.completeAggregatedDataStatistics());

    MapDataStatistics<String> checkpoint3Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask0DataStatistic.add("a");
    checkpoint3Subtask0DataStatistic.add("b");
    checkpoint3Subtask0DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint3Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(3, checkpoint3Subtask0DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint3Subtask0DataStatisticEvent);
    MapDataStatistics<String> checkpoint3Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask1DataStatistic.add("a");
    checkpoint3Subtask1DataStatistic.add("a");
    checkpoint3Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint3Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(3, checkpoint3Subtask1DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint3Subtask1DataStatisticEvent);
    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    // Receive data statistics from all subtasks at checkpoint 3
    Assert.assertEquals(
        3, dataStatisticsCoordinator.completeAggregatedDataStatistics().checkpointId());
    MapDataStatistics<String> globalDataStatistics =
        (MapDataStatistics<String>)
            dataStatisticsCoordinator.completeAggregatedDataStatistics().dataStatistics();
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.dataStatistics().get("a")
            + checkpoint3Subtask1DataStatistic.dataStatistics().get("a"),
        (long) globalDataStatistics.dataStatistics().get("a"));
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.dataStatistics().get("b")
            + checkpoint3Subtask1DataStatistic.dataStatistics().get("b"),
        (long) globalDataStatistics.dataStatistics().get("b"));
    Assert.assertNull(dataStatisticsCoordinator.incompleteAggregatedDataStatistics());

    MapDataStatistics<String> checkpoint1Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint1Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask1DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint1Subtask1DataStatisticEvent);
    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    // Receive event from old checkpoint, completedAggregatedDataStatistics and
    // pendingAggregatedDataStatistics won't be updated
    Assert.assertEquals(
        3, dataStatisticsCoordinator.completeAggregatedDataStatistics().checkpointId());
    Assert.assertNull(dataStatisticsCoordinator.incompleteAggregatedDataStatistics());

    MapDataStatistics<String> checkpoint4Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint4Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint4Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(4, checkpoint4Subtask0DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint4Subtask0DataStatisticEvent);
    MapDataStatistics<String> checkpoint4Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint4Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(4, checkpoint4Subtask1DataStatistic);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint4Subtask1DataStatisticEvent);
    waitForCoordinatorToProcessActions(dataStatisticsCoordinator.context());
    // Receive data statistics from all subtasks at checkpoint 4
    Assert.assertEquals(
        4, dataStatisticsCoordinator.completeAggregatedDataStatistics().checkpointId());
    Assert.assertNull(dataStatisticsCoordinator.incompleteAggregatedDataStatistics());
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

  static void waitForCoordinatorToProcessActions(DataStatisticsCoordinatorContext<String> context) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.callInCoordinatorThread(
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
