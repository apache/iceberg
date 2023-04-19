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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinatorProvider {
  private static final OperatorID OPERATOR_ID = new OperatorID();
  private static final int NUM_SUBTASKS = 1;

  private DataStatisticsCoordinatorProvider<String> provider;
  private EventReceivingTasks receivingTasks;

  @Before
  public void before() {
    provider =
        new DataStatisticsCoordinatorProvider<>(
            "DataStatisticsCoordinatorProviderTest", OPERATOR_ID, new MapDataStatisticsFactory<>());
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCheckpointAndReset() throws Exception {
    RecreateOnResetOperatorCoordinator coordinator =
        (RecreateOnResetOperatorCoordinator)
            provider.create(new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));
    DataStatisticsCoordinator<String> dataStatisticsCoordinator =
        (DataStatisticsCoordinator<String>) coordinator.getInternalCoordinator();

    // Start the coordinator
    coordinator.start();
    TestDataStatisticsCoordinator.setAllTasksReady(
        NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);
    MapDataStatistics<String> checkpoint1Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask0DataStatistic.add("a");
    checkpoint1Subtask0DataStatistic.add("b");
    checkpoint1Subtask0DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint1Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic);

    // Handle events from operators for checkpoint 1
    coordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(
        dataStatisticsCoordinator.context());
    // Verify checkpoint 1 global data statistics
    MapDataStatistics<String> checkpoint1GlobalDataStatistics =
        (MapDataStatistics<String>)
            dataStatisticsCoordinator.completeAggregatedDataStatistics().dataStatistics();
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("a"),
        checkpoint1GlobalDataStatistics.mapStatistics().get("a"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("b"),
        checkpoint1GlobalDataStatistics.mapStatistics().get("b"));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic.mapStatistics().get("c"),
        checkpoint1GlobalDataStatistics.mapStatistics().get("c"));
    byte[] bytes = TestDataStatisticsCoordinator.waitForCheckpoint(1L, dataStatisticsCoordinator);

    MapDataStatistics<String> checkpoint2Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask0DataStatistic.add("d");
    checkpoint1Subtask0DataStatistic.add("e");
    checkpoint1Subtask0DataStatistic.add("e");
    DataStatisticsEvent<String> checkpoint2Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(2, checkpoint2Subtask0DataStatistic);
    // Handle events from operators for checkpoint 2
    coordinator.handleEventFromOperator(0, 0, checkpoint2Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(
        dataStatisticsCoordinator.context());
    // Verify checkpoint 1 global data statistics
    MapDataStatistics<String> checkpoint2GlobalDataStatistics =
        (MapDataStatistics<String>)
            dataStatisticsCoordinator.completeAggregatedDataStatistics().dataStatistics();
    Assert.assertEquals(
        checkpoint2Subtask0DataStatistic.mapStatistics().get("d"),
        checkpoint2GlobalDataStatistics.mapStatistics().get("d"));
    Assert.assertEquals(
        checkpoint2Subtask0DataStatistic.mapStatistics().get("e"),
        checkpoint2GlobalDataStatistics.mapStatistics().get("e"));
    TestDataStatisticsCoordinator.waitForCheckpoint(2L, dataStatisticsCoordinator);

    // Reset coordinator to checkpoint 1
    coordinator.resetToCheckpoint(1L, bytes);
    DataStatisticsCoordinator<String> restoredDataStatisticsCoordinator =
        (DataStatisticsCoordinator<String>) coordinator.getInternalCoordinator();
    Assert.assertNotEquals(
        "The restored shuffle coordinator should be a different instance",
        restoredDataStatisticsCoordinator,
        dataStatisticsCoordinator);
    // Verify restored data statistics
    MapDataStatistics<String> restoredAggregateDataStatistics =
        (MapDataStatistics<String>)
            restoredDataStatisticsCoordinator.completeAggregatedDataStatistics().dataStatistics();
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics.mapStatistics().get("a"),
        restoredAggregateDataStatistics.mapStatistics().get("a"));
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics.mapStatistics().get("b"),
        restoredAggregateDataStatistics.mapStatistics().get("b"));
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics.mapStatistics().get("c"),
        restoredAggregateDataStatistics.mapStatistics().get("c"));
  }
}
