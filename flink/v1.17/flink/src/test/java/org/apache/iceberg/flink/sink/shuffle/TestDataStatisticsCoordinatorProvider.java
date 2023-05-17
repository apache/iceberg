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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinatorProvider {
  private static final OperatorID OPERATOR_ID = new OperatorID();
  private static final int NUM_SUBTASKS = 1;
  private DataStatisticsCoordinatorProvider<MapDataStatistics, Map<RowData, Long>> provider;
  private EventReceivingTasks receivingTasks;

  @Before
  public void before() {
    TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>> statisticsSerializer =
        MapDataStatisticsSerializer.fromKeySerializer(
            new RowDataSerializer(RowType.of(new VarCharType())));
    provider =
        new DataStatisticsCoordinatorProvider<>(
            "DataStatisticsCoordinatorProviderTest", OPERATOR_ID, statisticsSerializer);
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCheckpointAndReset() throws Exception {
    RecreateOnResetOperatorCoordinator coordinator =
        (RecreateOnResetOperatorCoordinator)
            provider.create(new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS));
    DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>> dataStatisticsCoordinator =
        (DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>)
            coordinator.getInternalCoordinator();

    // Start the coordinator
    coordinator.start();
    TestDataStatisticsCoordinator.setAllTasksReady(
        NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);
    MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("a")));
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("b")));
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("c")));
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic);

    // Handle events from operators for checkpoint 1
    coordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify checkpoint 1 global data statistics
    MapDataStatistics checkpoint1GlobalDataStatistics =
        (MapDataStatistics)
            dataStatisticsCoordinator
                .globalStatisticsAggregatorTracker()
                .lastCompletedAggregator()
                .dataStatistics();
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic
            .statistics()
            .get(GenericRowData.of(StringData.fromString("a"))),
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("a"))));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic
            .statistics()
            .get(GenericRowData.of(StringData.fromString("b"))),
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("b"))));
    Assert.assertEquals(
        checkpoint1Subtask0DataStatistic
            .statistics()
            .get(GenericRowData.of(StringData.fromString("c"))),
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("c"))));
    byte[] bytes = TestDataStatisticsCoordinator.waitForCheckpoint(1L, dataStatisticsCoordinator);

    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("d")));
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("e")));
    checkpoint1Subtask0DataStatistic.add(GenericRowData.of(StringData.fromString("e")));
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint2Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(2, checkpoint2Subtask0DataStatistic);
    // Handle events from operators for checkpoint 2
    coordinator.handleEventFromOperator(0, 0, checkpoint2Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify checkpoint 1 global data statistics
    MapDataStatistics checkpoint2GlobalDataStatistics =
        (MapDataStatistics)
            dataStatisticsCoordinator
                .globalStatisticsAggregatorTracker()
                .lastCompletedAggregator()
                .dataStatistics();
    Assert.assertEquals(
        checkpoint2Subtask0DataStatistic
            .statistics()
            .get(GenericRowData.of(StringData.fromString("d"))),
        checkpoint2GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("d"))));
    Assert.assertEquals(
        checkpoint2Subtask0DataStatistic
            .statistics()
            .get(GenericRowData.of(StringData.fromString("e"))),
        checkpoint2GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("e"))));
    TestDataStatisticsCoordinator.waitForCheckpoint(2L, dataStatisticsCoordinator);

    // Reset coordinator to checkpoint 1
    coordinator.resetToCheckpoint(1L, bytes);
    DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>
        restoredDataStatisticsCoordinator =
            (DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>)
                coordinator.getInternalCoordinator();
    Assert.assertNotEquals(
        "The restored shuffle coordinator should be a different instance",
        restoredDataStatisticsCoordinator,
        dataStatisticsCoordinator);
    // Verify restored data statistics
    MapDataStatistics restoredAggregateDataStatistics =
        (MapDataStatistics)
            restoredDataStatisticsCoordinator
                .globalStatisticsAggregatorTracker()
                .lastCompletedAggregator()
                .dataStatistics();
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("a"))),
        restoredAggregateDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("a"))));
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("b"))),
        restoredAggregateDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("b"))));
    Assert.assertEquals(
        checkpoint1GlobalDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("c"))),
        restoredAggregateDataStatistics
            .statistics()
            .get(GenericRowData.of(StringData.fromString("c"))));
  }
}
