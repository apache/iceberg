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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinatorProvider {
  private static final OperatorID OPERATOR_ID = new OperatorID();
  private static final int NUM_SUBTASKS = 1;

  private DataStatisticsCoordinatorProvider<MapDataStatistics, Map<RowData, Long>> provider;
  private EventReceivingTasks receivingTasks;
  private TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>>
      statisticsSerializer;

  @Before
  public void before() {
    statisticsSerializer =
        MapDataStatisticsSerializer.fromKeySerializer(
            new RowDataSerializer(RowType.of(new VarCharType())));
    provider =
        new DataStatisticsCoordinatorProvider<>(
            "DataStatisticsCoordinatorProvider", OPERATOR_ID, statisticsSerializer);
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCheckpointAndReset() throws Exception {
    RowType rowType = RowType.of(new VarCharType());
    // When coordinator handles events from operator, DataStatisticsUtil#deserializeDataStatistics
    // deserializes bytes into BinaryRowData
    BinaryRowData binaryRowDataA =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("a")));
    BinaryRowData binaryRowDataB =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("b")));
    BinaryRowData binaryRowDataC =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("c")));
    BinaryRowData binaryRowDataD =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("d")));
    BinaryRowData binaryRowDataE =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("e")));

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
    checkpoint1Subtask0DataStatistic.add(binaryRowDataA);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataB);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataC);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);

    // Handle events from operators for checkpoint 1
    coordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify checkpoint 1 global data statistics
    MapDataStatistics checkpoint1GlobalDataStatistics =
        (MapDataStatistics) dataStatisticsCoordinator.completedStatistics().dataStatistics();
    assertThat(checkpoint1GlobalDataStatistics.statistics())
        .isEqualTo(checkpoint1Subtask0DataStatistic.statistics());
    byte[] checkpoint1Bytes = waitForCheckpoint(1L, dataStatisticsCoordinator);

    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint2Subtask0DataStatistic.add(binaryRowDataD);
    checkpoint2Subtask0DataStatistic.add(binaryRowDataE);
    checkpoint2Subtask0DataStatistic.add(binaryRowDataE);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint2Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(2, checkpoint2Subtask0DataStatistic, statisticsSerializer);
    // Handle events from operators for checkpoint 2
    coordinator.handleEventFromOperator(0, 0, checkpoint2Subtask0DataStatisticEvent);
    TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify checkpoint 2 global data statistics
    MapDataStatistics checkpoint2GlobalDataStatistics =
        (MapDataStatistics) dataStatisticsCoordinator.completedStatistics().dataStatistics();
    assertThat(checkpoint2GlobalDataStatistics.statistics())
        .isEqualTo(checkpoint2Subtask0DataStatistic.statistics());
    waitForCheckpoint(2L, dataStatisticsCoordinator);

    // Reset coordinator to checkpoint 1
    coordinator.resetToCheckpoint(1L, checkpoint1Bytes);
    DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>
        restoredDataStatisticsCoordinator =
            (DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>)
                coordinator.getInternalCoordinator();
    assertThat(dataStatisticsCoordinator).isNotEqualTo(restoredDataStatisticsCoordinator);
    // Verify restored data statistics
    MapDataStatistics restoredAggregateDataStatistics =
        (MapDataStatistics)
            restoredDataStatisticsCoordinator.completedStatistics().dataStatistics();
    assertThat(restoredAggregateDataStatistics.statistics())
        .isEqualTo(checkpoint1GlobalDataStatistics.statistics());
  }

  private byte[] waitForCheckpoint(
      long checkpointId,
      DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>> coordinator)
      throws InterruptedException, ExecutionException {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(checkpointId, future);
    return future.get();
  }
}
