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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinator {
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  private static final int NUM_SUBTASKS = 2;
  private TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>>
      statisticsSerializer;

  private EventReceivingTasks receivingTasks;
  private DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>>
      dataStatisticsCoordinator;

  @Before
  public void before() throws Exception {
    receivingTasks = EventReceivingTasks.createForRunningTasks();
    statisticsSerializer =
        MapDataStatisticsSerializer.fromKeySerializer(
            new RowDataSerializer(RowType.of(new VarCharType())));

    dataStatisticsCoordinator =
        new DataStatisticsCoordinator<>(
            OPERATOR_NAME,
            new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, NUM_SUBTASKS),
            statisticsSerializer);
  }

  private void tasksReady() throws Exception {
    dataStatisticsCoordinator.start();
    setAllTasksReady(NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);
  }

  @Test
  public void testThrowExceptionWhenNotStarted() {
    String failureMessage = "The coordinator of TestCoordinator has not started yet.";

    assertThatThrownBy(
            () ->
                dataStatisticsCoordinator.handleEventFromOperator(
                    0,
                    0,
                    DataStatisticsEvent.create(0, new MapDataStatistics(), statisticsSerializer)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(failureMessage);
    assertThatThrownBy(() -> dataStatisticsCoordinator.executionAttemptFailed(0, 0, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(failureMessage);
    assertThatThrownBy(() -> dataStatisticsCoordinator.checkpointCoordinator(0, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(failureMessage);
  }

  @Test
  public void testDataStatisticsEventHandling() throws Exception {
    tasksReady();
    // When coordinator handles events from operator, DataStatisticsUtil#deserializeDataStatistics
    // deserializes bytes into BinaryRowData
    RowType rowType = RowType.of(new VarCharType());
    BinaryRowData binaryRowDataA =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("a")));
    BinaryRowData binaryRowDataB =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("b")));
    BinaryRowData binaryRowDataC =
        new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("c")));

    MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(binaryRowDataA);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataB);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataB);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataC);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataC);
    checkpoint1Subtask0DataStatistic.add(binaryRowDataC);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);
    MapDataStatistics checkpoint1Subtask1DataStatistic = new MapDataStatistics();
    checkpoint1Subtask1DataStatistic.add(binaryRowDataA);
    checkpoint1Subtask1DataStatistic.add(binaryRowDataB);
    checkpoint1Subtask1DataStatistic.add(binaryRowDataC);
    checkpoint1Subtask1DataStatistic.add(binaryRowDataC);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask1DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask1DataStatistic, statisticsSerializer);
    // Handle events from operators for checkpoint 1
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint1Subtask1DataStatisticEvent);

    waitForCoordinatorToProcessActions(dataStatisticsCoordinator);
    // Verify global data statistics is the aggregation of all subtasks data statistics
    MapDataStatistics globalDataStatistics =
        (MapDataStatistics) dataStatisticsCoordinator.completedStatistics().dataStatistics();
    assertThat(globalDataStatistics.statistics())
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                binaryRowDataA,
                checkpoint1Subtask0DataStatistic.statistics().get(binaryRowDataA)
                    + (long) checkpoint1Subtask1DataStatistic.statistics().get(binaryRowDataA),
                binaryRowDataB,
                checkpoint1Subtask0DataStatistic.statistics().get(binaryRowDataB)
                    + (long) checkpoint1Subtask1DataStatistic.statistics().get(binaryRowDataB),
                binaryRowDataC,
                checkpoint1Subtask0DataStatistic.statistics().get(binaryRowDataC)
                    + (long) checkpoint1Subtask1DataStatistic.statistics().get(binaryRowDataC)));
  }

  static void setAllTasksReady(
      int subtasks,
      DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>> dataStatisticsCoordinator,
      EventReceivingTasks receivingTasks) {
    for (int i = 0; i < subtasks; i++) {
      dataStatisticsCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  static void waitForCoordinatorToProcessActions(
      DataStatisticsCoordinator<MapDataStatistics, Map<RowData, Long>> coordinator) {
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
}
