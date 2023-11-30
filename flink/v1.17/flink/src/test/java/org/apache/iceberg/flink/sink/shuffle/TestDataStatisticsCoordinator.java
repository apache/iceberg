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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinator {
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);
  private static final int NUM_SUBTASKS = 2;

  private final Schema schema =
      new Schema(Types.NestedField.optional(1, "str", Types.StringType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("str").build();
  private final SortKey sortKey = new SortKey(schema, sortOrder);
  private final MapDataStatisticsSerializer statisticsSerializer =
      MapDataStatisticsSerializer.fromSortKeySerializer(new SortKeySerializer(schema, sortOrder));

  private EventReceivingTasks receivingTasks;
  private DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>>
      dataStatisticsCoordinator;

  @Before
  public void before() throws Exception {
    receivingTasks = EventReceivingTasks.createForRunningTasks();
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
    SortKey key = sortKey.copy();

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
    key.set(0, "a");
    checkpoint1Subtask0DataStatistic.add(key);
    key.set(0, "b");
    checkpoint1Subtask0DataStatistic.add(key);
    key.set(0, "b");
    checkpoint1Subtask0DataStatistic.add(key);
    key.set(0, "c");
    checkpoint1Subtask0DataStatistic.add(key);
    key.set(0, "c");
    checkpoint1Subtask0DataStatistic.add(key);
    key.set(0, "c");
    checkpoint1Subtask0DataStatistic.add(key);

    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);

    MapDataStatistics checkpoint1Subtask1DataStatistic = new MapDataStatistics();
    key.set(0, "a");
    checkpoint1Subtask1DataStatistic.add(key);
    key.set(0, "b");
    checkpoint1Subtask1DataStatistic.add(key);
    key.set(0, "c");
    checkpoint1Subtask1DataStatistic.add(key);
    key.set(0, "c");
    checkpoint1Subtask1DataStatistic.add(key);

    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask1DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask1DataStatistic, statisticsSerializer);

    // Handle events from operators for checkpoint 1
    dataStatisticsCoordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0DataStatisticEvent);
    dataStatisticsCoordinator.handleEventFromOperator(1, 0, checkpoint1Subtask1DataStatisticEvent);

    waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

    // Verify global data statistics is the aggregation of all subtasks data statistics
    SortKey keyA = sortKey.copy();
    keyA.set(0, "a");
    SortKey keyB = sortKey.copy();
    keyB.set(0, "b");
    SortKey keyC = sortKey.copy();
    keyC.set(0, "c");
    MapDataStatistics globalDataStatistics =
        (MapDataStatistics) dataStatisticsCoordinator.completedStatistics().dataStatistics();
    assertThat(globalDataStatistics.statistics())
        .containsExactlyInAnyOrderEntriesOf(
            ImmutableMap.of(
                keyA, 2L,
                keyB, 3L,
                keyC, 5L));
  }

  static void setAllTasksReady(
      int subtasks,
      DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>> dataStatisticsCoordinator,
      EventReceivingTasks receivingTasks) {
    for (int i = 0; i < subtasks; i++) {
      dataStatisticsCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  static void waitForCoordinatorToProcessActions(
      DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>> coordinator) {
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
