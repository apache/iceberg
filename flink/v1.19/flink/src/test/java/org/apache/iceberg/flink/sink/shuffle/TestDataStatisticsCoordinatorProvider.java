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

import static org.apache.iceberg.flink.sink.shuffle.Fixtures.CHAR_KEYS;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.TASK_STATISTICS_SERIALIZER;
import static org.apache.iceberg.flink.sink.shuffle.Fixtures.createStatisticsEvent;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestDataStatisticsCoordinatorProvider {
  private static final OperatorID OPERATOR_ID = new OperatorID();

  private EventReceivingTasks receivingTasks;

  @BeforeEach
  public void before() {
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testCheckpointAndReset(StatisticsType type) throws Exception {
    DataStatisticsCoordinatorProvider provider = createProvider(type, Fixtures.NUM_SUBTASKS);
    try (RecreateOnResetOperatorCoordinator coordinator =
        (RecreateOnResetOperatorCoordinator)
            provider.create(
                new MockOperatorCoordinatorContext(OPERATOR_ID, Fixtures.NUM_SUBTASKS))) {
      DataStatisticsCoordinator dataStatisticsCoordinator =
          (DataStatisticsCoordinator) coordinator.getInternalCoordinator();

      // Start the coordinator
      coordinator.start();
      TestDataStatisticsCoordinator.setAllTasksReady(
          Fixtures.NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);

      // Handle events from operators for checkpoint 1
      StatisticsEvent checkpoint1Subtask0StatisticsEvent =
          createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("a"));
      coordinator.handleEventFromOperator(0, 0, checkpoint1Subtask0StatisticsEvent);
      TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

      StatisticsEvent checkpoint1Subtask1StatisticsEvent =
          createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("b"));
      coordinator.handleEventFromOperator(1, 0, checkpoint1Subtask1StatisticsEvent);
      TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

      // Verify checkpoint 1 global data statistics
      AggregatedStatistics aggregatedStatistics = dataStatisticsCoordinator.completedStatistics();
      assertThat(aggregatedStatistics).isNotNull();
      assertThat(aggregatedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(aggregatedStatistics.keyFrequency())
            .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
      } else {
        assertThat(aggregatedStatistics.keySamples())
            .containsExactly(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"));
      }

      AggregatedStatistics globalStatistics = dataStatisticsCoordinator.globalStatistics();
      assertThat(globalStatistics).isNotNull();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency())
            .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
      } else {
        assertThat(globalStatistics.keySamples()).containsExactly(CHAR_KEYS.get("a"));
      }

      byte[] checkpoint1Bytes = waitForCheckpoint(1L, dataStatisticsCoordinator);

      StatisticsEvent checkpoint2Subtask0StatisticsEvent =
          createStatisticsEvent(
              type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("d"), CHAR_KEYS.get("e"));
      coordinator.handleEventFromOperator(0, 0, checkpoint2Subtask0StatisticsEvent);
      TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

      StatisticsEvent checkpoint2Subtask1StatisticsEvent =
          createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("f"));
      coordinator.handleEventFromOperator(1, 0, checkpoint2Subtask1StatisticsEvent);
      TestDataStatisticsCoordinator.waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

      // Verify checkpoint 2 global data statistics
      aggregatedStatistics = dataStatisticsCoordinator.completedStatistics();
      assertThat(aggregatedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(aggregatedStatistics.keyFrequency())
            .isEqualTo(
                ImmutableMap.of(
                    CHAR_KEYS.get("d"), 1L, CHAR_KEYS.get("e"), 1L, CHAR_KEYS.get("f"), 1L));
      } else {
        assertThat(aggregatedStatistics.keySamples())
            .containsExactly(CHAR_KEYS.get("d"), CHAR_KEYS.get("e"), CHAR_KEYS.get("f"));
      }

      globalStatistics = dataStatisticsCoordinator.globalStatistics();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency())
            .isEqualTo(
                ImmutableMap.of(
                    CHAR_KEYS.get("d"), 1L, CHAR_KEYS.get("e"), 1L, CHAR_KEYS.get("f"), 1L));
      } else {
        assertThat(globalStatistics.keySamples()).containsExactly(CHAR_KEYS.get("e"));
      }

      waitForCheckpoint(2L, dataStatisticsCoordinator);

      // Reset coordinator to checkpoint 1
      coordinator.resetToCheckpoint(1L, checkpoint1Bytes);
      DataStatisticsCoordinator restoredDataStatisticsCoordinator =
          (DataStatisticsCoordinator) coordinator.getInternalCoordinator();
      assertThat(dataStatisticsCoordinator).isNotSameAs(restoredDataStatisticsCoordinator);

      aggregatedStatistics = restoredDataStatisticsCoordinator.completedStatistics();
      assertThat(aggregatedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      // Verify restored data statistics
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(aggregatedStatistics.keyFrequency())
            .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
      } else {
        assertThat(aggregatedStatistics.keySamples())
            .containsExactly(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"));
      }

      globalStatistics = restoredDataStatisticsCoordinator.globalStatistics();
      assertThat(globalStatistics).isNotNull();
      assertThat(globalStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(globalStatistics.keyFrequency())
            .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
      } else {
        assertThat(globalStatistics.keySamples()).containsExactly(CHAR_KEYS.get("a"));
      }
    }
  }

  private byte[] waitForCheckpoint(long checkpointId, DataStatisticsCoordinator coordinator)
      throws InterruptedException, ExecutionException {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(checkpointId, future);
    return future.get();
  }

  private static DataStatisticsCoordinatorProvider createProvider(
      StatisticsType type, int downstreamParallelism) {
    return new DataStatisticsCoordinatorProvider(
        "DataStatisticsCoordinatorProvider",
        OPERATOR_ID,
        Fixtures.SCHEMA,
        Fixtures.SORT_ORDER,
        downstreamParallelism,
        type);
  }
}
