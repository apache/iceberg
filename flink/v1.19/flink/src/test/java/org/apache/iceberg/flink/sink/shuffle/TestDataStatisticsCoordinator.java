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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.util.ExceptionUtils;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestDataStatisticsCoordinator {
  private static final String OPERATOR_NAME = "TestCoordinator";
  private static final OperatorID TEST_OPERATOR_ID = new OperatorID(1234L, 5678L);

  private EventReceivingTasks receivingTasks;

  @BeforeEach
  public void before() throws Exception {
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  private void tasksReady(DataStatisticsCoordinator coordinator) {
    setAllTasksReady(Fixtures.NUM_SUBTASKS, coordinator, receivingTasks);
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testThrowExceptionWhenNotStarted(StatisticsType type) throws Exception {
    try (DataStatisticsCoordinator dataStatisticsCoordinator = createCoordinator(type)) {
      String failureMessage = "The coordinator of TestCoordinator has not started yet.";
      assertThatThrownBy(
              () ->
                  dataStatisticsCoordinator.handleEventFromOperator(
                      0,
                      0,
                      StatisticsEvent.createTaskStatisticsEvent(
                          0, new MapDataStatistics(), Fixtures.TASK_STATISTICS_SERIALIZER)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
      assertThatThrownBy(() -> dataStatisticsCoordinator.executionAttemptFailed(0, 0, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
      assertThatThrownBy(() -> dataStatisticsCoordinator.checkpointCoordinator(0, null))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(failureMessage);
    }
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void testDataStatisticsEventHandling(StatisticsType type) throws Exception {
    try (DataStatisticsCoordinator dataStatisticsCoordinator = createCoordinator(type)) {
      dataStatisticsCoordinator.start();
      tasksReady(dataStatisticsCoordinator);

      StatisticsEvent checkpoint1Subtask0DataStatisticEvent =
          Fixtures.createStatisticsEvent(
              type,
              Fixtures.TASK_STATISTICS_SERIALIZER,
              1L,
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("c"),
              CHAR_KEYS.get("c"),
              CHAR_KEYS.get("c"));
      StatisticsEvent checkpoint1Subtask1DataStatisticEvent =
          Fixtures.createStatisticsEvent(
              type,
              Fixtures.TASK_STATISTICS_SERIALIZER,
              1L,
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("c"),
              CHAR_KEYS.get("c"));
      // Handle events from operators for checkpoint 1
      dataStatisticsCoordinator.handleEventFromOperator(
          0, 0, checkpoint1Subtask0DataStatisticEvent);
      dataStatisticsCoordinator.handleEventFromOperator(
          1, 0, checkpoint1Subtask1DataStatisticEvent);

      waitForCoordinatorToProcessActions(dataStatisticsCoordinator);

      // Verify global data statistics is the aggregation of all subtasks data statistics
      AggregatedStatistics aggregatedStatistics = dataStatisticsCoordinator.completedStatistics();
      assertThat(aggregatedStatistics.checkpointId()).isEqualTo(1L);
      assertThat(aggregatedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
      if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
        assertThat(aggregatedStatistics.keyFrequency())
            .isEqualTo(
                ImmutableMap.of(
                    CHAR_KEYS.get("a"), 2L,
                    CHAR_KEYS.get("b"), 3L,
                    CHAR_KEYS.get("c"), 5L));
      } else {
        assertThat(aggregatedStatistics.rangeBounds()).containsExactly(CHAR_KEYS.get("b"));
      }
    }
  }

  static void setAllTasksReady(
      int subtasks,
      DataStatisticsCoordinator dataStatisticsCoordinator,
      EventReceivingTasks receivingTasks) {
    for (int i = 0; i < subtasks; i++) {
      dataStatisticsCoordinator.executionAttemptReady(
          i, 0, receivingTasks.createGatewayForSubtask(i, 0));
    }
  }

  static void waitForCoordinatorToProcessActions(DataStatisticsCoordinator coordinator) {
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

  private static DataStatisticsCoordinator createCoordinator(StatisticsType type) {
    return new DataStatisticsCoordinator(
        OPERATOR_NAME,
        new MockOperatorCoordinatorContext(TEST_OPERATOR_ID, Fixtures.NUM_SUBTASKS),
        Fixtures.SCHEMA,
        Fixtures.SORT_ORDER,
        Fixtures.NUM_SUBTASKS,
        type);
  }
}
