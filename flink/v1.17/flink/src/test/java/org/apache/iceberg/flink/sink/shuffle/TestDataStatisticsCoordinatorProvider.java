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
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.EventReceivingTasks;
import org.apache.flink.runtime.operators.coordination.MockOperatorCoordinatorContext;
import org.apache.flink.runtime.operators.coordination.RecreateOnResetOperatorCoordinator;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

public class TestDataStatisticsCoordinatorProvider {
  private static final OperatorID OPERATOR_ID = new OperatorID();
  private static final int NUM_SUBTASKS = 1;

  private final Schema schema =
      new Schema(Types.NestedField.optional(1, "str", Types.StringType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("str").build();
  private final SortKey sortKey = new SortKey(schema, sortOrder);
  private final MapDataStatisticsSerializer statisticsSerializer =
      MapDataStatisticsSerializer.fromSortKeySerializer(new SortKeySerializer(schema, sortOrder));

  private DataStatisticsCoordinatorProvider<MapDataStatistics, Map<SortKey, Long>> provider;
  private EventReceivingTasks receivingTasks;

  @Before
  public void before() {
    provider =
        new DataStatisticsCoordinatorProvider<>(
            "DataStatisticsCoordinatorProvider", OPERATOR_ID, statisticsSerializer);
    receivingTasks = EventReceivingTasks.createForRunningTasks();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCheckpointAndReset() throws Exception {
    SortKey keyA = sortKey.copy();
    keyA.set(0, "a");
    SortKey keyB = sortKey.copy();
    keyB.set(0, "b");
    SortKey keyC = sortKey.copy();
    keyC.set(0, "c");
    SortKey keyD = sortKey.copy();
    keyD.set(0, "c");
    SortKey keyE = sortKey.copy();
    keyE.set(0, "c");

    try (RecreateOnResetOperatorCoordinator coordinator =
        (RecreateOnResetOperatorCoordinator)
            provider.create(new MockOperatorCoordinatorContext(OPERATOR_ID, NUM_SUBTASKS))) {
      DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>> dataStatisticsCoordinator =
          (DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>>)
              coordinator.getInternalCoordinator();

      // Start the coordinator
      coordinator.start();
      TestDataStatisticsCoordinator.setAllTasksReady(
          NUM_SUBTASKS, dataStatisticsCoordinator, receivingTasks);
      MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
      checkpoint1Subtask0DataStatistic.add(keyA);
      checkpoint1Subtask0DataStatistic.add(keyB);
      checkpoint1Subtask0DataStatistic.add(keyC);
      DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
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
      checkpoint2Subtask0DataStatistic.add(keyD);
      checkpoint2Subtask0DataStatistic.add(keyE);
      checkpoint2Subtask0DataStatistic.add(keyE);
      DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
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
      DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>>
          restoredDataStatisticsCoordinator =
          (DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>>)
              coordinator.getInternalCoordinator();
      assertThat(dataStatisticsCoordinator).isNotEqualTo(restoredDataStatisticsCoordinator);
      // Verify restored data statistics
      MapDataStatistics restoredAggregateDataStatistics =
          (MapDataStatistics)
              restoredDataStatisticsCoordinator.completedStatistics().dataStatistics();
      assertThat(restoredAggregateDataStatistics.statistics())
          .isEqualTo(checkpoint1GlobalDataStatistics.statistics());
    }
  }

  private byte[] waitForCheckpoint(
      long checkpointId,
      DataStatisticsCoordinator<MapDataStatistics, Map<SortKey, Long>> coordinator)
      throws InterruptedException, ExecutionException {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    coordinator.checkpointCoordinator(checkpointId, future);
    return future.get();
  }
}
