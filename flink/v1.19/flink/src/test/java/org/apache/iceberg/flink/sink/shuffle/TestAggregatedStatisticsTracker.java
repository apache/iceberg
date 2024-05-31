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
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.junit.Before;
import org.junit.Test;

public class TestAggregatedStatisticsTracker {
  private static final int NUM_SUBTASKS = 2;

  private final Schema schema =
      new Schema(Types.NestedField.optional(1, "str", Types.StringType.get()));
  private final SortOrder sortOrder = SortOrder.builderFor(schema).asc("str").build();
  private final SortKey sortKey = new SortKey(schema, sortOrder);
  private final MapDataStatisticsSerializer statisticsSerializer =
      MapDataStatisticsSerializer.fromSortKeySerializer(new SortKeySerializer(schema, sortOrder));
  private final SortKey keyA = sortKey;
  private final SortKey keyB = sortKey;

  private AggregatedStatisticsTracker<MapDataStatistics, Map<SortKey, Long>>
      aggregatedStatisticsTracker;

  public TestAggregatedStatisticsTracker() {
    keyA.set(0, "a");
    keyB.set(0, "b");
  }

  @Before
  public void before() throws Exception {
    aggregatedStatisticsTracker =
        new AggregatedStatisticsTracker<>("testOperator", statisticsSerializer, NUM_SUBTASKS);
  }

  @Test
  public void receiveNewerDataStatisticEvent() {
    MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(keyA);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                0, checkpoint1Subtask0DataStatisticEvent))
        .isNull();
    assertThat(aggregatedStatisticsTracker.inProgressStatistics().checkpointId()).isEqualTo(1);

    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint2Subtask0DataStatistic.add(keyA);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint2Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(2, checkpoint2Subtask0DataStatistic, statisticsSerializer);
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                0, checkpoint2Subtask0DataStatisticEvent))
        .isNull();
    // Checkpoint 2 is newer than checkpoint1, thus dropping in progress statistics for checkpoint1
    assertThat(aggregatedStatisticsTracker.inProgressStatistics().checkpointId()).isEqualTo(2);
  }

  @Test
  public void receiveOlderDataStatisticEventTest() {
    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint2Subtask0DataStatistic.add(keyA);
    checkpoint2Subtask0DataStatistic.add(keyB);
    checkpoint2Subtask0DataStatistic.add(keyB);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint3Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(2, checkpoint2Subtask0DataStatistic, statisticsSerializer);
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                0, checkpoint3Subtask0DataStatisticEvent))
        .isNull();

    MapDataStatistics checkpoint1Subtask1DataStatistic = new MapDataStatistics();
    checkpoint1Subtask1DataStatistic.add(keyB);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask1DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask1DataStatistic, statisticsSerializer);
    // Receive event from old checkpoint, aggregatedStatisticsAggregatorTracker won't return
    // completed statistics and in progress statistics won't be updated
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                1, checkpoint1Subtask1DataStatisticEvent))
        .isNull();
    assertThat(aggregatedStatisticsTracker.inProgressStatistics().checkpointId()).isEqualTo(2);
  }

  @Test
  public void receiveCompletedDataStatisticEvent() {
    MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(keyA);
    checkpoint1Subtask0DataStatistic.add(keyB);
    checkpoint1Subtask0DataStatistic.add(keyB);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                0, checkpoint1Subtask0DataStatisticEvent))
        .isNull();

    MapDataStatistics checkpoint1Subtask1DataStatistic = new MapDataStatistics();
    checkpoint1Subtask1DataStatistic.add(keyA);
    checkpoint1Subtask1DataStatistic.add(keyA);
    checkpoint1Subtask1DataStatistic.add(keyB);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint1Subtask1DataStatisticEvent =
            DataStatisticsEvent.create(1, checkpoint1Subtask1DataStatistic, statisticsSerializer);
    // Receive data statistics from all subtasks at checkpoint 1
    AggregatedStatistics<MapDataStatistics, Map<SortKey, Long>> completedStatistics =
        aggregatedStatisticsTracker.updateAndCheckCompletion(
            1, checkpoint1Subtask1DataStatisticEvent);

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.checkpointId()).isEqualTo(1);
    MapDataStatistics globalDataStatistics =
        (MapDataStatistics) completedStatistics.dataStatistics();
    assertThat((long) globalDataStatistics.statistics().get(keyA))
        .isEqualTo(
            checkpoint1Subtask0DataStatistic.statistics().get(keyA)
                + checkpoint1Subtask1DataStatistic.statistics().get(keyA));
    assertThat((long) globalDataStatistics.statistics().get(keyB))
        .isEqualTo(
            checkpoint1Subtask0DataStatistic.statistics().get(keyB)
                + checkpoint1Subtask1DataStatistic.statistics().get(keyB));
    assertThat(aggregatedStatisticsTracker.inProgressStatistics().checkpointId())
        .isEqualTo(completedStatistics.checkpointId() + 1);

    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint2Subtask0DataStatistic.add(keyA);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint2Subtask0DataStatisticEvent =
            DataStatisticsEvent.create(2, checkpoint2Subtask0DataStatistic, statisticsSerializer);
    assertThat(
            aggregatedStatisticsTracker.updateAndCheckCompletion(
                0, checkpoint2Subtask0DataStatisticEvent))
        .isNull();
    assertThat(completedStatistics.checkpointId()).isEqualTo(1);

    MapDataStatistics checkpoint2Subtask1DataStatistic = new MapDataStatistics();
    checkpoint2Subtask1DataStatistic.add(keyB);
    DataStatisticsEvent<MapDataStatistics, Map<SortKey, Long>>
        checkpoint2Subtask1DataStatisticEvent =
            DataStatisticsEvent.create(2, checkpoint2Subtask1DataStatistic, statisticsSerializer);
    // Receive data statistics from all subtasks at checkpoint 2
    completedStatistics =
        aggregatedStatisticsTracker.updateAndCheckCompletion(
            1, checkpoint2Subtask1DataStatisticEvent);

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.checkpointId()).isEqualTo(2);
    assertThat(aggregatedStatisticsTracker.inProgressStatistics().checkpointId())
        .isEqualTo(completedStatistics.checkpointId() + 1);
  }
}
