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

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class TestAggregatedStatisticsTracker {
  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void receiveNewerStatisticsEvent(StatisticsType type) {
    AggregatedStatisticsTracker tracker = createTracker(type);

    StatisticsEvent checkpoint1Subtask0StatisticsEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("a"));
    CompletedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    AggregatedStatisticsTracker.Aggregation aggregation =
        tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics()).isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"));
    }

    StatisticsEvent checkpoint2Subtask0StatisticsEvent =
        createStatisticsEvent(
            type,
            TASK_STATISTICS_SERIALIZER,
            2L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("b"));
    completedStatistics = tracker.updateAndCheckCompletion(0, checkpoint2Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    // both checkpoints are tracked
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L, 2L);
    aggregation = tracker.aggregationsPerCheckpoint().get(2L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("b"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    // checkpoint 1 is completed
    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
    assertThat(completedStatistics.checkpointId()).isEqualTo(1L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(completedStatistics.keyFrequency())
          .isEqualTo(
              ImmutableMap.of(
                  CHAR_KEYS.get("a"), 1L,
                  CHAR_KEYS.get("b"), 1L));
    } else {
      assertThat(completedStatistics.keySamples())
          .containsExactly(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"));
    }

    // checkpoint 2 remains
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(2L);
    aggregation = tracker.aggregationsPerCheckpoint().get(2L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
    }
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void receiveOlderStatisticsEventTest(StatisticsType type) {
    AggregatedStatisticsTracker tracker = createTracker(type);

    StatisticsEvent checkpoint2Subtask0StatisticsEvent =
        createStatisticsEvent(
            type,
            TASK_STATISTICS_SERIALIZER,
            2L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("b"));
    CompletedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint2Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(2L);
    AggregatedStatisticsTracker.Aggregation aggregation =
        tracker.aggregationsPerCheckpoint().get(2L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("b"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    assertThat(completedStatistics).isNull();
    // both checkpoints are tracked
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L, 2L);
    aggregation = tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics()).isEqualTo(ImmutableMap.of(CHAR_KEYS.get("b"), 1L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint3Subtask0StatisticsEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 3L, CHAR_KEYS.get("x"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint3Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L, 2L, 3L);
    aggregation = tracker.aggregationsPerCheckpoint().get(3L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics()).isEqualTo(ImmutableMap.of(CHAR_KEYS.get("x"), 1L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("x"));
    }

    StatisticsEvent checkpoint2Subtask1StatisticsEvent =
        createStatisticsEvent(
            type,
            TASK_STATISTICS_SERIALIZER,
            2L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("b"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint2Subtask1StatisticsEvent);
    // checkpoint 1 is cleared along with checkpoint 2. checkpoint 3 remains
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(3L);
    aggregation = tracker.aggregationsPerCheckpoint().get(3L);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics()).isEqualTo(ImmutableMap.of(CHAR_KEYS.get("x"), 1L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("x"));
    }

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
    assertThat(completedStatistics.checkpointId()).isEqualTo(2L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(completedStatistics.keyFrequency())
          .isEqualTo(
              ImmutableMap.of(
                  CHAR_KEYS.get("a"), 2L,
                  CHAR_KEYS.get("b"), 4L));
    } else {
      assertThat(completedStatistics.keySamples())
          .containsExactly(
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("b"));
    }
  }

  @ParameterizedTest
  @EnumSource(StatisticsType.class)
  public void receiveCompletedStatisticsEvent(StatisticsType type) {
    AggregatedStatisticsTracker tracker = createTracker(type);

    StatisticsEvent checkpoint1Subtask0DataStatisticEvent =
        createStatisticsEvent(
            type,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("b"));

    CompletedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0DataStatisticEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    AggregatedStatisticsTracker.Aggregation aggregation =
        tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint1Subtask1DataStatisticEvent =
        createStatisticsEvent(
            type,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"));

    // Receive data statistics from all subtasks at checkpoint 1
    completedStatistics =
        tracker.updateAndCheckCompletion(1, checkpoint1Subtask1DataStatisticEvent);
    assertThat(tracker.aggregationsPerCheckpoint()).isEmpty();

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsUtil.collectType(type));
    assertThat(completedStatistics.checkpointId()).isEqualTo(1L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(completedStatistics.keyFrequency())
          .isEqualTo(
              ImmutableMap.of(
                  CHAR_KEYS.get("a"), 3L,
                  CHAR_KEYS.get("b"), 3L));
    } else {
      assertThat(completedStatistics.keySamples())
          .containsExactly(
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("b"),
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("a"),
              CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint2Subtask0DataStatisticEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("a"));
    completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint2Subtask0DataStatisticEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(2L);
    aggregation = tracker.aggregationsPerCheckpoint().get(2L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(aggregation.mapStatistics()).isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L));
    } else {
      assertThat(aggregation.sketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"));
    }

    StatisticsEvent checkpoint2Subtask1DataStatisticEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("b"));
    // Receive data statistics from all subtasks at checkpoint 2
    completedStatistics =
        tracker.updateAndCheckCompletion(1, checkpoint2Subtask1DataStatisticEvent);
    assertThat(tracker.aggregationsPerCheckpoint()).isEmpty();

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.checkpointId()).isEqualTo(2L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(completedStatistics.keyFrequency())
          .isEqualTo(
              ImmutableMap.of(
                  CHAR_KEYS.get("a"), 1L,
                  CHAR_KEYS.get("b"), 1L));
    } else {
      assertThat(completedStatistics.keySamples())
          .containsExactly(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"));
    }
  }

  @Test
  public void coordinatorSwitchToSketchOverThreshold() {
    int parallelism = 3;
    int downstreamParallelism = 3;
    int switchToSketchThreshold = 3;
    AggregatedStatisticsTracker tracker =
        new AggregatedStatisticsTracker(
            "testOperator",
            parallelism,
            Fixtures.SCHEMA,
            Fixtures.SORT_ORDER,
            downstreamParallelism,
            StatisticsType.Auto,
            switchToSketchThreshold,
            null);

    StatisticsEvent checkpoint1Subtask0StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"));
    CompletedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    AggregatedStatisticsTracker.Aggregation aggregation =
        tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsType.Map);
    assertThat(aggregation.sketchStatistics()).isNull();
    assertThat(aggregation.mapStatistics())
        .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));

    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    aggregation = tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0, 1);
    // converted to sketch statistics as map size is 4 (over the switch threshold of 3)
    assertThat(aggregation.currentType()).isEqualTo(StatisticsType.Sketch);
    assertThat(aggregation.mapStatistics()).isNull();
    assertThat(aggregation.sketchStatistics().getResult().getSamples())
        .containsExactlyInAnyOrder(
            CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("c"), CHAR_KEYS.get("d"));

    StatisticsEvent checkpoint1Subtask2StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("e"),
            CHAR_KEYS.get("f"));
    completedStatistics = tracker.updateAndCheckCompletion(2, checkpoint1Subtask2StatisticsEvent);
    assertThat(tracker.aggregationsPerCheckpoint()).isEmpty();
    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsType.Sketch);
    assertThat(completedStatistics.keySamples())
        .containsExactly(
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"),
            CHAR_KEYS.get("e"),
            CHAR_KEYS.get("f"));
  }

  @Test
  public void coordinatorMapOperatorSketch() {
    int parallelism = 3;
    int downstreamParallelism = 3;
    AggregatedStatisticsTracker tracker =
        new AggregatedStatisticsTracker(
            "testOperator",
            parallelism,
            Fixtures.SCHEMA,
            Fixtures.SORT_ORDER,
            downstreamParallelism,
            StatisticsType.Auto,
            SketchUtil.COORDINATOR_SKETCH_SWITCH_THRESHOLD,
            null);

    // first operator event has map statistics
    StatisticsEvent checkpoint1Subtask0StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"));
    CompletedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    AggregatedStatisticsTracker.Aggregation aggregation =
        tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsType.Map);
    assertThat(aggregation.sketchStatistics()).isNull();
    assertThat(aggregation.mapStatistics())
        .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));

    // second operator event contains sketch statistics
    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Sketch,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.aggregationsPerCheckpoint().keySet()).containsExactlyInAnyOrder(1L);
    aggregation = tracker.aggregationsPerCheckpoint().get(1L);
    assertThat(aggregation.subtaskSet()).containsExactlyInAnyOrder(0, 1);
    assertThat(aggregation.currentType()).isEqualTo(StatisticsType.Sketch);
    assertThat(aggregation.mapStatistics()).isNull();
    assertThat(aggregation.sketchStatistics().getResult().getSamples())
        .containsExactlyInAnyOrder(
            CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("c"), CHAR_KEYS.get("d"));

    // third operator event has Map statistics
    StatisticsEvent checkpoint1Subtask2StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("e"),
            CHAR_KEYS.get("f"));
    completedStatistics = tracker.updateAndCheckCompletion(2, checkpoint1Subtask2StatisticsEvent);
    assertThat(tracker.aggregationsPerCheckpoint()).isEmpty();
    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsType.Sketch);
    assertThat(completedStatistics.keySamples())
        .containsExactly(
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"),
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"),
            CHAR_KEYS.get("e"),
            CHAR_KEYS.get("f"));
  }

  private AggregatedStatisticsTracker createTracker(StatisticsType type) {
    return new AggregatedStatisticsTracker(
        "testOperator",
        Fixtures.NUM_SUBTASKS,
        Fixtures.SCHEMA,
        Fixtures.SORT_ORDER,
        Fixtures.NUM_SUBTASKS,
        type,
        SketchUtil.COORDINATOR_SKETCH_SWITCH_THRESHOLD,
        null);
  }
}
