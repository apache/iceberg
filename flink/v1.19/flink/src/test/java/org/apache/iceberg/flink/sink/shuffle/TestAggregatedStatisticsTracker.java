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

import static org.apache.flink.runtime.checkpoint.CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
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
    AggregatedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsUtil.collectType(type));
    if (type == StatisticsType.Map || type == StatisticsType.Auto) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
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
    // Checkpoint 2 is newer than checkpoint1, thus dropping in progress and incomplete statistics
    // for checkpoint1
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(2L);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsUtil.collectType(type));
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
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
    AggregatedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint2Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(2L);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsUtil.collectType(type));
    if (type == StatisticsType.Map || type == StatisticsType.Auto) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
    }

    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 1L, CHAR_KEYS.get("b"));
    // Receive event from old checkpoint, aggregatedStatisticsAggregatorTracker won't return
    // completed statistics and in progress statistics won't be updated
    assertThat(tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent)).isNull();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(2L);
    if (type == StatisticsType.Map || type == StatisticsType.Auto) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"), CHAR_KEYS.get("b"), CHAR_KEYS.get("b"));
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

    AggregatedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0DataStatisticEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0);
    if (type == StatisticsType.Map || type == StatisticsType.Auto) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 2L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
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
    assertThat(tracker.inProgressSubtaskSet()).isEmpty();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(INVALID_CHECKPOINT_ID);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsUtil.collectType(type));
    assertThat(tracker.coordinatorMapStatistics()).isNull();
    assertThat(tracker.coordinatorSketchStatistics()).isNull();

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
      assertThat(completedStatistics.rangeBounds()).containsExactly(CHAR_KEYS.get("a"));
    }

    StatisticsEvent checkpoint2Subtask0DataStatisticEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("a"));
    completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint2Subtask0DataStatisticEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0);
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(2L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(tracker.coordinatorMapStatistics())
          .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L));
    } else {
      assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
          .containsExactlyInAnyOrder(CHAR_KEYS.get("a"));
    }

    StatisticsEvent checkpoint2Subtask1DataStatisticEvent =
        createStatisticsEvent(type, TASK_STATISTICS_SERIALIZER, 2L, CHAR_KEYS.get("b"));
    // Receive data statistics from all subtasks at checkpoint 2
    completedStatistics =
        tracker.updateAndCheckCompletion(1, checkpoint2Subtask1DataStatisticEvent);
    assertThat(tracker.inProgressSubtaskSet()).isEmpty();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(INVALID_CHECKPOINT_ID);

    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.checkpointId()).isEqualTo(2L);
    if (StatisticsUtil.collectType(type) == StatisticsType.Map) {
      assertThat(completedStatistics.keyFrequency())
          .isEqualTo(
              ImmutableMap.of(
                  CHAR_KEYS.get("a"), 1L,
                  CHAR_KEYS.get("b"), 1L));
    } else {
      assertThat(completedStatistics.rangeBounds()).containsExactly(CHAR_KEYS.get("a"));
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
            StatisticsType.Map,
            switchToSketchThreshold,
            null);

    StatisticsEvent checkpoint1Subtask0StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("a"),
            CHAR_KEYS.get("b"));
    AggregatedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0);
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Map);
    assertThat(tracker.coordinatorMapStatistics())
        .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
    assertThat(tracker.coordinatorSketchStatistics()).isNull();

    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Map,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0, 1);
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    // converted to sketch statistics as map size is 4 (over the switch threshold of 3)
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Sketch);
    assertThat(tracker.coordinatorMapStatistics()).isNull();
    assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
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
    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsType.Sketch);
    assertThat(completedStatistics.rangeBounds())
        .containsExactly(CHAR_KEYS.get("b"), CHAR_KEYS.get("d"));
    assertThat(tracker.inProgressSubtaskSet()).isEmpty();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(INVALID_CHECKPOINT_ID);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Sketch);
    assertThat(tracker.coordinatorMapStatistics()).isNull();
    assertThat(tracker.coordinatorSketchStatistics()).isNull();
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
    AggregatedStatistics completedStatistics =
        tracker.updateAndCheckCompletion(0, checkpoint1Subtask0StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0);
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Map);
    assertThat(tracker.coordinatorMapStatistics())
        .isEqualTo(ImmutableMap.of(CHAR_KEYS.get("a"), 1L, CHAR_KEYS.get("b"), 1L));
    assertThat(tracker.coordinatorSketchStatistics()).isNull();

    // second operator event has sketch statistics
    StatisticsEvent checkpoint1Subtask1StatisticsEvent =
        createStatisticsEvent(
            StatisticsType.Sketch,
            TASK_STATISTICS_SERIALIZER,
            1L,
            CHAR_KEYS.get("c"),
            CHAR_KEYS.get("d"));
    completedStatistics = tracker.updateAndCheckCompletion(1, checkpoint1Subtask1StatisticsEvent);
    assertThat(completedStatistics).isNull();
    assertThat(tracker.inProgressSubtaskSet()).containsExactlyInAnyOrder(0, 1);
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(1L);
    // coordinator stats switched to sketch
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Sketch);
    assertThat(tracker.coordinatorMapStatistics()).isNull();
    assertThat(tracker.coordinatorSketchStatistics().getResult().getSamples())
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
    assertThat(completedStatistics).isNotNull();
    assertThat(completedStatistics.type()).isEqualTo(StatisticsType.Sketch);
    assertThat(completedStatistics.rangeBounds())
        .containsExactly(CHAR_KEYS.get("b"), CHAR_KEYS.get("d"));
    assertThat(tracker.inProgressSubtaskSet()).isEmpty();
    assertThat(tracker.inProgressCheckpointId()).isEqualTo(INVALID_CHECKPOINT_ID);
    assertThat(tracker.coordinatorStatisticsType()).isEqualTo(StatisticsType.Sketch);
    assertThat(tracker.coordinatorMapStatistics()).isNull();
    assertThat(tracker.coordinatorSketchStatistics()).isNull();
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
