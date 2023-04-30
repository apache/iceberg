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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGlobalStatisticsAggregatorTracker {
  private static final int NUM_SUBTASKS = 2;
  private GlobalStatisticsAggregatorTracker<String> globalStatisticsAggregatorTracker;

  @Before
  public void before() throws Exception {
    globalStatisticsAggregatorTracker =
        new GlobalStatisticsAggregatorTracker<>(new MapDataStatisticsFactory<>(), NUM_SUBTASKS);
  }

  @Test
  public void receiveDataStatisticEventAndCheckCompletionTest() {
    MapDataStatistics<String> checkpoint1Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint1Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint1Subtask0DataStatisticEvent));

    Assert.assertEquals(1, globalStatisticsAggregatorTracker.inProgressAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.lastCompletedAggregator());

    MapDataStatistics<String> checkpoint2Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint2Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint2Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(2, checkpoint2Subtask0DataStatistic);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint2Subtask0DataStatisticEvent));

    // Checkpoint 2 is newer than checkpoint1, thus drop inProgressAggregator for checkpoint1
    Assert.assertEquals(2, globalStatisticsAggregatorTracker.inProgressAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.lastCompletedAggregator());

    MapDataStatistics<String> checkpoint3Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask0DataStatistic.add("a");
    checkpoint3Subtask0DataStatistic.add("b");
    checkpoint3Subtask0DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint3Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(3, checkpoint3Subtask0DataStatistic);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint3Subtask0DataStatisticEvent));

    MapDataStatistics<String> checkpoint3Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask1DataStatistic.add("a");
    checkpoint3Subtask1DataStatistic.add("a");
    checkpoint3Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint3Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(3, checkpoint3Subtask1DataStatistic);
    // Receive data statistics from all subtasks at checkpoint 3
    Assert.assertTrue(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint3Subtask1DataStatisticEvent));
    Assert.assertEquals(
        3, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    MapDataStatistics<String> globalDataStatistics =
        (MapDataStatistics<String>)
            globalStatisticsAggregatorTracker.lastCompletedAggregator().dataStatistics();
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.mapStatistics().get("a")
            + checkpoint3Subtask1DataStatistic.mapStatistics().get("a"),
        (long) globalDataStatistics.mapStatistics().get("a"));
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.mapStatistics().get("b")
            + checkpoint3Subtask1DataStatistic.mapStatistics().get("b"),
        (long) globalDataStatistics.mapStatistics().get("b"));
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());

    MapDataStatistics<String> checkpoint1Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint1Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint1Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(1, checkpoint1Subtask1DataStatistic);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint1Subtask1DataStatisticEvent));
    // Receive event from old checkpoint, globalStatisticsAggregatorTracker lastCompletedAggregator
    // and
    // inProgressAggregator won't be updated
    Assert.assertEquals(
        3, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());

    MapDataStatistics<String> checkpoint4Subtask0DataStatistic = new MapDataStatistics<>();
    checkpoint4Subtask0DataStatistic.add("a");
    DataStatisticsEvent<String> checkpoint4Subtask0DataStatisticEvent =
        new DataStatisticsEvent<>(4, checkpoint4Subtask0DataStatistic);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint4Subtask0DataStatisticEvent));

    MapDataStatistics<String> checkpoint4Subtask1DataStatistic = new MapDataStatistics<>();
    checkpoint3Subtask1DataStatistic.add("b");
    DataStatisticsEvent<String> checkpoint4Subtask1DataStatisticEvent =
        new DataStatisticsEvent<>(4, checkpoint4Subtask1DataStatistic);
    // Receive data statistics from all subtasks at checkpoint 4
    Assert.assertTrue(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint4Subtask1DataStatisticEvent));
    Assert.assertEquals(
        4, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());
  }
}
