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

import java.util.Map;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGlobalStatisticsAggregatorTracker {
  private static final int NUM_SUBTASKS = 2;
  private final RowType rowType = RowType.of(new VarCharType());
  private final BinaryRowData binaryRowDataA =
      new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("a")));
  private final BinaryRowData binaryRowDataB =
      new RowDataSerializer(rowType).toBinaryRow(GenericRowData.of(StringData.fromString("b")));
  private final TypeSerializer<RowData> rowSerializer = new RowDataSerializer(rowType);
  private final TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>>
      statisticsSerializer = MapDataStatisticsSerializer.fromKeySerializer(rowSerializer);
  private GlobalStatisticsAggregatorTracker<MapDataStatistics, Map<RowData, Long>>
      globalStatisticsAggregatorTracker;

  @Before
  public void before() throws Exception {
    TypeSerializer<DataStatistics<MapDataStatistics, Map<RowData, Long>>> statisticsSerializer =
        MapDataStatisticsSerializer.fromKeySerializer(
            new RowDataSerializer(RowType.of(new VarCharType())));

    globalStatisticsAggregatorTracker =
        new GlobalStatisticsAggregatorTracker<>(statisticsSerializer, NUM_SUBTASKS);
  }

  @Test
  public void receiveDataStatisticEventAndCheckCompletionTest() {
    MapDataStatistics checkpoint1Subtask0DataStatistic = new MapDataStatistics();
    checkpoint1Subtask0DataStatistic.add(binaryRowDataA);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(1, checkpoint1Subtask0DataStatistic, statisticsSerializer);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint1Subtask0DataStatisticEvent));

    Assert.assertEquals(1, globalStatisticsAggregatorTracker.inProgressAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.lastCompletedAggregator());

    MapDataStatistics checkpoint2Subtask0DataStatistic = new MapDataStatistics();
    checkpoint2Subtask0DataStatistic.add(binaryRowDataA);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint2Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(2, checkpoint2Subtask0DataStatistic, statisticsSerializer);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint2Subtask0DataStatisticEvent));

    // Checkpoint 2 is newer than checkpoint1, thus drop inProgressAggregator for checkpoint1
    Assert.assertEquals(2, globalStatisticsAggregatorTracker.inProgressAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.lastCompletedAggregator());

    MapDataStatistics checkpoint3Subtask0DataStatistic = new MapDataStatistics();
    checkpoint3Subtask0DataStatistic.add(binaryRowDataA);
    checkpoint3Subtask0DataStatistic.add(binaryRowDataB);
    checkpoint3Subtask0DataStatistic.add(binaryRowDataB);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint3Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(3, checkpoint3Subtask0DataStatistic, statisticsSerializer);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint3Subtask0DataStatisticEvent));

    MapDataStatistics checkpoint3Subtask1DataStatistic = new MapDataStatistics();
    checkpoint3Subtask1DataStatistic.add(binaryRowDataA);
    checkpoint3Subtask1DataStatistic.add(binaryRowDataA);
    checkpoint3Subtask1DataStatistic.add(binaryRowDataB);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint3Subtask1DataStatisticEvent =
            new DataStatisticsEvent<>(3, checkpoint3Subtask1DataStatistic, statisticsSerializer);
    // Receive data statistics from all subtasks at checkpoint 3
    Assert.assertTrue(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint3Subtask1DataStatisticEvent));
    Assert.assertEquals(
        3, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    MapDataStatistics globalDataStatistics =
        (MapDataStatistics)
            globalStatisticsAggregatorTracker.lastCompletedAggregator().dataStatistics();
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.statistics().get(binaryRowDataA)
            + checkpoint3Subtask1DataStatistic.statistics().get(binaryRowDataA),
        (long) globalDataStatistics.statistics().get(binaryRowDataA));
    Assert.assertEquals(
        checkpoint3Subtask0DataStatistic.statistics().get(binaryRowDataB)
            + checkpoint3Subtask1DataStatistic.statistics().get(binaryRowDataB),
        (long) globalDataStatistics.statistics().get(binaryRowDataB));
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());

    MapDataStatistics checkpoint1Subtask1DataStatistic = new MapDataStatistics();
    checkpoint1Subtask1DataStatistic.add(binaryRowDataB);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint1Subtask1DataStatisticEvent =
            new DataStatisticsEvent<>(1, checkpoint1Subtask1DataStatistic, statisticsSerializer);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint1Subtask1DataStatisticEvent));
    // Receive event from old checkpoint, globalStatisticsAggregatorTracker lastCompletedAggregator
    // and inProgressAggregator won't be updated
    Assert.assertEquals(
        3, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());

    MapDataStatistics checkpoint4Subtask0DataStatistic = new MapDataStatistics();
    checkpoint4Subtask0DataStatistic.add(binaryRowDataA);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint4Subtask0DataStatisticEvent =
            new DataStatisticsEvent<>(4, checkpoint4Subtask0DataStatistic, statisticsSerializer);
    Assert.assertFalse(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            0, checkpoint4Subtask0DataStatisticEvent));

    MapDataStatistics checkpoint4Subtask1DataStatistic = new MapDataStatistics();
    checkpoint3Subtask1DataStatistic.add(binaryRowDataB);
    DataStatisticsEvent<MapDataStatistics, Map<RowData, Long>>
        checkpoint4Subtask1DataStatisticEvent =
            new DataStatisticsEvent<>(4, checkpoint4Subtask1DataStatistic, statisticsSerializer);
    // Receive data statistics from all subtasks at checkpoint 4
    Assert.assertTrue(
        globalStatisticsAggregatorTracker.receiveDataStatisticEventAndCheckCompletion(
            1, checkpoint4Subtask1DataStatisticEvent));
    Assert.assertEquals(
        4, globalStatisticsAggregatorTracker.lastCompletedAggregator().checkpointId());
    Assert.assertNull(globalStatisticsAggregatorTracker.inProgressAggregator());
  }
}
