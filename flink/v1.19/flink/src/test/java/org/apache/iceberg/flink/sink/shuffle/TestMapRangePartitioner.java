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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataWrapper;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMapRangePartitioner {
  private static final SortOrder SORT_ORDER =
      SortOrder.builderFor(TestFixtures.SCHEMA).asc("data").build();

  private static final SortKey SORT_KEY = new SortKey(TestFixtures.SCHEMA, SORT_ORDER);
  private static final RowType ROW_TYPE = FlinkSchemaUtil.convert(TestFixtures.SCHEMA);
  private static final SortKey[] SORT_KEYS = initSortKeys();

  private static SortKey[] initSortKeys() {
    SortKey[] sortKeys = new SortKey[10];
    for (int i = 0; i < 10; ++i) {
      RowData rowData =
          GenericRowData.of(StringData.fromString("k" + i), i, StringData.fromString("2023-06-20"));
      RowDataWrapper keyWrapper = new RowDataWrapper(ROW_TYPE, TestFixtures.SCHEMA.asStruct());
      keyWrapper.wrap(rowData);
      SortKey sortKey = SORT_KEY;
      sortKey.wrap(keyWrapper);
      sortKeys[i] = sortKey;
    }
    return sortKeys;
  }

  // Total weight is 800
  private final MapDataStatistics mapDataStatistics =
      new MapDataStatistics(
          ImmutableMap.of(
              SORT_KEYS[0],
              350L,
              SORT_KEYS[1],
              230L,
              SORT_KEYS[2],
              120L,
              SORT_KEYS[3],
              40L,
              SORT_KEYS[4],
              10L,
              SORT_KEYS[5],
              10L,
              SORT_KEYS[6],
              10L,
              SORT_KEYS[7],
              10L,
              SORT_KEYS[8],
              10L,
              SORT_KEYS[9],
              10L));

  @Test
  public void testEvenlyDividableNoClosingFileCost() {
    MapRangePartitioner partitioner =
        new MapRangePartitioner(TestFixtures.SCHEMA, SORT_ORDER, mapDataStatistics, 0.0);
    int numPartitions = 8;

    // each task should get targeted weight of 100 (=800/8)
    Map<SortKey, MapRangePartitioner.KeyAssignment> expectedAssignment =
        ImmutableMap.of(
            SORT_KEYS[0],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(0, 1, 2, 3), ImmutableList.of(100L, 100L, 100L, 50L), 0L),
            SORT_KEYS[1],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(3, 4, 5), ImmutableList.of(50L, 100L, 80L), 0L),
            SORT_KEYS[2],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(5, 6), ImmutableList.of(20L, 100L), 0L),
            SORT_KEYS[3],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(40L), 0L),
            SORT_KEYS[4],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L),
            SORT_KEYS[5],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L),
            SORT_KEYS[6],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L),
            SORT_KEYS[7],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L),
            SORT_KEYS[8],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L),
            SORT_KEYS[9],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(10L), 0L));
    Map<SortKey, MapRangePartitioner.KeyAssignment> actualAssignment =
        partitioner.assignment(numPartitions);
    Assertions.assertThat(actualAssignment).isEqualTo(expectedAssignment);

    // key: subtask id
    // value pair: first is the assigned weight, second is the number of assigned keys
    Map<Integer, Pair<Long, Integer>> expectedAssignmentInfo =
        ImmutableMap.of(
            0,
            Pair.of(100L, 1),
            1,
            Pair.of(100L, 1),
            2,
            Pair.of(100L, 1),
            3,
            Pair.of(100L, 2),
            4,
            Pair.of(100L, 1),
            5,
            Pair.of(100L, 2),
            6,
            Pair.of(100L, 1),
            7,
            Pair.of(100L, 7));
    Map<Integer, Pair<Long, Integer>> actualAssignmentInfo = partitioner.assignmentInfo();
    Assertions.assertThat(actualAssignmentInfo).isEqualTo(expectedAssignmentInfo);

    Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults =
        runPartitioner(partitioner, numPartitions);
    validatePartitionResults(expectedAssignmentInfo, partitionResults, 5.0);
  }

  @Test
  public void testEvenlyDividableWithClosingFileCost() {
    MapRangePartitioner partitioner =
        new MapRangePartitioner(TestFixtures.SCHEMA, SORT_ORDER, mapDataStatistics, 5.0);
    int numPartitions = 8;

    // target subtask weight is 100 before close file cost factored in.
    // close file cost is 5 = 5% * 100.
    // key weights before and after close file cost factored in
    // before:     350, 230, 120, 40, 10, 10, 10, 10, 10, 10
    // close-cost:  20,  15,  10,  5,  5,  5,  5,  5,  5,  5
    // after:      370, 245, 130, 45, 15, 15, 15, 15, 15, 15
    // target subtask weight with close cost per subtask is 110 (880/8)
    Map<SortKey, MapRangePartitioner.KeyAssignment> expectedAssignment =
        ImmutableMap.of(
            SORT_KEYS[0],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(0, 1, 2, 3), ImmutableList.of(110L, 110L, 110L, 40L), 5L),
            SORT_KEYS[1],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(3, 4, 5), ImmutableList.of(70L, 110L, 65L), 5L),
            SORT_KEYS[2],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(5, 6), ImmutableList.of(45L, 85L), 5L),
            SORT_KEYS[3],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(6, 7), ImmutableList.of(25L, 20L), 5L),
            SORT_KEYS[4],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L),
            SORT_KEYS[5],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L),
            SORT_KEYS[6],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L),
            SORT_KEYS[7],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L),
            SORT_KEYS[8],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L),
            SORT_KEYS[9],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(7), ImmutableList.of(15L), 5L));
    Map<SortKey, MapRangePartitioner.KeyAssignment> actualAssignment =
        partitioner.assignment(numPartitions);
    Assertions.assertThat(actualAssignment).isEqualTo(expectedAssignment);

    // key: subtask id
    // value pair: first is the assigned weight (excluding close file cost) for the subtask,
    // second is the number of keys assigned to the subtask
    Map<Integer, Pair<Long, Integer>> expectedAssignmentInfo =
        ImmutableMap.of(
            0,
            Pair.of(105L, 1),
            1,
            Pair.of(105L, 1),
            2,
            Pair.of(105L, 1),
            3,
            Pair.of(100L, 2),
            4,
            Pair.of(105L, 1),
            5,
            Pair.of(100L, 2),
            6,
            Pair.of(100L, 2),
            7,
            Pair.of(75L, 7));
    Map<Integer, Pair<Long, Integer>> actualAssignmentInfo = partitioner.assignmentInfo();
    Assertions.assertThat(actualAssignmentInfo).isEqualTo(expectedAssignmentInfo);

    Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults =
        runPartitioner(partitioner, numPartitions);
    validatePartitionResults(expectedAssignmentInfo, partitionResults, 5.0);
  }

  @Test
  public void testNonDividableNoClosingFileCost() {
    MapRangePartitioner partitioner =
        new MapRangePartitioner(TestFixtures.SCHEMA, SORT_ORDER, mapDataStatistics, 0.0);
    int numPartitions = 9;

    // before:     350, 230, 120, 40, 10, 10, 10, 10, 10, 10
    // each task should get targeted weight of 89 = ceiling(800/9)
    Map<SortKey, MapRangePartitioner.KeyAssignment> expectedAssignment =
        ImmutableMap.of(
            SORT_KEYS[0],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(0, 1, 2, 3), ImmutableList.of(89L, 89L, 89L, 83L), 0L),
            SORT_KEYS[1],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(3, 4, 5, 6), ImmutableList.of(6L, 89L, 89L, 46L), 0L),
            SORT_KEYS[2],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(6, 7), ImmutableList.of(43L, 77L), 0L),
            SORT_KEYS[3],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(7, 8), ImmutableList.of(12L, 28L), 0L),
            SORT_KEYS[4],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L),
            SORT_KEYS[5],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L),
            SORT_KEYS[6],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L),
            SORT_KEYS[7],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L),
            SORT_KEYS[8],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L),
            SORT_KEYS[9],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(10L), 0L));
    Map<SortKey, MapRangePartitioner.KeyAssignment> actualAssignment =
        partitioner.assignment(numPartitions);
    Assertions.assertThat(actualAssignment).isEqualTo(expectedAssignment);

    // key: subtask id
    // value pair: first is the assigned weight, second is the number of assigned keys
    Map<Integer, Pair<Long, Integer>> expectedAssignmentInfo =
        ImmutableMap.of(
            0,
            Pair.of(89L, 1),
            1,
            Pair.of(89L, 1),
            2,
            Pair.of(89L, 1),
            3,
            Pair.of(89L, 2),
            4,
            Pair.of(89L, 1),
            5,
            Pair.of(89L, 1),
            6,
            Pair.of(89L, 2),
            7,
            Pair.of(89L, 2),
            8,
            Pair.of(88L, 7));
    Map<Integer, Pair<Long, Integer>> actualAssignmentInfo = partitioner.assignmentInfo();
    Assertions.assertThat(actualAssignmentInfo).isEqualTo(expectedAssignmentInfo);

    Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults =
        runPartitioner(partitioner, numPartitions);
    validatePartitionResults(expectedAssignmentInfo, partitionResults, 5.0);
  }

  @Test
  public void testNonDividableWithClosingFileCost() {
    MapRangePartitioner partitioner =
        new MapRangePartitioner(TestFixtures.SCHEMA, SORT_ORDER, mapDataStatistics, 5.0);
    int numPartitions = 9;

    // target subtask weight is 89 before close file cost factored in.
    // close file cost is 5 (= 5% * 89) per file.
    // key weights before and after close file cost factored in
    // before:     350, 230, 120, 40, 10, 10, 10, 10, 10, 10
    // close-cost:  20,  15,  10,  5,  5,  5,  5,  5,  5,  5
    // after:      370, 245, 130, 45, 15, 15, 15, 15, 15, 15
    // target subtask weight per subtask is 98 ceiling(880/9)
    Map<SortKey, MapRangePartitioner.KeyAssignment> expectedAssignment =
        ImmutableMap.of(
            SORT_KEYS[0],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(0, 1, 2, 3), ImmutableList.of(98L, 98L, 98L, 76L), 5L),
            SORT_KEYS[1],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(3, 4, 5, 6), ImmutableList.of(22L, 98L, 98L, 27L), 5L),
            SORT_KEYS[2],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(6, 7), ImmutableList.of(71L, 59L), 5L),
            SORT_KEYS[3],
            new MapRangePartitioner.KeyAssignment(
                ImmutableList.of(7, 8), ImmutableList.of(39L, 6L), 5L),
            SORT_KEYS[4],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L),
            SORT_KEYS[5],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L),
            SORT_KEYS[6],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L),
            SORT_KEYS[7],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L),
            SORT_KEYS[8],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L),
            SORT_KEYS[9],
            new MapRangePartitioner.KeyAssignment(ImmutableList.of(8), ImmutableList.of(15L), 5L));
    Map<SortKey, MapRangePartitioner.KeyAssignment> actualAssignment =
        partitioner.assignment(numPartitions);
    Assertions.assertThat(actualAssignment).isEqualTo(expectedAssignment);

    // key: subtask id
    // value pair: first is the assigned weight for the subtask, second is the number of keys
    // assigned to the subtask
    Map<Integer, Pair<Long, Integer>> expectedAssignmentInfo =
        ImmutableMap.of(
            0,
            Pair.of(93L, 1),
            1,
            Pair.of(93L, 1),
            2,
            Pair.of(93L, 1),
            3,
            Pair.of(88L, 2),
            4,
            Pair.of(93L, 1),
            5,
            Pair.of(93L, 1),
            6,
            Pair.of(88L, 2),
            7,
            Pair.of(88L, 2),
            8,
            Pair.of(61L, 7));
    Map<Integer, Pair<Long, Integer>> actualAssignmentInfo = partitioner.assignmentInfo();
    Assertions.assertThat(actualAssignmentInfo).isEqualTo(expectedAssignmentInfo);

    Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults =
        runPartitioner(partitioner, numPartitions);
    // drift threshold is high for non-dividable scenario with close cost
    validatePartitionResults(expectedAssignmentInfo, partitionResults, 10.0);
  }

  private static Map<Integer, Pair<AtomicLong, Set<RowData>>> runPartitioner(
      MapRangePartitioner partitioner, int numPartitions) {
    // The Map key is the subtaskId.
    // For the map value pair, the first element is the count of assigned and
    // the second element of Set<String> is for the set of assigned keys.
    Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults = Maps.newHashMap();
    partitioner
        .mapStatistics()
        .forEach(
            (sortKey, weight) -> {
              String key = sortKey.get(0, String.class);
              // run 100x times of the weight
              long iterations = weight * 100;
              for (int i = 0; i < iterations; ++i) {
                RowData rowData =
                    GenericRowData.of(
                        StringData.fromString(key), 1, StringData.fromString("2023-06-20"));
                int subtaskId = partitioner.partition(rowData, numPartitions);
                partitionResults.computeIfAbsent(
                    subtaskId, k -> Pair.of(new AtomicLong(0), Sets.newHashSet()));
                Pair<AtomicLong, Set<RowData>> pair = partitionResults.get(subtaskId);
                pair.first().incrementAndGet();
                pair.second().add(rowData);
              }
            });
    return partitionResults;
  }

  /** @param expectedAssignmentInfo excluding closing cost */
  private void validatePartitionResults(
      Map<Integer, Pair<Long, Integer>> expectedAssignmentInfo,
      Map<Integer, Pair<AtomicLong, Set<RowData>>> partitionResults,
      double maxDriftPercentage) {

    Assertions.assertThat(partitionResults.size()).isEqualTo(expectedAssignmentInfo.size());

    List<Integer> expectedAssignedKeyCounts =
        Lists.newArrayListWithExpectedSize(expectedAssignmentInfo.size());
    List<Integer> actualAssignedKeyCounts =
        Lists.newArrayListWithExpectedSize(partitionResults.size());
    List<Double> expectedNormalizedWeights =
        Lists.newArrayListWithExpectedSize(expectedAssignmentInfo.size());
    List<Double> actualNormalizedWeights =
        Lists.newArrayListWithExpectedSize(partitionResults.size());

    long expectedTotalWeight =
        expectedAssignmentInfo.values().stream().mapToLong(Pair::first).sum();
    expectedAssignmentInfo.forEach(
        (subtaskId, pair) -> {
          expectedAssignedKeyCounts.add(pair.second());
          expectedNormalizedWeights.add(pair.first().doubleValue() / expectedTotalWeight);
        });

    long actualTotalWeight =
        partitionResults.values().stream().mapToLong(pair -> pair.first().longValue()).sum();
    partitionResults.forEach(
        (subtaskId, pair) -> {
          actualAssignedKeyCounts.add(pair.second().size());
          actualNormalizedWeights.add(pair.first().doubleValue() / actualTotalWeight);
        });

    // number of assigned keys should match exactly
    Assertions.assertThat(actualAssignedKeyCounts)
        .as("the number of assigned keys should match for every subtask")
        .isEqualTo(expectedAssignedKeyCounts);

    // weight for every subtask shouldn't differ for more than some threshold relative to the
    // expected weight
    for (int subtaskId = 0; subtaskId < expectedNormalizedWeights.size(); ++subtaskId) {
      double expectedWeight = expectedNormalizedWeights.get(subtaskId);
      double min = expectedWeight * (1 - maxDriftPercentage / 100);
      double max = expectedWeight * (1 + maxDriftPercentage / 100);
      Assertions.assertThat(actualNormalizedWeights.get(subtaskId))
          .as(
              "Subtask %d weight should within %.1f percent of the expected range %s",
              subtaskId, maxDriftPercentage, expectedWeight)
          .isBetween(min, max);
    }
  }
}
