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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Comparator;
import java.util.DoubleSummaryStatistics;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortKey;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.SortOrderComparators;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestRangePartitionerSkew {
  private static final Logger LOG = LoggerFactory.getLogger(TestRangePartitionerSkew.class);

  // change the iterations to a larger number (like 100) to see the statistics of max skew.
  // like min, max, avg, stddev of max skew.
  private static final int ITERATIONS = 1;

  /**
   * @param parallelism number of partitions
   * @param maxSkewUpperBound the upper bound of max skew. maxSkewUpperBound is set to a loose bound
   *     (~5x of the max value) to avoid flakiness.
   *     <p>
   *     <li>Map parallelism 8: max skew statistics over 100 iterations: mean = 0.0124, min =
   *         0.0046, max = 0.0213
   *     <li>Map parallelism 32: max skew statistics over 100 iterations: mean = 0.0183, min =
   *         0.0100, max = 0.0261
   */
  @ParameterizedTest
  @CsvSource({"8, 100_000, 0.1", "32, 400_000, 0.15"})
  public void testMapStatisticsSkewWithLongTailDistribution(
      int parallelism, int sampleSize, double maxSkewUpperBound) {
    Schema schema =
        new Schema(Types.NestedField.optional(1, "event_hour", Types.IntegerType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("event_hour").build();
    Comparator<StructLike> comparator = SortOrderComparators.forSchema(schema, sortOrder);
    SortKey sortKey = new SortKey(schema, sortOrder);

    NavigableMap<Integer, Long> weights =
        DataDistributionUtil.longTailDistribution(100_000, 24, 240, 100, 2.0, 0.7);
    Map<SortKey, Long> mapStatistics =
        DataDistributionUtil.mapStatisticsWithLongTailDistribution(weights, sortKey);
    MapAssignment mapAssignment =
        MapAssignment.fromKeyFrequency(parallelism, mapStatistics, 0.0, comparator);
    MapRangePartitioner partitioner = new MapRangePartitioner(schema, sortOrder, mapAssignment);

    List<Integer> keys = Lists.newArrayList(weights.keySet().iterator());
    long[] weightsCDF = DataDistributionUtil.computeCumulativeWeights(keys, weights);
    long totalWeight = weightsCDF[weightsCDF.length - 1];

    // change the iterations to a larger number (like 100) to see the statistics of max skew.
    // like min, max, avg, stddev of max skew.
    double[] maxSkews = new double[ITERATIONS];
    for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
      int[] recordsPerTask = new int[parallelism];
      for (int i = 0; i < sampleSize; ++i) {
        // randomly pick a key according to the weight distribution
        long weight = ThreadLocalRandom.current().nextLong(totalWeight);
        int index = DataDistributionUtil.binarySearchIndex(weightsCDF, weight);
        RowData row = GenericRowData.of(keys.get(index));
        int subtaskId = partitioner.partition(row, parallelism);
        recordsPerTask[subtaskId] += 1;
      }

      IntSummaryStatistics recordsPerTaskStats = Arrays.stream(recordsPerTask).summaryStatistics();
      LOG.debug("Map parallelism {}: records per task stats: {}", parallelism, recordsPerTaskStats);
      double maxSkew =
          (recordsPerTaskStats.getMax() - recordsPerTaskStats.getAverage())
              / recordsPerTaskStats.getAverage();
      LOG.debug("Map parallelism {}: max skew: {}", parallelism, format("%.03f", maxSkew));
      assertThat(maxSkew).isLessThan(maxSkewUpperBound);
      maxSkews[iteration] = maxSkew;
    }

    DoubleSummaryStatistics maxSkewStats = Arrays.stream(maxSkews).summaryStatistics();
    LOG.info(
        "Map parallelism {}: max skew statistics over {} iterations: mean = {}, min = {}, max = {}",
        parallelism,
        ITERATIONS,
        format("%.4f", maxSkewStats.getAverage()),
        format("%.4f", maxSkewStats.getMin()),
        format("%.4f", maxSkewStats.getMax()));
  }

  /**
   * @param parallelism number of partitions
   * @param maxSkewUpperBound the upper bound of max skew. maxSkewUpperBound is set to a loose bound
   *     (~5x of the max value) to avoid flakiness.
   *     <p>
   *     <li>pMap parallelism 8: max skew statistics over 100 iterations: mean = 0.0192, min =
   *         0.0073, max = 0.0437
   *     <li>Map parallelism 32: max skew statistics over 100 iterations: mean = 0.0426, min =
   *         0.0262, max = 0.0613
   */
  @ParameterizedTest
  @CsvSource({"8, 100_000, 0.20", "32, 400_000, 0.25"})
  public void testSketchStatisticsSkewWithLongTailDistribution(
      int parallelism, int sampleSize, double maxSkewUpperBound) {
    Schema schema = new Schema(Types.NestedField.optional(1, "uuid", Types.UUIDType.get()));
    SortOrder sortOrder = SortOrder.builderFor(schema).asc("uuid").build();
    SortKey sortKey = new SortKey(schema, sortOrder);

    UUID[] reservoir = DataDistributionUtil.reservoirSampleUUIDs(1_000_000, 100_000);
    UUID[] rangeBound = DataDistributionUtil.rangeBoundSampleUUIDs(reservoir, parallelism);
    SortKey[] rangeBoundSortKeys =
        Arrays.stream(rangeBound)
            .map(
                uuid -> {
                  SortKey sortKeyCopy = sortKey.copy();
                  sortKeyCopy.set(0, uuid);
                  return sortKeyCopy;
                })
            .toArray(SortKey[]::new);

    SketchRangePartitioner partitioner =
        new SketchRangePartitioner(schema, sortOrder, rangeBoundSortKeys);

    double[] maxSkews = new double[ITERATIONS];
    for (int iteration = 0; iteration < ITERATIONS; ++iteration) {
      int[] recordsPerTask = new int[parallelism];
      for (int i = 0; i < sampleSize; ++i) {
        UUID uuid = UUID.randomUUID();
        Object uuidBytes = DataDistributionUtil.uuidBytes(uuid);
        RowData row = GenericRowData.of(uuidBytes);
        int subtaskId = partitioner.partition(row, parallelism);
        recordsPerTask[subtaskId] += 1;
      }

      IntSummaryStatistics recordsPerTaskStats = Arrays.stream(recordsPerTask).summaryStatistics();
      LOG.debug("Map parallelism {}: records per task stats: {}", parallelism, recordsPerTaskStats);
      double maxSkew =
          (recordsPerTaskStats.getMax() - recordsPerTaskStats.getAverage())
              / recordsPerTaskStats.getAverage();
      LOG.debug("Map parallelism {}: max skew: {}", parallelism, format("%.03f", maxSkew));
      assertThat(maxSkew).isLessThan(maxSkewUpperBound);
      maxSkews[iteration] = maxSkew;
    }

    DoubleSummaryStatistics maxSkewStats = Arrays.stream(maxSkews).summaryStatistics();
    LOG.info(
        "Map parallelism {}: max skew statistics over {} iterations: mean = {}, min = {}, max = {}",
        parallelism,
        ITERATIONS,
        format("%.4f", maxSkewStats.getAverage()),
        format("%.4f", maxSkewStats.getMin()),
        format("%.4f", maxSkewStats.getMax()));
  }
}
