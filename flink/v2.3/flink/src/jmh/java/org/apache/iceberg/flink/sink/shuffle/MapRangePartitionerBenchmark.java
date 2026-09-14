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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
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
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class MapRangePartitionerBenchmark {

  private static final int SAMPLE_SIZE = 100_000;
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "name2", Types.StringType.get()),
          Types.NestedField.required(3, "name3", Types.StringType.get()),
          Types.NestedField.required(4, "name4", Types.StringType.get()),
          Types.NestedField.required(5, "name5", Types.StringType.get()),
          Types.NestedField.required(6, "name6", Types.StringType.get()),
          Types.NestedField.required(7, "name7", Types.StringType.get()),
          Types.NestedField.required(8, "name8", Types.StringType.get()),
          Types.NestedField.required(9, "name9", Types.StringType.get()));

  private static final SortOrder SORT_ORDER = SortOrder.builderFor(SCHEMA).asc("id").build();
  private static final Comparator<StructLike> SORT_ORDER_COMPARTOR =
      SortOrderComparators.forSchema(SCHEMA, SORT_ORDER);
  private static final SortKey SORT_KEY = new SortKey(SCHEMA, SORT_ORDER);
  private static final int PARALLELISM = 100;

  private MapRangePartitioner partitioner;
  private RowData[] rows;

  @Setup
  public void setupBenchmark() {
    NavigableMap<Integer, Long> weights =
        DataDistributionUtil.longTailDistribution(100_000, 24, 240, 100, 2.0, 0.7);
    Map<SortKey, Long> mapStatistics =
        DataDistributionUtil.mapStatisticsWithLongTailDistribution(weights, SORT_KEY);

    MapAssignment mapAssignment =
        MapAssignment.fromKeyFrequency(PARALLELISM, mapStatistics, 0.0, SORT_ORDER_COMPARTOR);
    this.partitioner = new MapRangePartitioner(SCHEMA, SORT_ORDER, mapAssignment);

    List<Integer> keys = Lists.newArrayList(weights.keySet().iterator());
    long[] weightsCDF = DataDistributionUtil.computeCumulativeWeights(keys, weights);
    long totalWeight = weightsCDF[weightsCDF.length - 1];

    // pre-calculate the samples for benchmark run
    this.rows = new GenericRowData[SAMPLE_SIZE];
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      long weight = ThreadLocalRandom.current().nextLong(totalWeight);
      int index = DataDistributionUtil.binarySearchIndex(weightsCDF, weight);
      rows[i] =
          GenericRowData.of(
              keys.get(index),
              DataDistributionUtil.randomString("name2-", 200),
              DataDistributionUtil.randomString("name3-", 200),
              DataDistributionUtil.randomString("name4-", 200),
              DataDistributionUtil.randomString("name5-", 200),
              DataDistributionUtil.randomString("name6-", 200),
              DataDistributionUtil.randomString("name7-", 200),
              DataDistributionUtil.randomString("name8-", 200),
              DataDistributionUtil.randomString("name9-", 200));
    }
  }

  @TearDown
  public void tearDownBenchmark() {}

  @Benchmark
  @Threads(1)
  public void testPartitionerLongTailDistribution(Blackhole blackhole) {
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      blackhole.consume(partitioner.partition(rows[i], PARALLELISM));
    }
  }
}
