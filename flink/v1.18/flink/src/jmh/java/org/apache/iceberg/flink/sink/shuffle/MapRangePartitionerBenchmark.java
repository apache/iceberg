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

import java.nio.charset.StandardCharsets;
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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
  private static final String CHARS =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-.!?";
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

  private MapRangePartitioner partitioner;
  private RowData[] rows;

  @Setup
  public void setupBenchmark() {
    NavigableMap<Integer, Long> weights = longTailDistribution(100_000, 24, 240, 100, 2.0);
    Map<SortKey, Long> mapStatistics = Maps.newHashMapWithExpectedSize(weights.size());
    weights.forEach(
        (id, weight) -> {
          SortKey sortKey = SORT_KEY.copy();
          sortKey.set(0, id);
          mapStatistics.put(sortKey, weight);
        });

    MapAssignment mapAssignment =
        MapAssignment.fromKeyFrequency(2, mapStatistics, 0.0, SORT_ORDER_COMPARTOR);
    this.partitioner =
        new MapRangePartitioner(
            SCHEMA, SortOrder.builderFor(SCHEMA).asc("id").build(), mapAssignment);

    List<Integer> keys = Lists.newArrayList(weights.keySet().iterator());
    long[] weightsCDF = new long[keys.size()];
    long totalWeight = 0;
    for (int i = 0; i < keys.size(); ++i) {
      totalWeight += weights.get(keys.get(i));
      weightsCDF[i] = totalWeight;
    }

    // pre-calculate the samples for benchmark run
    this.rows = new GenericRowData[SAMPLE_SIZE];
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      long weight = ThreadLocalRandom.current().nextLong(totalWeight);
      int index = binarySearchIndex(weightsCDF, weight);
      rows[i] =
          GenericRowData.of(
              keys.get(index),
              randomString("name2-"),
              randomString("name3-"),
              randomString("name4-"),
              randomString("name5-"),
              randomString("name6-"),
              randomString("name7-"),
              randomString("name8-"),
              randomString("name9-"));
    }
  }

  @TearDown
  public void tearDownBenchmark() {}

  @Benchmark
  @Threads(1)
  public void testPartitionerLongTailDistribution(Blackhole blackhole) {
    for (int i = 0; i < SAMPLE_SIZE; ++i) {
      blackhole.consume(partitioner.partition(rows[i], 128));
    }
  }

  private static String randomString(String prefix) {
    int length = ThreadLocalRandom.current().nextInt(200);
    byte[] buffer = new byte[length];

    for (int i = 0; i < length; i += 1) {
      buffer[i] = (byte) CHARS.charAt(ThreadLocalRandom.current().nextInt(CHARS.length()));
    }

    // CHARS is all ASCII
    return prefix + new String(buffer, StandardCharsets.US_ASCII);
  }

  /** find the index where weightsUDF[index] < weight && weightsUDF[index+1] >= weight */
  private static int binarySearchIndex(long[] weightsUDF, long target) {
    Preconditions.checkArgument(
        target < weightsUDF[weightsUDF.length - 1],
        "weight is out of range: total weight = %s, search target = %s",
        weightsUDF[weightsUDF.length - 1],
        target);
    int start = 0;
    int end = weightsUDF.length - 1;
    while (start < end) {
      int mid = (start + end) / 2;
      if (weightsUDF[mid] < target && weightsUDF[mid + 1] >= target) {
        return mid;
      }

      if (weightsUDF[mid] >= target) {
        end = mid - 1;
      } else if (weightsUDF[mid + 1] < target) {
        start = mid + 1;
      }
    }
    return start;
  }

  /** Key is the id string and value is the weight in long value. */
  private static NavigableMap<Integer, Long> longTailDistribution(
      long startingWeight,
      int longTailStartingIndex,
      int longTailLength,
      long longTailBaseWeight,
      double weightRandomJitterPercentage) {

    NavigableMap<Integer, Long> weights = Maps.newTreeMap();

    // first part just decays the weight by half
    long currentWeight = startingWeight;
    for (int index = 0; index < longTailStartingIndex; ++index) {
      double jitter = ThreadLocalRandom.current().nextDouble(weightRandomJitterPercentage / 100);
      long weight = (long) (currentWeight * (1.0 + jitter));
      weight = weight > 0 ? weight : 1;
      weights.put(index, weight);
      if (currentWeight > longTailBaseWeight) {
        currentWeight = currentWeight / 2;
      }
    }

    // long tail part
    for (int index = longTailStartingIndex;
        index < longTailStartingIndex + longTailLength;
        ++index) {
      long longTailWeight =
          (long)
              (longTailBaseWeight
                  * ThreadLocalRandom.current().nextDouble(weightRandomJitterPercentage));
      longTailWeight = longTailWeight > 0 ? longTailWeight : 1;
      weights.put(index, longTailWeight);
    }

    return weights;
  }
}
