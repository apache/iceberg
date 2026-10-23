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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.ToDoubleFunction;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.Test;

public class TestKeyFilters {

  private static final ToDoubleFunction<Object> NUMERIC = value -> ((Number) value).doubleValue();

  private static final Schema KEY_SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Schema TWO_COLUMN_KEY_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.required(2, "tenant", Types.LongType.get()));

  @Test
  public void testComparisonsPerFileFollowsDataFileCount() {
    assertThat(KeyFilters.comparisonsPerFile(snapshot("1000000"))).isEqualTo(200);
    assertThat(KeyFilters.comparisonsPerFile(snapshot("400000"))).isEqualTo(500);
    assertThat(KeyFilters.comparisonsPerFile(snapshot("200000"))).isEqualTo(1000);
    assertThat(KeyFilters.comparisonsPerFile(snapshot("2000"))).isEqualTo(1000);
    // without a usable count the filter stays as small as for the largest table
    assertThat(KeyFilters.comparisonsPerFile(snapshot(null))).isEqualTo(200);
    assertThat(KeyFilters.comparisonsPerFile(snapshot("not a number"))).isEqualTo(200);
    // an empty table can afford the largest filter
    assertThat(KeyFilters.comparisonsPerFile(snapshot("0"))).isEqualTo(1000);
    assertThat(KeyFilters.comparisonsPerFile(null)).isEqualTo(1000);
  }

  @Test
  public void testListsSelectOnlyFilesHoldingKeys() {
    // 300 hot keys and 150 scattered keys, one every 50 ids
    StructLikeSet keys = StructLikeSet.create(KEY_SCHEMA.asStruct());
    for (long id = 9700; id < 10000; id++) {
      keys.add(key(id));
    }
    for (long file = 0; file < 150; file++) {
      keys.add(key(file * 50 + 7));
    }

    Expression lists = KeyFilters.keyFilter(KEY_SCHEMA, keys, 2000);
    assertThat(lists.toString()).contains(" in (").doesNotContain(" <= ");

    // a file between two scattered keys holds no key and is pruned by the lists
    assertThat(mightMatch(lists, 20, 39)).isFalse();
    // files holding a scattered key or the hot keys are kept
    assertThat(mightMatch(lists, 0, 49)).isTrue();
    assertThat(mightMatch(lists, 9750, 9799)).isTrue();
    // files past the last key are pruned
    assertThat(mightMatch(lists, 10000, 10049)).isFalse();
  }

  @Test
  public void testRangesAboveTheBudget() {
    StructLikeSet keys = StructLikeSet.create(KEY_SCHEMA.asStruct());
    for (long id = 9700; id < 10000; id++) {
      keys.add(key(id));
    }
    for (long file = 0; file < 150; file++) {
      keys.add(key(file * 50 + 7));
    }

    // a budget of 200 comparisons allows 100 ranges, fewer than the 151 clusters of keys
    Expression ranges = KeyFilters.keyFilter(KEY_SCHEMA, keys, 200);
    assertThat(ranges.toString()).contains(" <= ").doesNotContain(" in (");

    // the hot cluster and files far from any key are handled like the lists do
    assertThat(mightMatch(ranges, 9750, 9799)).isTrue();
    assertThat(mightMatch(ranges, 10000, 10049)).isFalse();
    // but some scattered keys share a range, so a file between them cannot be pruned everywhere
    int keptFilesWithoutKeys = 0;
    for (long file = 0; file < 150; file++) {
      long first = file * 50 + 20;
      if (mightMatch(ranges, first, first + 19)) {
        keptFilesWithoutKeys++;
      }
    }
    assertThat(keptFilesWithoutKeys).isGreaterThan(0);
  }

  @Test
  public void testEveryKeyColumnCountsTowardsTheBudget() {
    StructLikeSet keys = StructLikeSet.create(TWO_COLUMN_KEY_SCHEMA.asStruct());
    for (long id = 0; id < 150; id++) {
      Record key = GenericRecord.create(TWO_COLUMN_KEY_SCHEMA);
      key.set(0, id);
      key.set(1, id % 4);
      keys.add(key);
    }

    // 150 keys x 2 columns = 300 comparisons: within a budget of 300, above a budget of 299
    assertThat(KeyFilters.keyFilter(TWO_COLUMN_KEY_SCHEMA, keys, 300).toString()).contains(" in (");
    assertThat(KeyFilters.keyFilter(TWO_COLUMN_KEY_SCHEMA, keys, 299).toString()).contains(" <= ");
  }

  @Test
  public void testAdjacentIntegersFormOneRange() {
    // 200 consecutive values and a cluster of 100 far away: the only gap that leaves out a value
    // is the one between 199 and 1000
    List<int[]> ranges = KeyFilters.splitIntoRanges(clusters(), NUMERIC, 100);

    assertThat(ranges).hasSize(2);
    assertThat(ranges.get(0)).containsExactly(0, 199);
    assertThat(ranges.get(1)).containsExactly(200, 299);
  }

  @Test
  public void testScatteredValuesAreCappedAtTheRangeLimit() {
    List<Object> scattered = Lists.newArrayList();
    for (long value = 0; value < 300; value++) {
      scattered.add(value * 10);
    }

    List<int[]> ranges = KeyFilters.splitIntoRanges(scattered, NUMERIC, 100);

    // every gap leaves out values, so the widest ones split the values and the ranges still cover
    // every value
    assertThat(ranges).hasSize(100);
    assertThat(ranges.get(0)[0]).isEqualTo(0);
    assertThat(ranges.get(ranges.size() - 1)[1]).isEqualTo(scattered.size() - 1);
    for (int i = 1; i < ranges.size(); i++) {
      assertThat(ranges.get(i)[0]).isEqualTo(ranges.get(i - 1)[1] + 1);
    }
  }

  @Test
  public void testValuesWithoutNumericGapsAreSplitEvenly() {
    List<int[]> ranges = KeyFilters.splitIntoRanges(clusters(), null, 100);

    assertThat(ranges).hasSize(100);
    assertThat(ranges).allMatch(range -> range[1] - range[0] + 1 == 3);
  }

  @Test
  public void testFewerValuesThanRangesGetOneRangeEach() {
    List<int[]> ranges = KeyFilters.splitIntoRanges(clusters().subList(0, 5), null, 100);

    assertThat(ranges).hasSize(5);
    assertThat(ranges).allMatch(range -> range[0] == range[1]);
  }

  private static List<Object> clusters() {
    List<Object> values = Lists.newArrayList();
    for (long value = 0; value < 200; value++) {
      values.add(value);
    }
    for (long value = 1000; value < 1100; value++) {
      values.add(value);
    }

    return values;
  }

  private static Snapshot snapshot(String totalDataFiles) {
    Snapshot snapshot = mock(Snapshot.class);
    Map<String, String> summary =
        totalDataFiles == null
            ? ImmutableMap.of()
            : ImmutableMap.of("total-data-files", totalDataFiles);
    when(snapshot.summary()).thenReturn(summary);
    return snapshot;
  }

  private static Record key(long id) {
    Record key = GenericRecord.create(KEY_SCHEMA);
    key.set(0, id);
    return key;
  }

  /** Evaluates the filter against a data file whose id column spans [lower, upper]. */
  private static boolean mightMatch(Expression filter, long lower, long upper) {
    ByteBuffer lowerBound = Conversions.toByteBuffer(Types.LongType.get(), lower);
    ByteBuffer upperBound = Conversions.toByteBuffer(Types.LongType.get(), upper);
    long rows = upper - lower + 1;
    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath("/data/" + lower + ".parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1)
            .withMetrics(
                new Metrics(
                    rows,
                    ImmutableMap.of(1, rows),
                    ImmutableMap.of(1, rows),
                    ImmutableMap.of(1, 0L),
                    null,
                    ImmutableMap.of(1, lowerBound),
                    ImmutableMap.of(1, upperBound)))
            .build();
    return new InclusiveMetricsEvaluator(KEY_SCHEMA, filter).eval(file);
  }
}
