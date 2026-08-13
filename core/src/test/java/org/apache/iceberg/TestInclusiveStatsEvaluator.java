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
package org.apache.iceberg;

import static org.apache.iceberg.StatsTestUtil.contentStats;
import static org.apache.iceberg.StatsTestUtil.fieldStats;
import static org.apache.iceberg.StatsTestUtil.trackedFile;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.InclusiveStatsEvaluator;
import org.apache.iceberg.expressions.TestInclusiveMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestInclusiveStatsEvaluator extends TestInclusiveMetricsEvaluator<TrackedFile> {
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(
          SCHEMA, ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14));

  // stats are tracked for the leaf fields, not for the address structs themselves
  private static final Types.StructType NESTED_STATS_TYPE =
      StatsUtil.statsReadSchema(NESTED_SCHEMA, ImmutableList.of(102, 103, 104, 105));

  private static final Types.StructType FLOAT_STATS_TYPE =
      StatsUtil.statsReadSchema(FLOAT_SCHEMA, ImmutableList.of(1));

  @Override
  protected boolean shouldRead(
      Schema schema, Expression expr, boolean caseSensitive, TrackedFile testFile) {
    return new InclusiveStatsEvaluator(schema, expr, caseSensitive).eval(testFile);
  }

  @Override
  protected TrackedFile file() {
    return trackedFile(
        "file.avro",
        50,
        contentStats(
            STATS_TYPE,
            stats(1, INT_MIN_VALUE, INT_MAX_VALUE, null, null, null),
            stats(4, null, null, 50L, 50L, null),
            stats(5, null, null, 50L, 10L, null),
            stats(6, null, null, 50L, 0L, null),
            stats(7, null, null, 50L, null, 50L),
            stats(8, null, null, 50L, null, 10L),
            stats(9, null, null, 50L, null, 0L),
            stats(10, null, null, 50L, 50L, null),
            stats(11, Float.NaN, Float.NaN, 50L, 0L, null),
            stats(12, Double.NaN, Double.NaN, 50L, 1L, null),
            stats(13, null, null, 50L, null, null),
            stats(14, "", "房东整租霍营小区二层两居室", 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file2() {
    return trackedFile(
        "file_2.avro", 50, contentStats(STATS_TYPE, stats(3, "aa", "dC", 50L, null, null)));
  }

  @Override
  protected TrackedFile file3() {
    return trackedFile(
        "file_3.avro", 50, contentStats(STATS_TYPE, stats(3, "1str1", "3str3", 50L, null, null)));
  }

  @Override
  protected TrackedFile file4() {
    return trackedFile(
        "file_4.avro", 50, contentStats(STATS_TYPE, stats(3, "abc", "イロハニホヘト", 50L, null, null)));
  }

  @Override
  protected TrackedFile file5() {
    return trackedFile(
        "file_5.avro", 50, contentStats(STATS_TYPE, stats(3, "abc", "abcdefghi", 50L, null, null)));
  }

  @Override
  protected TrackedFile file6() {
    return trackedFile(
        "file_6.avro",
        10,
        contentStats(
            NESTED_STATS_TYPE,
            stats(NESTED_STATS_TYPE, 102, null, null, 5L, null, null),
            stats(NESTED_STATS_TYPE, 103, null, null, 5L, 5L, null),
            // required_street2 is required, so its stats do not track a null count
            stats(NESTED_STATS_TYPE, 104, null, null, 5L, null, null),
            stats(NESTED_STATS_TYPE, 105, null, null, 5L, 5L, null)));
  }

  @Override
  protected TrackedFile missingStats() {
    return trackedFile("file.parquet", 50, contentStats(STATS_TYPE));
  }

  @Override
  protected TrackedFile emptyFile() {
    return trackedFile("file.parquet", 0, contentStats(STATS_TYPE));
  }

  @Override
  protected TrackedFile rangeOfValues() {
    return trackedFile(
        "range_of_values.avro",
        10,
        contentStats(STATS_TYPE, stats(3, "aaa", "zzz", 10L, null, null)));
  }

  @Override
  protected TrackedFile singleValueFile() {
    return trackedFile(
        "single_value.avro", 10, contentStats(STATS_TYPE, stats(3, "abc", "abc", 10L, null, null)));
  }

  @Override
  protected TrackedFile singleValueWithNulls() {
    return trackedFile(
        "single_value_nulls.avro",
        10,
        contentStats(STATS_TYPE, stats(14, "abc", "abc", 10L, 2L, null)));
  }

  @Override
  protected TrackedFile singleValueWithNaN() {
    return trackedFile(
        "single_value_nan.avro", 10, contentStats(STATS_TYPE, stats(9, 5.0f, 5.0f, 10L, 0L, 2L)));
  }

  @Override
  protected TrackedFile singleValueNaNBounds() {
    return trackedFile(
        "single_value_nan_bounds.avro",
        10,
        contentStats(STATS_TYPE, stats(9, Float.NaN, Float.NaN, 10L, 0L, 0L)));
  }

  @Override
  protected TrackedFile singleFloatValueFile() {
    return trackedFile(
        "single_value_file.avro",
        10,
        contentStats(FLOAT_STATS_TYPE, stats(FLOAT_STATS_TYPE, 1, 1.0f, 1.0f, 10L, null, 0L)));
  }

  @Override
  protected TrackedFile singleFloatValueFileWithNaN() {
    return trackedFile(
        "single_value_file.avro",
        10,
        contentStats(FLOAT_STATS_TYPE, stats(FLOAT_STATS_TYPE, 1, 1.0f, 1.0f, 10L, null, 1L)));
  }

  /**
   * Content stats omit the null count for a required field, even when an optional struct contains
   * it, so a file where every value is null cannot be pruned.
   */
  @Override
  @Test
  public void notNullForRequiredFieldInOptionalStruct() {
    boolean shouldRead =
        shouldRead(NESTED_SCHEMA, notNull("optional_address.required_street2"), file6());
    assertThat(shouldRead).as("Should read: the null count is not tracked").isTrue();
  }

  @Test
  void fileWithoutContentStats() {
    TrackedFile file = trackedFile("file.avro", 50, null);

    assertThat(shouldRead(SCHEMA, lessThan("id", INT_MIN_VALUE), file))
        .as("Should read: file does not track content stats")
        .isTrue();
  }

  private static FieldStats<Object> stats(
      int fieldId, Object lower, Object upper, Long valueCount, Long nullCount, Long nanCount) {
    return stats(STATS_TYPE, fieldId, lower, upper, valueCount, nullCount, nanCount);
  }

  private static FieldStats<Object> stats(
      Types.StructType statsType,
      int fieldId,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount) {
    return fieldStats(statsType, fieldId, lower, upper, valueCount, nullCount, nanCount);
  }
}
