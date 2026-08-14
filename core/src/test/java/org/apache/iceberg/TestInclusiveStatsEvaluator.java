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
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.notNull;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
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

  private static final Schema LOCATION_SCHEMA =
      new Schema(
          optional(
              1,
              "location",
              Types.StructType.of(
                  required(2, "lat", Types.FloatType.get()),
                  optional(3, "lon", Types.FloatType.get()),
                  optional(4, "alt", Types.FloatType.get()))));

  private static final Types.StructType LOCATION_STATS_TYPE =
      StatsUtil.statsReadSchema(LOCATION_SCHEMA, ImmutableList.of(2, 3, 4));

  @Override
  protected boolean shouldRead(
      Schema schema, Expression expr, boolean caseSensitive, TrackedFile testFile) {
    return new InclusiveStatsEvaluator(schema, expr, caseSensitive)
        .eval(testFile.contentStats(), testFile.recordCount());
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
        "file_2.avro", 50, contentStats(STATS_TYPE, stats(3, "aa", "dC", 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file3() {
    return trackedFile(
        "file_3.avro", 50, contentStats(STATS_TYPE, stats(3, "1str1", "3str3", 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file4() {
    return trackedFile(
        "file_4.avro", 50, contentStats(STATS_TYPE, stats(3, "abc", "イロハニホヘト", 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file5() {
    return trackedFile(
        "file_5.avro", 50, contentStats(STATS_TYPE, stats(3, "abc", "abcdefghi", 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file6() {
    return trackedFile(
        "file_6.avro",
        10,
        contentStats(
            NESTED_STATS_TYPE,
            // required_address is present in every row and optional_street1 is always null
            stats(NESTED_STATS_TYPE, 102, null, null, 5L, null, null),
            stats(NESTED_STATS_TYPE, 103, null, null, 5L, 5L, null),
            // optional_address is null in every row, so the fields it contains have no values
            stats(NESTED_STATS_TYPE, 104, null, null, 5L, 5L, null),
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
        contentStats(STATS_TYPE, stats(3, "aaa", "zzz", 10L, 0L, null)));
  }

  @Override
  protected TrackedFile singleValueFile() {
    return trackedFile(
        "single_value.avro", 10, contentStats(STATS_TYPE, stats(3, "abc", "abc", 10L, 0L, null)));
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
        contentStats(FLOAT_STATS_TYPE, stats(FLOAT_STATS_TYPE, 1, 1.0f, 1.0f, 10L, 0L, 0L)));
  }

  @Override
  protected TrackedFile singleFloatValueFileWithNaN() {
    return trackedFile(
        "single_value_file.avro",
        10,
        contentStats(FLOAT_STATS_TYPE, stats(FLOAT_STATS_TYPE, 1, 1.0f, 1.0f, 10L, 0L, 1L)));
  }

  @Test
  void nullsForFieldsInPartiallyPresentStruct() {
    // location is null in 2 of the 5 rows:
    //   {"lat": 1.0, "lon": 1.0, "alt": null}
    //   {"lat": 2.0, "lon": 2.0, "alt": 0.0}
    //   {"lat": 3.0, "lon": 3.0, "alt": null}
    //   null
    //   null
    // rows where location is null are not counted in the value count of the fields it contains, so
    // each of them has 3 values. lat is required and tracks no null count, lon has a value in every
    // row where location is present, and alt is null in 2 of those rows
    TrackedFile file =
        trackedFile(
            "partially_present.avro",
            5,
            contentStats(
                LOCATION_STATS_TYPE,
                stats(LOCATION_STATS_TYPE, 2, 1.0f, 3.0f, 3L, null, 0L),
                stats(LOCATION_STATS_TYPE, 3, 1.0f, 3.0f, 3L, 0L, 0L),
                stats(LOCATION_STATS_TYPE, 4, 0.0f, 0.0f, 3L, 2L, 0L)));

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.lat"), file))
        .as("Should read: location.lat is null in 2 rows")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, notNull("location.lat"), file))
        .as("Should read: location.lat has a value in 3 rows")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.lon"), file))
        .as("Should read: location.lon is null in the 2 rows where location is null")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, notNull("location.lon"), file))
        .as("Should read: location.lon has a value in 3 rows")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.alt"), file))
        .as("Should read: location.alt is null in 4 rows")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, notNull("location.alt"), file))
        .as("Should read: location.alt has a value in 1 row")
        .isTrue();
  }

  @Test
  void nullsForFieldsInFullyPresentStruct() {
    // location is present in every row, so every field it contains has 5 values. lat is required
    // and tracks no null count, lon does not track a null count either, which leaves whether it is
    // null unknown, and alt is not null in any row
    TrackedFile file =
        trackedFile(
            "fully_present.avro",
            5,
            contentStats(
                LOCATION_STATS_TYPE,
                stats(LOCATION_STATS_TYPE, 2, 1.0f, 5.0f, 5L, 0L, 0L),
                stats(LOCATION_STATS_TYPE, 3, 1.0f, 5.0f, 5L, null, 0L),
                stats(LOCATION_STATS_TYPE, 4, 1.0f, 5.0f, 5L, 0L, 0L)));

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.lat"), file))
        .as("Should not read: location.lat has a value in every row")
        .isFalse();

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.lon"), file))
        .as("Should read: location.lon does not track a null count")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, isNull("location.alt"), file))
        .as("Should not read: location.alt has a value in every row")
        .isFalse();

    assertThat(shouldRead(LOCATION_SCHEMA, notNull("location.lat"), file))
        .as("Should read: location.lat has a value in every row")
        .isTrue();

    assertThat(shouldRead(LOCATION_SCHEMA, notNull("location.alt"), file))
        .as("Should read: location.alt has a value in every row")
        .isTrue();
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
