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
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.StrictStatsEvaluator;
import org.apache.iceberg.expressions.TestStrictMetricsEvaluator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestStrictStatsEvaluator extends TestStrictMetricsEvaluator<TrackedFile> {
  // stats are tracked for the leaf fields, not for the struct itself
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(
          SCHEMA, ImmutableList.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18));

  @Override
  protected boolean shouldRead(Schema schema, Expression expr, TrackedFile testFile) {
    return new StrictStatsEvaluator(schema, expr)
        .eval(testFile.contentStats(), testFile.recordCount());
  }

  @Override
  protected TrackedFile file() {
    return trackedFile(
        "file.avro",
        50,
        contentStats(
            STATS_TYPE,
            // id is required, so its stats do not track a null count
            stats(1, INT_MIN_VALUE, INT_MAX_VALUE, null, null, null),
            stats(4, null, null, 50L, 50L, null),
            stats(5, null, null, 50L, 10L, null),
            stats(6, null, null, 50L, 0L, null),
            stats(7, 5, 5, null, null, null),
            stats(8, null, null, 50L, null, 50L),
            stats(9, null, null, 50L, null, 10L),
            stats(10, null, null, 50L, null, 0L),
            stats(11, null, null, 50L, 50L, null),
            stats(12, Float.NaN, Float.NaN, 50L, 0L, null),
            stats(13, Double.NaN, Double.NaN, 50L, 1L, null),
            stats(14, null, null, 50L, null, null),
            stats(17, INT_MIN_VALUE, INT_MAX_VALUE, 50L, 0L, null)));
  }

  @Override
  protected TrackedFile file2() {
    return trackedFile(
        "file_2.avro",
        50,
        contentStats(
            STATS_TYPE,
            stats(4, null, null, 50L, 50L, null),
            stats(5, "bbb", "eee", 50L, 10L, null),
            stats(6, null, null, 50L, 0L, null),
            stats(8, null, null, 50L, null, null)));
  }

  @Override
  protected TrackedFile file3() {
    return trackedFile(
        "file_3.avro",
        50,
        contentStats(
            STATS_TYPE,
            stats(4, null, null, 50L, 50L, null),
            stats(5, "bbb", "bbb", 50L, 10L, null),
            stats(6, null, null, 50L, 0L, null)));
  }

  @Override
  protected TrackedFile stringFile() {
    return trackedFile(
        "string_file.avro", 50, contentStats(STATS_TYPE, stats(3, "abc", "abd", 50L, null, null)));
  }

  @Override
  protected TrackedFile stringFile2() {
    return trackedFile(
        "string_file_2.avro", 50, contentStats(STATS_TYPE, stats(3, "aa", "dC", 50L, null, null)));
  }

  @Override
  protected TrackedFile missingNullCountsFile() {
    return trackedFile(
        "missing_null_counts.avro",
        50,
        contentStats(
            STATS_TYPE,
            stats(1, INT_MIN_VALUE, INT_MAX_VALUE, 50L, null, null),
            stats(2, INT_MIN_VALUE, INT_MAX_VALUE, 50L, null, null)));
  }

  @Override
  protected TrackedFile partialNullCountsFile() {
    return trackedFile(
        "partial_null_counts.avro",
        50,
        contentStats(
            STATS_TYPE,
            stats(1, INT_MIN_VALUE, INT_MAX_VALUE, 50L, null, null),
            stats(2, INT_MIN_VALUE, INT_MAX_VALUE, 50L, null, null),
            // the null count is tracked only for all_nulls
            stats(4, null, null, 50L, 0L, null)));
  }

  @Override
  protected TrackedFile floatFile() {
    return trackedFile(
        "float_file.avro",
        50,
        contentStats(
            STATS_TYPE, stats(10, 1.0F, 5.0F, 50L, 0L, 0L), stats(14, 1.0D, 5.0D, 50L, 0L, null)));
  }

  @Override
  protected TrackedFile missingStats() {
    return trackedFile("file.parquet", 50, contentStats(STATS_TYPE));
  }

  @Override
  protected TrackedFile emptyFile() {
    return trackedFile("file.parquet", 0, contentStats(STATS_TYPE));
  }

  @Test
  void fileWithoutContentStats() {
    TrackedFile file = trackedFile("file.avro", 50, null);

    assertThat(shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE + 1), file))
        .as("Should not match: file does not track content stats")
        .isFalse();
  }

  @Test
  void fileWithUnknownRecordCount() {
    TrackedFile file =
        trackedFile(
            "file.avro",
            -1,
            contentStats(STATS_TYPE, stats(1, INT_MIN_VALUE, INT_MAX_VALUE, 50L, null, null)));

    assertThat(shouldRead(SCHEMA, lessThan("id", INT_MAX_VALUE + 1), file))
        .as("Should not match: record count is unknown")
        .isFalse();
  }

  private static FieldStats<Object> stats(
      int fieldId, Object lower, Object upper, Long valueCount, Long nullCount, Long nanCount) {
    return fieldStats(STATS_TYPE, fieldId, lower, upper, valueCount, nullCount, nanCount);
  }
}
