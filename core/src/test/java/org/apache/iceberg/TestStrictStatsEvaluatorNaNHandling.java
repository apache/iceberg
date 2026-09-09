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

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.StrictStatsEvaluator;
import org.apache.iceberg.expressions.TestMetricsEvaluatorsNaNHandling;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;

class TestStrictStatsEvaluatorNaNHandling extends TestMetricsEvaluatorsNaNHandling<TrackedFile> {
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(SCHEMA, ImmutableList.of(1, 2, 3, 4, 5));

  @Override
  protected boolean allRowsMatch(Schema schema, Expression expr, TrackedFile testFile) {
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
            // all_nan, max_nan and all_nan_null_bounds are required, so their stats do not track a
            // null count
            stats(1, Double.NaN, Double.NaN, 10L, null, 10L),
            stats(2, 7D, Double.NaN, 10L, null, null),
            stats(3, Float.NaN, Float.NaN, 10L, 0L, null),
            stats(4, null, null, 10L, null, 10L),
            stats(5, 7F, 22F, 10L, 0L, 5L)));
  }

  private static FieldStats<Object> stats(
      int fieldId, Object lower, Object upper, Long valueCount, Long nullCount, Long nanCount) {
    return fieldStats(STATS_TYPE, fieldId, lower, upper, valueCount, nullCount, nanCount);
  }
}
