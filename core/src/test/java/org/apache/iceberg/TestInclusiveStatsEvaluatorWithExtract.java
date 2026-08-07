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

import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.TestInclusiveMetricsEvaluatorWithExtract;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantTestUtil;
import org.apache.iceberg.variants.VariantValue;
import org.apache.iceberg.variants.Variants;

class TestInclusiveStatsEvaluatorWithExtract
    extends TestInclusiveMetricsEvaluatorWithExtract<TrackedFile> {
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(SCHEMA, ImmutableList.of(1, 2, 3));

  private static final Variant LOWER_BOUND =
      VariantTestUtil.variant(
          Map.of("$['event_id']", Variants.of(INT_MIN_VALUE), "$['str']", Variants.of("abc")));

  private static final Variant UPPER_BOUND =
      VariantTestUtil.variant(
          Map.of("$['event_id']", Variants.of(INT_MAX_VALUE), "$['str']", Variants.of("abe")));

  @Override
  protected boolean shouldRead(Expression expr, TrackedFile testFile, boolean caseSensitive) {
    return new InclusiveStatsEvaluator(SCHEMA, expr, caseSensitive).eval(testFile);
  }

  @Override
  protected TrackedFile file() {
    return trackedFile(
        "file.avro",
        50,
        contentStats(
            STATS_TYPE,
            fieldStats(STATS_TYPE, 1, null, null, 50L, null, null),
            fieldStats(STATS_TYPE, 2, LOWER_BOUND, UPPER_BOUND, 50L, null, null),
            fieldStats(STATS_TYPE, 3, null, null, 50L, 50L, null)));
  }

  @Override
  protected TrackedFile emptyFile() {
    return trackedFile("file.parquet", 0, contentStats(STATS_TYPE));
  }

  @Override
  protected TrackedFile fileWithVariantBounds(String path, VariantValue lower, VariantValue upper) {
    return trackedFile(
        "file.parquet",
        50,
        contentStats(
            STATS_TYPE,
            fieldStats(
                STATS_TYPE,
                2,
                VariantTestUtil.variant(Map.of(path, lower)),
                VariantTestUtil.variant(Map.of(path, upper)),
                null,
                null,
                null)));
  }
}
