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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsUtil {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "measurement", Types.DoubleType.get()));

  private static final int DROPPED_FIELD_ID = 99;

  @Test
  public void testPruneColumnStatsRespectsPerColumnModes() {
    Metrics metrics = metricsForAllColumns();
    MetricsConfig config =
        MetricsConfig.from(
            ImmutableMap.of(
                "write.metadata.metrics.column.id", "full",
                "write.metadata.metrics.column.data", "counts",
                "write.metadata.metrics.column.measurement", "none"),
            SCHEMA,
            SortOrder.unsorted());

    Metrics pruned = MetricsUtil.pruneColumnStats(SCHEMA, config, metrics);

    assertThat(pruned.valueCounts()).containsKeys(1, 2).doesNotContainKey(3);
    assertThat(pruned.columnSizes()).containsKeys(1, 2).doesNotContainKey(3);
    assertThat(pruned.lowerBounds()).containsKey(1).doesNotContainKeys(2, 3);
    assertThat(pruned.upperBounds()).containsKey(1).doesNotContainKeys(2, 3);
  }

  @Test
  public void testPruneColumnStatsKeepsStatsForFieldsMissingFromSchema() {
    Metrics metrics = metricsForAllColumns();
    MetricsConfig config =
        MetricsConfig.from(
            ImmutableMap.of("write.metadata.metrics.default", "none"),
            SCHEMA,
            SortOrder.unsorted());

    Metrics pruned = MetricsUtil.pruneColumnStats(SCHEMA, config, metrics);

    assertThat(pruned.valueCounts()).containsOnlyKeys(DROPPED_FIELD_ID);
    assertThat(pruned.lowerBounds()).containsOnlyKeys(DROPPED_FIELD_ID);
  }

  @Test
  public void testPruneColumnStatsDoesNotReTruncateBounds() {
    ByteBuffer lowerBound = stringBuffer("aaaaaaaa");
    ByteBuffer upperBound = stringBuffer("zzzzzzzz");
    Metrics metrics =
        new Metrics(
            10L,
            ImmutableMap.of(2, 200L),
            ImmutableMap.of(2, 20L),
            ImmutableMap.of(2, 0L),
            null,
            ImmutableMap.of(2, lowerBound),
            ImmutableMap.of(2, upperBound));
    MetricsConfig config =
        MetricsConfig.from(
            ImmutableMap.of("write.metadata.metrics.column.data", "truncate(2)"),
            SCHEMA,
            SortOrder.unsorted());

    Metrics pruned = MetricsUtil.pruneColumnStats(SCHEMA, config, metrics);

    assertThat(pruned.lowerBounds().get(2)).isEqualTo(lowerBound);
    assertThat(pruned.upperBounds().get(2)).isEqualTo(upperBound);
  }

  private static Metrics metricsForAllColumns() {
    return new Metrics(
        10L,
        ImmutableMap.of(1, 100L, 2, 200L, 3, 300L, DROPPED_FIELD_ID, 400L),
        ImmutableMap.of(1, 10L, 2, 20L, 3, 30L, DROPPED_FIELD_ID, 40L),
        ImmutableMap.of(1, 0L, 2, 0L, 3, 0L, DROPPED_FIELD_ID, 0L),
        ImmutableMap.of(3, 1L),
        ImmutableMap.of(
            1, intBuffer(1), 2, intBuffer(2), 3, intBuffer(3), DROPPED_FIELD_ID, intBuffer(4)),
        ImmutableMap.of(
            1, intBuffer(5), 2, intBuffer(6), 3, intBuffer(7), DROPPED_FIELD_ID, intBuffer(8)));
  }

  private static ByteBuffer intBuffer(int value) {
    return Conversions.toByteBuffer(Types.IntegerType.get(), value);
  }

  private static ByteBuffer stringBuffer(String value) {
    return Conversions.toByteBuffer(Types.StringType.get(), value);
  }
}
