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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestMetricsUtil {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "req", Types.LongType.get()),
          optional(2, "opt", Types.LongType.get()),
          optional(3, "dbl", Types.DoubleType.get()));
  private static final Types.StructType STATS = StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2, 3));

  private static Types.StructType statsType(String name) {
    return STATS.field(name).type().asStructType();
  }

  @Test
  public void testValueCounts() {
    ContentStatsStruct stats = new ContentStatsStruct(STATS);
    stats.setStats(1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, 0L, 0L, null));
    stats.setStats(2, new FieldStatsStruct<>(statsType("opt"), 1L, 5L, false, 20L, 0L, 0L, null));
    stats.setStats(3, new FieldStatsStruct<>(statsType("dbl"), 1.0, 5.0, false, 30L, 0L, 0L, null));

    // value_count is tracked for every column
    assertThat(MetricsUtil.valueCounts(stats))
        .containsOnly(Map.entry(1, 10L), Map.entry(2, 20L), Map.entry(3, 30L));
  }

  @Test
  public void testNullValueCountsSkipsColumnsThatDoNotTrackIt() {
    ContentStatsStruct stats = new ContentStatsStruct(STATS);
    // A required column has no null_value_count field; a deserialized struct leaves it null, so
    // nullValueCounts must not call the primitive accessor on it.
    stats.setStats(1, new FieldStatsStruct<Long>(statsType("req")));
    stats.setStats(2, new FieldStatsStruct<>(statsType("opt"), 1L, 5L, false, 20L, 3L, 0L, null));
    stats.setStats(3, new FieldStatsStruct<>(statsType("dbl"), 1.0, 5.0, false, 30L, 7L, 0L, null));

    // required column 1 is skipped; the optional columns are kept
    assertThat(MetricsUtil.nullValueCounts(stats)).containsOnly(Map.entry(2, 3L), Map.entry(3, 7L));
  }

  @Test
  public void testNanValueCountsOnlyForFloatingColumns() {
    ContentStatsStruct stats = new ContentStatsStruct(STATS);
    stats.setStats(2, new FieldStatsStruct<>(statsType("opt"), 1L, 5L, false, 20L, 0L, 0L, null));
    stats.setStats(3, new FieldStatsStruct<>(statsType("dbl"), 1.0, 5.0, false, 30L, 0L, 4L, null));

    // nan_value_count is tracked only for floating-point columns
    assertThat(MetricsUtil.nanValueCounts(stats)).containsOnly(Map.entry(3, 4L));
  }

  @Test
  public void testNullContentStatsReturnsNull() {
    assertThat(MetricsUtil.valueCounts(null)).isNull();
    assertThat(MetricsUtil.nullValueCounts(null)).isNull();
    assertThat(MetricsUtil.nanValueCounts(null)).isNull();
    assertThat(MetricsUtil.lowerBounds(null)).isNull();
    assertThat(MetricsUtil.upperBounds(null)).isNull();
  }
}
