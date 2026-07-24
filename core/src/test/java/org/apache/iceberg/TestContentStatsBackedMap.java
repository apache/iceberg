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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestContentStatsBackedMap {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "req", Types.LongType.get()),
          optional(2, "opt", Types.LongType.get()),
          optional(3, "dbl", Types.DoubleType.get()));
  private static final Types.StructType STATS_TYPE =
      StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2, 3));

  private static Types.StructType statsType(String name) {
    return STATS_TYPE.field(name).type().asStructType();
  }

  private static final ContentStatsStruct POPULATED_STATS = new ContentStatsStruct(STATS_TYPE);
  private static final ContentStatsStruct ONLY_REQUIRED_STATS = new ContentStatsStruct(STATS_TYPE);

  static {
    POPULATED_STATS.setStats(
        1, StatsTestUtil.fieldStats(statsType("req"), 1L, 5L, false, 10L, null, null, null));
    POPULATED_STATS.setStats(
        2, StatsTestUtil.fieldStats(statsType("opt"), 2L, 6L, false, 20L, 3L, null, null));
    POPULATED_STATS.setStats(
        3, StatsTestUtil.fieldStats(statsType("dbl"), 1.0, 9.0, false, 30L, 7L, 4L, null));

    // only a required long column: tracks a value count but no null or NaN count
    ONLY_REQUIRED_STATS.setStats(
        1, StatsTestUtil.fieldStats(statsType("req"), 1L, 5L, false, 10L, null, null, null));
  }

  @Test
  public void testValueCounts() {
    Map<Integer, Long> map = ContentStatsBackedMap.valueCounts(POPULATED_STATS);
    assertThat(map).containsOnly(Map.entry(1, 10L), Map.entry(2, 20L), Map.entry(3, 30L));
  }

  @Test
  public void testNullValueCountsSkipsColumnsThatDoNotTrackIt() {
    // required column 1 does not track null_value_count; only the optional columns do
    Map<Integer, Long> map = ContentStatsBackedMap.nullValueCounts(POPULATED_STATS);
    assertThat(map.get(1)).isNull();
    assertThat(map.containsKey(1)).isFalse();
    assertThat(map).containsOnly(Map.entry(2, 3L), Map.entry(3, 7L));
  }

  @Test
  public void testNanValueCountsOnlyForFloatingColumns() {
    Map<Integer, Long> map = ContentStatsBackedMap.nanValueCounts(POPULATED_STATS);
    assertThat(map).containsOnly(Map.entry(3, 4L));
  }

  @Test
  public void testLowerBounds() {
    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(POPULATED_STATS);
    assertThat(lower)
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 1L)),
            Map.entry(2, Conversions.toByteBuffer(Types.LongType.get(), 2L)),
            Map.entry(3, Conversions.toByteBuffer(Types.DoubleType.get(), 1.0)));
  }

  @Test
  public void testUpperBounds() {
    Map<Integer, ByteBuffer> upper = ContentStatsBackedMap.upperBounds(POPULATED_STATS);
    assertThat(upper)
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 5L)),
            Map.entry(2, Conversions.toByteBuffer(Types.LongType.get(), 6L)),
            Map.entry(3, Conversions.toByteBuffer(Types.DoubleType.get(), 9.0)));
  }

  @Test
  public void testGetReturnsNullForMissingKey() {
    Map<Integer, Long> map = ContentStatsBackedMap.valueCounts(POPULATED_STATS);
    assertThat(map.get(999)).isNull();
  }

  @Test
  public void testGetReturnsNullForNonIntegerKey() {
    // consistent with common Map implementations: a wrong-typed key is absent, not an error
    Map<Integer, Long> map = ContentStatsBackedMap.valueCounts(POPULATED_STATS);
    assertThat(map.get("not-an-int")).isNull();
    assertThat(map.containsKey("not-an-int")).isFalse();
  }

  @Test
  public void testFactoryReturnsNullWhenNoColumnTracksMetric() {
    // only a required long column: it tracks neither null_value_count nor nan_value_count
    assertThat(ContentStatsBackedMap.nullValueCounts(ONLY_REQUIRED_STATS)).isNull();
    assertThat(ContentStatsBackedMap.nanValueCounts(ONLY_REQUIRED_STATS)).isNull();

    Map<Integer, Long> valueCounts = ContentStatsBackedMap.valueCounts(ONLY_REQUIRED_STATS);
    assertThat(valueCounts).isNotNull().containsOnly(Map.entry(1, 10L));
  }

  @Test
  public void testFactoryReturnsPopulatedViewWhenStatsStructsMissing() {
    // the schema includes all three columns but only the optional one has a stats struct; the
    // value-count map is still non-null and simply omits the missing columns
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(
        2, StatsTestUtil.fieldStats(statsType("opt"), 2L, 6L, false, 20L, 3L, null, null));

    Map<Integer, Long> valueCounts = ContentStatsBackedMap.valueCounts(stats);
    assertThat(valueCounts).isNotNull().containsOnly(Map.entry(2, 20L));
  }
}
