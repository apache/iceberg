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
import org.mockito.Mockito;

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
    POPULATED_STATS.setStats(1, mockFieldStats("req", 1, 1L, 5L, 10L, null, null));
    POPULATED_STATS.setStats(2, mockFieldStats("opt", 2, 2L, 6L, 20L, 3L, null));
    POPULATED_STATS.setStats(3, mockFieldStats("dbl", 3, 1.0, 9.0, 30L, 7L, 4L));

    // only a required long column: tracks a value count but no null or NaN count
    ONLY_REQUIRED_STATS.setStats(1, mockFieldStats("req", 1, 1L, 5L, 10L, null, null));
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
  public void testGetDoesNotUnboxUntrackedNullCount() {
    // a deserialized required-column struct leaves null_value_count null; get must not unbox it.
    // Column 2 tracks the null count, so the view itself is non-null.
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(1, mockFieldStats("req", 1, null, null, null, null, null));
    stats.setStats(2, mockFieldStats("opt", 2, 2L, 6L, 20L, 3L, null));
    Map<Integer, Long> map = ContentStatsBackedMap.nullValueCounts(stats);
    assertThat(map.get(1)).isNull();
    assertThat(map).containsOnly(Map.entry(2, 3L));
  }

  @Test
  public void testNanValueCountsOnlyForFloatingColumns() {
    Map<Integer, Long> map = ContentStatsBackedMap.nanValueCounts(POPULATED_STATS);
    assertThat(map).containsOnly(Map.entry(3, 4L));
  }

  @Test
  public void testLowerAndUpperBounds() {
    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(POPULATED_STATS);
    Map<Integer, ByteBuffer> upper = ContentStatsBackedMap.upperBounds(POPULATED_STATS);
    assertThat(lower.get(1)).isEqualTo(Conversions.toByteBuffer(Types.LongType.get(), 1L));
    assertThat(upper.get(3)).isEqualTo(Conversions.toByteBuffer(Types.DoubleType.get(), 9.0));
    assertThat(lower).containsOnlyKeys(1, 2, 3);
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
  public void testFactoryReturnsPopulatedViewWhenColumnsTrackMetric() {
    Map<Integer, Long> nanCounts = ContentStatsBackedMap.nanValueCounts(POPULATED_STATS);
    assertThat(nanCounts).isNotNull().containsOnly(Map.entry(3, 4L));

    Map<Integer, Long> nullCounts = ContentStatsBackedMap.nullValueCounts(POPULATED_STATS);
    assertThat(nullCounts).isNotNull().containsOnly(Map.entry(2, 3L), Map.entry(3, 7L));
  }

  @Test
  public void testPopulatedViewIsNotEmpty() {
    // isEmpty() answers from a scan without materializing entrySet
    assertThat(ContentStatsBackedMap.valueCounts(ONLY_REQUIRED_STATS)).isNotEmpty();
  }

  // A mock FieldStats presenting a column's stats through the interface (as a reader would after
  // deserialization). A null count reports absent via has*(); type() returns the real per-column
  // struct so lower/upper bounds decode against the right types.
  @SuppressWarnings("unchecked")
  private static FieldStats<Object> mockFieldStats(
      String name,
      int id,
      Object lower,
      Object upper,
      Long valueCount,
      Long nullCount,
      Long nanCount) {
    FieldStats<Object> stats = Mockito.mock(FieldStats.class);
    Mockito.when(stats.fieldId()).thenReturn(id);
    Mockito.when(stats.type()).thenReturn(statsType(name));
    Mockito.when(stats.lowerBound()).thenReturn(lower);
    Mockito.when(stats.upperBound()).thenReturn(upper);
    Mockito.when(stats.hasValueCount()).thenReturn(valueCount != null);
    Mockito.when(stats.hasNullValueCount()).thenReturn(nullCount != null);
    Mockito.when(stats.hasNanValueCount()).thenReturn(nanCount != null);
    if (valueCount != null) {
      Mockito.when(stats.valueCount()).thenReturn(valueCount);
    }

    if (nullCount != null) {
      Mockito.when(stats.nullValueCount()).thenReturn(nullCount);
    }

    if (nanCount != null) {
      Mockito.when(stats.nanValueCount()).thenReturn(nanCount);
    }

    return stats;
  }
}
