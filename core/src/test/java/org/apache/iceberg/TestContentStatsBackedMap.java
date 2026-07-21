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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

  static {
    POPULATED_STATS.setStats(
        1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, null, null, null));
    POPULATED_STATS.setStats(
        2, new FieldStatsStruct<>(statsType("opt"), 2L, 6L, false, 20L, 3L, null, null));
    POPULATED_STATS.setStats(
        3, new FieldStatsStruct<>(statsType("dbl"), 1.0, 9.0, false, 30L, 7L, 4L, null));
  }

  @Test
  public void testValueCounts() {
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(POPULATED_STATS, ContentStatsBackedMap.Kind.VALUE_COUNT);
    assertThat(map).containsOnly(Map.entry(1, 10L), Map.entry(2, 20L), Map.entry(3, 30L));
  }

  @Test
  public void testNullValueCountsSkipsColumnsThatDoNotTrackIt() {
    // required column 1 does not track null_value_count; only the optional columns do
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(POPULATED_STATS, ContentStatsBackedMap.Kind.NULL_VALUE_COUNT);
    assertThat(map.get(1)).isNull();
    assertThat(map.containsKey(1)).isFalse();
    assertThat(map).containsOnly(Map.entry(2, 3L), Map.entry(3, 7L));
  }

  @Test
  public void testNullValueCountDoesNotThrowForUntrackedColumnWithNullValue() {
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    // a deserialized required-column struct leaves null_value_count null; get must not unbox it
    stats.setStats(1, new FieldStatsStruct<Long>(statsType("req")));
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(stats, ContentStatsBackedMap.Kind.NULL_VALUE_COUNT);
    assertThat(map.get(1)).isNull();
    assertThat(map).isEmpty();
  }

  @Test
  public void testNanValueCountsOnlyForFloatingColumns() {
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(POPULATED_STATS, ContentStatsBackedMap.Kind.NAN_VALUE_COUNT);
    assertThat(map).containsOnly(Map.entry(3, 4L));
  }

  @Test
  public void testLowerAndUpperBounds() {
    ContentStatsStruct stats = POPULATED_STATS;
    Map<Integer, ByteBuffer> lower =
        new ContentStatsBackedMap<>(stats, ContentStatsBackedMap.Kind.LOWER_BOUND);
    Map<Integer, ByteBuffer> upper =
        new ContentStatsBackedMap<>(stats, ContentStatsBackedMap.Kind.UPPER_BOUND);
    assertThat(lower.get(1)).isEqualTo(Conversions.toByteBuffer(Types.LongType.get(), 1L));
    assertThat(upper.get(3)).isEqualTo(Conversions.toByteBuffer(Types.DoubleType.get(), 9.0));
    assertThat(lower).containsOnlyKeys(1, 2, 3);
  }

  @Test
  public void testGetReturnsNullForMissingKey() {
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(POPULATED_STATS, ContentStatsBackedMap.Kind.VALUE_COUNT);
    assertThat(map.get(999)).isNull();
  }

  @Test
  public void testThrowsForNonIntegerKey() {
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(POPULATED_STATS, ContentStatsBackedMap.Kind.VALUE_COUNT);
    assertThatThrownBy(() -> map.get("not-an-int"))
        .isInstanceOf(ClassCastException.class)
        .hasMessageContaining("Key must be an Integer field id");
    assertThatThrownBy(() -> map.containsKey("not-an-int"))
        .isInstanceOf(ClassCastException.class)
        .hasMessageContaining("Key must be an Integer field id");
  }

  @Test
  public void testConstructorYieldsEmptyMapWhenNoColumnTracksMetric() {
    // a stats object with only a required column: null_value_count is tracked by no column. The raw
    // constructor yields a non-null empty view; forKind restores the eager converters' null instead
    // (see testForKindReturnsNullWhenNoColumnTracksMetric).
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(
        1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, null, null, null));
    Map<Integer, Long> map =
        new ContentStatsBackedMap<>(stats, ContentStatsBackedMap.Kind.NULL_VALUE_COUNT);
    assertThat(map).isNotNull().isEmpty();
  }

  @Test
  public void testForKindReturnsNullWhenNoColumnTracksMetric() {
    // only a required long column: it tracks neither null_value_count nor nan_value_count
    ContentStatsStruct stats = new ContentStatsStruct(STATS_TYPE);
    stats.setStats(
        1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, null, null, null));

    assertThat(ContentStatsBackedMap.forKind(ContentStatsBackedMap.Kind.NULL_VALUE_COUNT, stats))
        .isNull();
    assertThat(ContentStatsBackedMap.forKind(ContentStatsBackedMap.Kind.NAN_VALUE_COUNT, stats))
        .isNull();

    Map<Integer, Long> valueCounts =
        ContentStatsBackedMap.forKind(ContentStatsBackedMap.Kind.VALUE_COUNT, stats);
    assertThat(valueCounts).isNotNull().containsOnly(Map.entry(1, 10L));
  }

  @Test
  public void testForKindReturnsPopulatedViewWhenColumnsTrackMetric() {
    ContentStatsStruct stats = POPULATED_STATS;

    Map<Integer, Long> nanCounts =
        ContentStatsBackedMap.forKind(ContentStatsBackedMap.Kind.NAN_VALUE_COUNT, stats);
    assertThat(nanCounts).isNotNull().containsOnly(Map.entry(3, 4L));

    Map<Integer, Long> nullCounts =
        ContentStatsBackedMap.forKind(ContentStatsBackedMap.Kind.NULL_VALUE_COUNT, stats);
    assertThat(nullCounts).isNotNull().containsOnly(Map.entry(2, 3L), Map.entry(3, 7L));
  }

  @Test
  public void testIsEmptyScansWithoutMaterializing() {
    ContentStatsStruct onlyRequired = new ContentStatsStruct(STATS_TYPE);
    onlyRequired.setStats(
        1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, null, null, null));
    assertThat(
            ContentStatsBackedMap.isEmpty(
                onlyRequired, ContentStatsBackedMap.Kind.NULL_VALUE_COUNT))
        .isTrue();
    assertThat(
            ContentStatsBackedMap.isEmpty(onlyRequired, ContentStatsBackedMap.Kind.NAN_VALUE_COUNT))
        .isTrue();
    assertThat(ContentStatsBackedMap.isEmpty(onlyRequired, ContentStatsBackedMap.Kind.VALUE_COUNT))
        .isFalse();

    ContentStatsStruct populated = POPULATED_STATS;
    assertThat(ContentStatsBackedMap.isEmpty(populated, ContentStatsBackedMap.Kind.NAN_VALUE_COUNT))
        .isFalse();
  }

  @Test
  public void testInstanceIsEmptyReflectsContents() {
    ContentStatsStruct onlyRequired = new ContentStatsStruct(STATS_TYPE);
    onlyRequired.setStats(
        1, new FieldStatsStruct<>(statsType("req"), 1L, 5L, false, 10L, null, null, null));
    // isEmpty() reflects whether any column contributes an entry for the kind
    assertThat(
            new ContentStatsBackedMap<>(onlyRequired, ContentStatsBackedMap.Kind.NAN_VALUE_COUNT))
        .isEmpty();
    assertThat(new ContentStatsBackedMap<>(onlyRequired, ContentStatsBackedMap.Kind.VALUE_COUNT))
        .isNotEmpty();
  }
}
