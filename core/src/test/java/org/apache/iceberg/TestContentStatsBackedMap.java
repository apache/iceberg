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
import org.apache.iceberg.geospatial.GeospatialBound;
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
    POPULATED_STATS.setStats(
        1, StatsTestUtil.mockFieldStats(statsType("req"), 1, 1L, 5L, 10L, null, null));
    POPULATED_STATS.setStats(
        2, StatsTestUtil.mockFieldStats(statsType("opt"), 2, 2L, 6L, 20L, 3L, null));
    POPULATED_STATS.setStats(
        3, StatsTestUtil.mockFieldStats(statsType("dbl"), 3, 1.0, 9.0, 30L, 7L, 4L));

    // only a required long column: tracks a value count but no null or NaN count
    ONLY_REQUIRED_STATS.setStats(
        1, StatsTestUtil.mockFieldStats(statsType("req"), 1, 1L, 5L, 10L, null, null));
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
  public void testGeoBoundsUseSinglePointEncoding() {
    Schema geoSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(10, "geom", Types.GeometryType.crs84()),
            optional(11, "geog", Types.GeographyType.crs84()));
    Types.StructType statsType = StatsUtil.statsReadSchema(geoSchema, List.of(1, 10, 11));

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        1,
        StatsTestUtil.mockFieldStats(
            statsType.field("id").type().asStructType(), 1, 1L, 5L, 26L, null, null));
    stats.setStats(
        10,
        StatsTestUtil.mockFieldStats(
            statsType.field("geom").type().asStructType(),
            10,
            TestHelpers.Row.of(1.0, 2.0, null, null),
            TestHelpers.Row.of(5.0, 6.0, null, null),
            26L,
            2L,
            null));
    stats.setStats(
        11,
        StatsTestUtil.mockFieldStats(
            statsType.field("geog").type().asStructType(),
            11,
            TestHelpers.Row.of(-1.0, -2.0, 3.0, 4.0),
            TestHelpers.Row.of(7.0, 8.0, 9.0, 10.0),
            26L,
            0L,
            null));

    // geometry and geography bounds are stored as bounding-box structs (x, y, z, m) but must be
    // presented in the legacy maps using the spec's single-point encoding
    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(stats);
    assertThat(lower)
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 1L)),
            Map.entry(10, GeospatialBound.createXY(1.0, 2.0).toByteBuffer()),
            Map.entry(11, GeospatialBound.createXYZM(-1.0, -2.0, 3.0, 4.0).toByteBuffer()));

    Map<Integer, ByteBuffer> upper = ContentStatsBackedMap.upperBounds(stats);
    assertThat(upper)
        .containsOnly(
            Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 5L)),
            Map.entry(10, GeospatialBound.createXY(5.0, 6.0).toByteBuffer()),
            Map.entry(11, GeospatialBound.createXYZM(7.0, 8.0, 9.0, 10.0).toByteBuffer()));

    // the encoding must round-trip through the geo conversion used by legacy readers
    GeospatialBound geomLower =
        Conversions.fromByteBuffer(Types.GeometryType.crs84(), lower.get(10));
    assertThat(geomLower).isEqualTo(GeospatialBound.createXY(1.0, 2.0));
    GeospatialBound geogUpper =
        Conversions.fromByteBuffer(Types.GeographyType.crs84(), upper.get(11));
    assertThat(geogUpper).isEqualTo(GeospatialBound.createXYZM(7.0, 8.0, 9.0, 10.0));
  }

  @Test
  public void testGeoBoundWithZOnlyAndMOnly() {
    Schema geoSchema = new Schema(optional(10, "geom", Types.GeometryType.crs84()));
    Types.StructType statsType = StatsUtil.statsReadSchema(geoSchema, List.of(10));

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        10,
        StatsTestUtil.mockFieldStats(
            statsType.field("geom").type().asStructType(),
            10,
            TestHelpers.Row.of(1.0, 2.0, 3.0, null),
            TestHelpers.Row.of(5.0, 6.0, null, 7.0),
            26L,
            2L,
            null));

    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(stats);
    assertThat(lower.get(10)).isEqualTo(GeospatialBound.createXYZ(1.0, 2.0, 3.0).toByteBuffer());

    Map<Integer, ByteBuffer> upper = ContentStatsBackedMap.upperBounds(stats);
    assertThat(upper.get(10)).isEqualTo(GeospatialBound.createXYM(5.0, 6.0, 7.0).toByteBuffer());

    // the encoding must round-trip through the geo conversion used by legacy readers
    GeospatialBound geomLower =
        Conversions.fromByteBuffer(Types.GeometryType.crs84(), lower.get(10));
    assertThat(geomLower).isEqualTo(GeospatialBound.createXYZ(1.0, 2.0, 3.0));
    GeospatialBound geomUpper =
        Conversions.fromByteBuffer(Types.GeometryType.crs84(), upper.get(10));
    assertThat(geomUpper).isEqualTo(GeospatialBound.createXYM(5.0, 6.0, 7.0));
  }

  @Test
  public void testGeoFieldWithoutLowerBoundIsAbsentFromView() {
    Schema geoSchema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(10, "geom", Types.GeometryType.crs84()));
    Types.StructType statsType = StatsUtil.statsReadSchema(geoSchema, List.of(1, 10));

    ContentStatsStruct stats = new ContentStatsStruct(statsType);
    stats.setStats(
        1,
        StatsTestUtil.mockFieldStats(
            statsType.field("id").type().asStructType(), 1, 1L, 5L, 26L, null, null));
    stats.setStats(
        10,
        StatsTestUtil.mockFieldStats(
            statsType.field("geom").type().asStructType(),
            10,
            null,
            TestHelpers.Row.of(5.0, 6.0, null, null),
            26L,
            2L,
            null));

    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(stats);
    assertThat(lower.get(10)).isNull();
    assertThat(lower.containsKey(10)).isFalse();
    assertThat(lower)
        .containsOnly(Map.entry(1, Conversions.toByteBuffer(Types.LongType.get(), 1L)));

    Map<Integer, ByteBuffer> upper = ContentStatsBackedMap.upperBounds(stats);
    assertThat(upper.get(10)).isEqualTo(GeospatialBound.createXY(5.0, 6.0).toByteBuffer());
  }

  @Test
  public void testNonGeoStructBoundIsRejected() {
    Types.StructType fakeStatsType =
        Types.StructType.of(
            optional(
                1,
                StatsUtil.LOWER_BOUND_NAME,
                Types.StructType.of(required(2, "a", Types.DoubleType.get()))));
    FieldStats<Object> fieldStats =
        StatsTestUtil.mockFieldStats(
            fakeStatsType, 7, TestHelpers.Row.of(1.0), null, 10L, null, null);
    ContentStats stats = Mockito.mock(ContentStats.class);
    Mockito.when(stats.fieldStats()).thenReturn(List.of(fieldStats));
    Mockito.when(stats.statsFor(7)).thenReturn(fieldStats);

    // a struct bound that is not the geo bounding box must fail loudly, not encode as geo
    Map<Integer, ByteBuffer> lower = ContentStatsBackedMap.lowerBounds(stats);
    assertThatThrownBy(() -> lower.get(7))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("Cannot serialize type");
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
    stats.setStats(2, StatsTestUtil.mockFieldStats(statsType("opt"), 2, 2L, 6L, 20L, 3L, null));

    Map<Integer, Long> valueCounts = ContentStatsBackedMap.valueCounts(stats);
    assertThat(valueCounts).isNotNull().containsOnly(Map.entry(2, 20L));
  }

  @Test
  public void testIsEmptyMatchesEntrySet() {
    // a factory returns null for an empty view, so a live map always has entries; isEmpty() must
    // agree with entrySet() and never report a false positive
    Map<Integer, Long> valueCounts = ContentStatsBackedMap.valueCounts(POPULATED_STATS);
    assertThat(valueCounts).isNotNull();
    assertThat(valueCounts.isEmpty()).isFalse();
    assertThat(valueCounts.isEmpty()).isEqualTo(valueCounts.entrySet().isEmpty());
  }
}
