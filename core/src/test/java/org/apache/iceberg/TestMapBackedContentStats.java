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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestMapBackedContentStats {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "score", Types.FloatType.get()),
          Types.NestedField.optional(3, "ts", Types.LongType.get()),
          Types.NestedField.optional(4, "name", Types.StringType.get()),
          Types.NestedField.optional(5, "flag", Types.BooleanType.get()));

  private static final PartitionData EMPTY_PARTITION =
      new PartitionData(PartitionSpec.unpartitioned().partitionType());

  /**
   * Stats on fields 1-4; field 5 is absent. Field 3 has no value-count entry, so valueCount()
   * throws on unboxing.
   */
  private static final DataFile FILE_WITH_STATS =
      dataFile(
          100L,
          ImmutableMap.of(1, 100L, 2, 100L, 4, 100L),
          ImmutableMap.of(2, 5L, 3, 1L, 4, 2L),
          ImmutableMap.of(2, 3L),
          ImmutableMap.of(
              1, buf(Types.IntegerType.get(), 1),
              2, buf(Types.FloatType.get(), 1.5f),
              3, buf(Types.LongType.get(), 100L),
              4, buf(Types.StringType.get(), "aaa")),
          ImmutableMap.of(
              1, buf(Types.IntegerType.get(), 1000),
              2, buf(Types.FloatType.get(), 9.5f),
              3, buf(Types.LongType.get(), 999L),
              4, buf(Types.StringType.get(), "zzz")));

  @Test
  void typeMatchesIdsPresentInMaps() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);

    assertThat(stats.type().fields()).isEmpty();

    stats.wrap(FILE_WITH_STATS);

    assertThat(stats.type().fields())
        .containsExactlyInAnyOrderElementsOf(
            StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2, 3, 4)).fields());
  }

  @Test
  void wrapInvalidatesType() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);
    Types.StructType firstType = stats.type();
    assertThat(firstType.fields())
        .containsExactlyInAnyOrderElementsOf(
            StatsUtil.statsReadSchema(SCHEMA, List.of(1, 2, 3, 4)).fields());

    DataFile file2 =
        dataFile(
            100L,
            ImmutableMap.of(1, 50L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 500)),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 5000)));
    stats.wrap(file2);

    assertThat(stats.type().fields())
        .containsExactlyInAnyOrderElementsOf(
            StatsUtil.statsReadSchema(SCHEMA, List.of(1)).fields());
    assertThat(stats.type()).isNotEqualTo(firstType);
  }

  @ParameterizedTest
  @MethodSource("typesAndBounds")
  void boundDecodingPerType(Type type, Object lower, Object upper) {
    int fieldId = 1;
    Schema schema = new Schema(Types.NestedField.optional(fieldId, "col", type));
    DataFile file =
        dataFile(
            100L,
            ImmutableMap.of(fieldId, 1L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            bound(fieldId, Conversions.toByteBuffer(type, lower)),
            bound(fieldId, Conversions.toByteBuffer(type, upper)));
    FieldStats<?> stats = new MapBackedContentStats(schema).wrap(file).statsFor(fieldId);

    Comparator<Object> comparator = Comparators.forType(type.asPrimitiveType());
    assertThat(comparator.compare(stats.lowerBound(), lower)).isZero();
    assertThat(comparator.compare(stats.upperBound(), upper)).isZero();
  }

  private static Stream<Arguments> typesAndBounds() {
    return Stream.of(
        Arguments.of(Types.BooleanType.get(), false, true),
        Arguments.of(Types.IntegerType.get(), -5, 100),
        Arguments.of(Types.LongType.get(), 0L, 1_000L),
        Arguments.of(Types.FloatType.get(), 1.5f, 9.5f),
        Arguments.of(Types.DoubleType.get(), 0.0d, 25.0d),
        Arguments.of(Types.DateType.get(), 100, 200),
        Arguments.of(Types.TimeType.get(), 1_000L, 2_000L),
        Arguments.of(Types.TimestampType.withZone(), 111L, 222L),
        Arguments.of(Types.TimestampNanoType.withZone(), 111L, 222L),
        Arguments.of(Types.StringType.get(), "a", "z"),
        Arguments.of(
            Types.UUIDType.get(),
            UUID.fromString("07ceab48-62b2-4219-9172-856e687c92ad"),
            UUID.fromString("a9a7c24d-2869-4c66-8803-b1f6a36256ed")),
        Arguments.of(
            Types.FixedType.ofLength(4),
            ByteBuffer.wrap(new byte[] {0, 1, 2, 3}),
            ByteBuffer.wrap(new byte[] {4, 5, 6, 7})),
        Arguments.of(
            Types.BinaryType.get(),
            ByteBuffer.wrap(new byte[] {1, 2}),
            ByteBuffer.wrap(new byte[] {3, 4, 5})),
        Arguments.of(Types.DecimalType.of(9, 2), new BigDecimal("1.23"), new BigDecimal("9.99")),
        Arguments.of(Types.UnknownType.get(), null, null));
  }

  @Test
  void geoBoundsDecode() {
    GeospatialBound lower = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound upper = GeospatialBound.createXYZM(3.0, 4.0, 5.0, 6.0);
    Schema schema = new Schema(Types.NestedField.optional(10, "geom", Types.GeometryType.crs84()));
    DataFile file =
        dataFile(
            100L,
            ImmutableMap.of(10, 26L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(10, lower.toByteBuffer()),
            ImmutableMap.of(10, upper.toByteBuffer()));
    MapBackedContentStats stats = new MapBackedContentStats(schema).wrap(file);

    FieldStats<?> geom = stats.statsFor(10);
    assertThat(geom.lowerBound()).isInstanceOf(GeospatialBound.class).isEqualTo(lower);
    assertThat(geom.upperBound()).isInstanceOf(GeospatialBound.class).isEqualTo(upper);
  }

  @Test
  void missingBoundsDecodeToNull() {
    DataFile file =
        dataFile(
            100L,
            ImmutableMap.of(1, 100L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of());
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(file);

    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.lowerBound()).isNull();
    assertThat(id.upperBound()).isNull();
    assertThat(id.valueCount()).isEqualTo(100L);
  }

  @Test
  void countsAndTightBounds() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);

    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.hasValueCount()).isTrue();
    assertThat(id.valueCount()).isEqualTo(100L);
    assertThat(id.hasNullValueCount()).isFalse();
    assertThat(id.hasNanValueCount()).isFalse();
    assertThatThrownBy(id::nullValueCount)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Long.longValue()");
    assertThatThrownBy(id::nanValueCount)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Long.longValue()");
    // ContentFile maps have no tight-bounds flag so the view always reports false.
    assertThat(id.tightBounds()).isFalse();
    // FILE_WITH_STATS has no avg-size map.
    assertThat(id.avgValueSizeInBytes()).isNull();

    FieldStats<?> score = stats.statsFor(2);
    assertThat(score.hasNullValueCount()).isTrue();
    assertThat(score.nullValueCount()).isEqualTo(5L);
    assertThat(score.hasNanValueCount()).isTrue();
    assertThat(score.nanValueCount()).isEqualTo(3L);

    FieldStats<?> ts = stats.statsFor(3);
    assertThat(ts.hasValueCount()).isFalse();
    assertThatThrownBy(ts::valueCount)
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Long.longValue()");
    assertThat(ts.hasNullValueCount()).isTrue();
    assertThat(ts.nullValueCount()).isEqualTo(1L);
  }

  @Test
  void fieldWithoutStatsIsExcluded() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);

    assertThat(stats.type().field(StatsUtil.toBaseId(5))).isNull();
    assertThat(stats.statsFor(5)).isNull();
    assertThat(stats.fieldStats())
        .extracting(FieldStats::fieldId)
        .containsExactlyInAnyOrder(1, 2, 3, 4);
  }

  @Test
  void unknownFieldIdInMaps() {
    DataFile file =
        dataFile(
            100L,
            ImmutableMap.of(99, 1L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of());
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(file);

    assertThat(stats.statsFor(99)).isNull();
    assertThat(stats.type().fields()).isEmpty();
    assertThat(stats.fieldStats()).isEmpty();
  }

  @Test
  void reuseRebindsFields() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);

    stats.wrap(FILE_WITH_STATS);
    assertThat(stats.statsFor(1).lowerBound()).isEqualTo(1);
    assertThat(stats.statsFor(1).upperBound()).isEqualTo(1000);
    assertThat(stats.statsFor(1).valueCount()).isEqualTo(100L);

    DataFile file2 =
        dataFile(
            100L,
            ImmutableMap.of(1, 50L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 500)),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 5000)));
    stats.wrap(file2);

    assertThat(stats.statsFor(1).lowerBound()).isEqualTo(500);
    assertThat(stats.statsFor(1).upperBound()).isEqualTo(5000);
    assertThat(stats.statsFor(1).valueCount()).isEqualTo(50L);
    assertThat(stats.statsFor(2)).isNull();
  }

  @Test
  void absentIdIsRereadWhenPresentAgain() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);
    stats.wrap(FILE_WITH_STATS);
    assertThat(stats.statsFor(1)).isNotNull();

    stats.wrap(
        dataFile(
            100L,
            ImmutableMap.of(2, 10L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(2, buf(Types.FloatType.get(), 0.0f)),
            ImmutableMap.of(2, buf(Types.FloatType.get(), 1.0f))));
    assertThat(stats.statsFor(1)).isNull();

    stats.wrap(
        dataFile(
            100L,
            ImmutableMap.of(1, 7L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 42)),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 43))));
    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.lowerBound()).isEqualTo(42);
    assertThat(id.upperBound()).isEqualTo(43);
    assertThat(id.valueCount()).isEqualTo(7L);
  }

  @Test
  void listElementFieldStats() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1, "nums", Types.ListType.ofRequired(2, Types.IntegerType.get())));
    DataFile file =
        dataFile(
            100L,
            ImmutableMap.of(2, 4L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(2, buf(Types.IntegerType.get(), 1)),
            ImmutableMap.of(2, buf(Types.IntegerType.get(), 9)));
    MapBackedContentStats stats = new MapBackedContentStats(schema).wrap(file);

    FieldStats<?> element = stats.statsFor(2);
    assertThat(element.lowerBound()).isEqualTo(1);
    assertThat(element.upperBound()).isEqualTo(9);
    assertThat(stats.fieldStats()).extracting(FieldStats::fieldId).containsExactly(2);
  }

  private static ByteBuffer buf(Type type, Object value) {
    return Conversions.toByteBuffer(type, value);
  }

  private static Map<Integer, ByteBuffer> bound(int fieldId, ByteBuffer value) {
    return value == null ? ImmutableMap.of() : ImmutableMap.of(fieldId, value);
  }

  private static DataFile dataFile(
      long recordCount,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    Metrics metrics =
        new Metrics(
            recordCount,
            null,
            valueCounts,
            nullValueCounts,
            nanValueCounts,
            lowerBounds,
            upperBounds);
    return new GenericDataFile(
        PartitionSpec.unpartitioned().specId(),
        "s3://bucket/data/file.parquet",
        FileFormat.PARQUET,
        EMPTY_PARTITION,
        1024L,
        metrics,
        null,
        ImmutableList.of(0L),
        null,
        null);
  }
}
