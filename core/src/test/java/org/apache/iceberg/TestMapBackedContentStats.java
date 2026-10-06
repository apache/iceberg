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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.geospatial.GeospatialBound;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

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
   * Stats on fields 1-4; field 5 is absent. Field 3 has no value_count entry so the absent-count
   * getter throws.
   */
  private static final DataFile FILE_WITH_STATS =
      dataFile(
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
  void contentStatsTypeBuiltLazilyFromMapIds() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);

    assertThat(stats.type().fields()).isEmpty();

    stats.wrap(FILE_WITH_STATS);

    assertThat(stats.type().fields())
        .extracting(field -> StatsUtil.toFieldId(field.fieldId()))
        .containsExactlyInAnyOrder(1, 2, 3, 4);
  }

  @Test
  void wrapInvalidatesType() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);
    Types.StructType firstType = stats.type();
    assertThat(firstType.fields())
        .extracting(field -> StatsUtil.toFieldId(field.fieldId()))
        .containsExactlyInAnyOrder(1, 2, 3, 4);

    DataFile file2 =
        dataFile(
            ImmutableMap.of(1, 50L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 500)),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 5000)));
    stats.wrap(file2);

    assertThat(stats.type().fields())
        .extracting(field -> StatsUtil.toFieldId(field.fieldId()))
        .containsExactlyInAnyOrder(1);
    assertThat(stats.type()).isNotEqualTo(firstType);
  }

  @Test
  void boundDecodingPerType() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);

    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.lowerBound()).isInstanceOf(Integer.class).isEqualTo(1);
    assertThat(id.upperBound()).isInstanceOf(Integer.class).isEqualTo(1000);

    FieldStats<?> score = stats.statsFor(2);
    assertThat(score.lowerBound()).isInstanceOf(Float.class).isEqualTo(1.5f);
    assertThat(score.upperBound()).isInstanceOf(Float.class).isEqualTo(9.5f);

    FieldStats<?> ts = stats.statsFor(3);
    assertThat(ts.lowerBound()).isInstanceOf(Long.class).isEqualTo(100L);
    assertThat(ts.upperBound()).isInstanceOf(Long.class).isEqualTo(999L);

    FieldStats<?> name = stats.statsFor(4);
    assertThat(name.lowerBound()).isInstanceOf(CharSequence.class);
    assertThat(name.lowerBound().toString()).isEqualTo("aaa");
    assertThat(name.upperBound()).isInstanceOf(CharSequence.class);
    assertThat(name.upperBound().toString()).isEqualTo("zzz");
  }

  @Test
  void geoBoundsDecode() {
    GeospatialBound lower = GeospatialBound.createXY(1.0, 2.0);
    GeospatialBound upper = GeospatialBound.createXYZM(3.0, 4.0, 5.0, 6.0);
    Schema schema = new Schema(Types.NestedField.optional(10, "geom", Types.GeometryType.crs84()));
    DataFile file =
        dataFile(
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

    assertThat(stats.containsFieldInMaps(5)).isFalse();
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
            ImmutableMap.of(99, 1L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of());
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(file);

    assertThat(stats.containsFieldInMaps(99)).isTrue();
    assertThat(stats.statsFor(99)).isNull();
    assertThat(stats.type().fields()).isEmpty();
    assertThat(stats.fieldStats()).isEmpty();
  }

  @Test
  void copyNotSupported() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);

    assertThatThrownBy(stats::copy)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("copy is not implemented");

    assertThatThrownBy(() -> stats.copy(ImmutableSet.of(1)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("copy is not implemented");
  }

  @Test
  void fieldStatsCopyNotSupported() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA).wrap(FILE_WITH_STATS);

    assertThatThrownBy(stats.statsFor(1)::copy)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("copy is not implemented");
  }

  @Test
  void reuseRebindsBounds() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);

    stats.wrap(FILE_WITH_STATS);
    assertThat(stats.statsFor(1).lowerBound()).isEqualTo(1);

    DataFile file2 =
        dataFile(
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
  void wrapReusesFieldStats() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);
    stats.wrap(FILE_WITH_STATS);
    FieldStats<?> id = stats.statsFor(1);
    FieldStats<?> score = stats.statsFor(2);
    FieldStats<?> ts = stats.statsFor(3);
    FieldStats<?> name = stats.statsFor(4);

    DataFile file2 =
        dataFile(
            ImmutableMap.of(1, 50L, 2, 40L, 3, 20L, 4, 30L),
            ImmutableMap.of(1, 9L, 2, 8L, 3, 7L, 4, 6L),
            ImmutableMap.of(2, 11L),
            ImmutableMap.of(
                1, buf(Types.IntegerType.get(), 500),
                2, buf(Types.FloatType.get(), 2.5f),
                3, buf(Types.LongType.get(), 200L),
                4, buf(Types.StringType.get(), "bbb")),
            ImmutableMap.of(
                1, buf(Types.IntegerType.get(), 5000),
                2, buf(Types.FloatType.get(), 8.5f),
                3, buf(Types.LongType.get(), 800L),
                4, buf(Types.StringType.get(), "yyy")));
    stats.wrap(file2);

    assertThat(stats.statsFor(1)).isSameAs(id);
    assertThat(id.valueCount()).isEqualTo(50L);
    assertThat(id.nullValueCount()).isEqualTo(9L);
    assertThat(id.lowerBound()).isEqualTo(500);
    assertThat(id.upperBound()).isEqualTo(5000);

    assertThat(stats.statsFor(2)).isSameAs(score);
    assertThat(score.valueCount()).isEqualTo(40L);
    assertThat(score.nullValueCount()).isEqualTo(8L);
    assertThat(score.nanValueCount()).isEqualTo(11L);
    assertThat(score.lowerBound()).isEqualTo(2.5f);
    assertThat(score.upperBound()).isEqualTo(8.5f);

    assertThat(stats.statsFor(3)).isSameAs(ts);
    assertThat(ts.valueCount()).isEqualTo(20L);
    assertThat(ts.nullValueCount()).isEqualTo(7L);
    assertThat(ts.lowerBound()).isEqualTo(200L);
    assertThat(ts.upperBound()).isEqualTo(800L);

    assertThat(stats.statsFor(4)).isSameAs(name);
    assertThat(name.valueCount()).isEqualTo(30L);
    assertThat(name.nullValueCount()).isEqualTo(6L);
    assertThat(name.lowerBound().toString()).isEqualTo("bbb");
    assertThat(name.upperBound().toString()).isEqualTo("yyy");
  }

  @Test
  void absentIdIsRereadWhenPresentAgain() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA);
    stats.wrap(FILE_WITH_STATS);
    FieldStats<?> id = stats.statsFor(1);
    assertThat(id).isNotNull();

    stats.wrap(
        dataFile(
            ImmutableMap.of(2, 10L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(2, buf(Types.FloatType.get(), 0.0f)),
            ImmutableMap.of(2, buf(Types.FloatType.get(), 1.0f))));
    assertThat(stats.statsFor(1)).isNull();

    stats.wrap(
        dataFile(
            ImmutableMap.of(1, 7L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 42)),
            ImmutableMap.of(1, buf(Types.IntegerType.get(), 43))));
    assertThat(stats.statsFor(1)).isSameAs(id);
    assertThat(id.lowerBound()).isEqualTo(42);
    assertThat(id.upperBound()).isEqualTo(43);
    assertThat(id.valueCount()).isEqualTo(7L);
  }

  @Test
  void outOfRangeFieldStatsAreNullAndCached() {
    int fieldId = 999_950;
    Schema schema =
        new Schema(Types.NestedField.optional(fieldId, "too_high", Types.IntegerType.get()));
    DataFile file =
        dataFile(
            ImmutableMap.of(fieldId, 1L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(fieldId, buf(Types.IntegerType.get(), 1)),
            ImmutableMap.of(fieldId, buf(Types.IntegerType.get(), 2)));
    CountingContentStats stats = new CountingContentStats(schema);
    stats.wrap(file);

    assertThat(stats.statsFor(fieldId)).isNull();
    assertThat(stats.creates).isEqualTo(1);

    DataFile file2 =
        dataFile(
            ImmutableMap.of(fieldId, 9L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(fieldId, buf(Types.IntegerType.get(), 30)),
            ImmutableMap.of(fieldId, buf(Types.IntegerType.get(), 40)));
    stats.wrap(file2);

    assertThat(stats.statsFor(fieldId)).isNull();
    assertThat(stats.creates).isEqualTo(1);
    assertThat(stats.fieldStats()).isEmpty();
    assertThat(stats.creates).isEqualTo(1);
  }

  @Test
  void listElementFieldStats() {
    Schema schema =
        new Schema(
            Types.NestedField.required(
                1, "nums", Types.ListType.ofRequired(2, Types.IntegerType.get())));
    DataFile file =
        dataFile(
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

  private static final class CountingContentStats extends MapBackedContentStats {
    private int creates;

    private CountingContentStats(Schema tableSchema) {
      super(tableSchema);
    }

    @Override
    FieldStats<?> createFieldStats(int fieldId) {
      creates += 1;
      return super.createFieldStats(fieldId);
    }
  }

  private static ByteBuffer buf(Type type, Object value) {
    return Conversions.toByteBuffer(type, value);
  }

  private static DataFile dataFile(
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullValueCounts,
      Map<Integer, Long> nanValueCounts,
      Map<Integer, ByteBuffer> lowerBounds,
      Map<Integer, ByteBuffer> upperBounds) {
    Metrics metrics =
        new Metrics(
            100L, null, valueCounts, nullValueCounts, nanValueCounts, lowerBounds, upperBounds);
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
