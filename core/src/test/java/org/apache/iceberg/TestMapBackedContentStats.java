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
import org.apache.iceberg.TrackedFileAdapters.MapBackedContentStats;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestMapBackedContentStats {

  // field 5 (flag) is intentionally left out of every stat map to exercise the absent-field path.
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "score", Types.FloatType.get()),
          Types.NestedField.optional(3, "ts", Types.LongType.get()),
          Types.NestedField.optional(4, "name", Types.StringType.get()),
          Types.NestedField.optional(5, "flag", Types.BooleanType.get()));

  private static final MetricsConfig METRICS_CONFIG =
      MetricsConfig.from(ImmutableMap.of(), SCHEMA, SortOrder.unsorted());

  private static final PartitionData EMPTY_PARTITION =
      new PartitionData(PartitionSpec.unpartitioned().partitionType());

  /** A file with stats on fields 1-4 (int, float, long, string) but none on field 5 (boolean). */
  private static final DataFile FILE_WITH_STATS =
      dataFile(
          ImmutableMap.of(1, 100L, 2, 100L, 3, 100L, 4, 100L),
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
  void testBoundDecodingPerType() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);

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
    assertThat(name.upperBound().toString()).isEqualTo("zzz");
  }

  @Test
  void testMissingBoundsDecodeToNull() {
    // value counts present but no lower/upper bound entries for the field
    DataFile file =
        dataFile(
            ImmutableMap.of(1, 100L),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of(),
            ImmutableMap.of());
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(file);

    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.lowerBound()).isNull();
    assertThat(id.upperBound()).isNull();
    assertThat(id.valueCount()).isEqualTo(100L);
  }

  @Test
  void testCountsAndDefaults() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);

    FieldStats<?> id = stats.statsFor(1);
    assertThat(id.valueCount()).isEqualTo(100L);
    // absent counts return -1 ("not tracked"), not 0, so callers don't read a false known-zero
    assertThat(id.nullValueCount()).isEqualTo(-1L);
    assertThat(id.nanValueCount()).isEqualTo(-1L);
    // the wrapper never tracks tight bounds or avg value size on the write path
    assertThat(id.tightBounds()).isFalse();
    assertThat(id.avgValueSizeInBytes()).isNull();

    FieldStats<?> score = stats.statsFor(2);
    assertThat(score.nullValueCount()).isEqualTo(5L);
    assertThat(score.nanValueCount()).isEqualTo(3L);
  }

  @Test
  void testCountsOnlyColumnOmitsBounds() {
    // configure the float column (score) as counts-only; its stats sub-struct must drop bounds
    MetricsConfig countsConfig =
        MetricsConfig.from(
            ImmutableMap.of("write.metadata.metrics.column.score", "counts"),
            SCHEMA,
            SortOrder.unsorted());
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, countsConfig).wrap(FILE_WITH_STATS);

    // counts mode drops lower_bound/upper_bound/tight_bounds; only the counts remain
    FieldStats<?> score = stats.statsFor(2);
    assertThat(score.type().fields())
        .extracting(Types.NestedField::name)
        .containsExactly("value_count", "null_value_count", "nan_value_count");

    // the serialization surface exposes only the counts, in struct order
    StructLike struct = (StructLike) score;
    assertThat(struct.size()).isEqualTo(3);
    assertThat(struct.get(0, Long.class)).isEqualTo(100L); // value_count
    assertThat(struct.get(1, Long.class)).isEqualTo(5L); // null_value_count
    assertThat(struct.get(2, Long.class)).isEqualTo(3L); // nan_value_count

    // a column left at the default mode still carries bounds
    assertThat(stats.statsFor(1).type().fields())
        .extracting(Types.NestedField::name)
        .contains("lower_bound", "upper_bound");
  }

  @Test
  void testAbsentFieldHasNoStats() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);

    // field 5 (flag) has no entry in any stat map
    assertThat(stats.statsFor(5)).isNull();
    assertThat(stats.fieldStats())
        .extracting(FieldStats::fieldId)
        .containsExactlyInAnyOrder(1, 2, 3, 4);
  }

  @Test
  void testType() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA, METRICS_CONFIG);
    assertThat(stats.type())
        .isEqualTo(StatsUtil.statsReadSchema(SCHEMA, ImmutableList.of(1, 2, 3, 4, 5)));
  }

  @Test
  void testFieldStatsStructLikeAccessByOffset() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);
    // required int field: struct is [lower_bound, upper_bound, tight_bounds, value_count]
    FieldStats<?> id = stats.statsFor(1);
    StructLike struct = (StructLike) id;

    assertThat(struct.size()).isEqualTo(id.type().fields().size());
    assertThat(struct.get(0, Integer.class)).isEqualTo(1); // lower_bound
    assertThat(struct.get(1, Integer.class)).isEqualTo(1000); // upper_bound
    assertThat(struct.get(2, Boolean.class)).isEqualTo(false); // tight_bounds
    assertThat(struct.get(3, Long.class)).isEqualTo(100L); // value_count
  }

  @Test
  void testContentStructLikeGetReturnsChildrenOrNull() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);

    int nullPositions = 0;
    for (int pos = 0; pos < stats.size(); pos += 1) {
      FieldStats<?> child = stats.get(pos, FieldStats.class);
      if (child == null) {
        nullPositions += 1;
      } else {
        assertThat(stats.statsFor(child.fieldId())).isSameAs(child);
      }
    }

    // only field 5 (flag) lacks stats
    assertThat(nullPositions).isEqualTo(1);
  }

  @Test
  void testCopyNotSupported() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);

    // the reusable wrapper is serialized directly; snapshots must be materialized via a writer
    assertThatThrownBy(stats::copy)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support copy()");

    assertThatThrownBy(() -> stats.copy(ImmutableSet.of(1)))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support copy()");
  }

  @Test
  void testFieldStatsCopyAndSetNotSupported() {
    MapBackedContentStats stats =
        new MapBackedContentStats(SCHEMA, METRICS_CONFIG).wrap(FILE_WITH_STATS);
    FieldStats<?> id = stats.statsFor(1);

    // the reusable field wrapper is serialized directly; it neither copies nor mutates
    assertThatThrownBy(id::copy)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support copy()");
    assertThatThrownBy(() -> ((StructLike) id).set(0, 5))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("does not support set()");
  }

  @Test
  void testReuseRebindsBounds() {
    MapBackedContentStats stats = new MapBackedContentStats(SCHEMA, METRICS_CONFIG);

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
    // fields that had stats in file1 but not file2 are now absent
    assertThat(stats.statsFor(2)).isNull();
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
