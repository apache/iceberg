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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.MethodSource;

public class TestStatsUtil {
  @Test
  public void testToBaseIdWithData() {
    assertThat(StatsUtil.toBaseId(0)).isEqualTo(10_000);
    assertThat(StatsUtil.toBaseId(1)).isEqualTo(10_200);
    assertThat(StatsUtil.toBaseId(100)).isEqualTo(30_000);
    assertThat(StatsUtil.toBaseId(999_949)).isEqualTo(199_999_800);
  }

  @Test
  public void testToBaseIdWithMetadata() {
    assertThat(StatsUtil.toBaseId(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId()))
        .isEqualTo(9_000);
    assertThat(StatsUtil.toBaseId(MetadataColumns.ROW_ID.fieldId())).isEqualTo(9_200);
  }

  @Test
  public void testBaseIdUnassigned() {
    assertThat(StatsUtil.toBaseId(999_950)).isEqualTo(-1);
    assertThat(StatsUtil.toBaseId(-1)).isEqualTo(-1);
    assertThat(StatsUtil.toBaseId(MetadataColumns.FILE_PATH.fieldId())).isEqualTo(-1);
  }

  @Test
  public void testToFieldIdWithData() {
    assertThat(StatsUtil.toFieldId(10_000)).isEqualTo(0);
    assertThat(StatsUtil.toFieldId(10_200)).isEqualTo(1);
    assertThat(StatsUtil.toFieldId(30_000)).isEqualTo(100);
    assertThat(StatsUtil.toFieldId(199_999_800)).isEqualTo(999_949);
  }

  @Test
  public void testToFieldIdWithMetadata() {
    assertThat(StatsUtil.toFieldId(9_000))
        .isEqualTo(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId());
    assertThat(StatsUtil.toFieldId(9_200)).isEqualTo(MetadataColumns.ROW_ID.fieldId());
  }

  @Test
  public void testToFieldIdOutsideRange() {
    assertThatThrownBy(() -> StatsUtil.toFieldId(200_000_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 200000000");
    assertThatThrownBy(() -> StatsUtil.toFieldId(200_000_001))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 200000001");
    assertThatThrownBy(() -> StatsUtil.toFieldId(8_800))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 8800");
    assertThatThrownBy(() -> StatsUtil.toFieldId(8_801))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 8801");
  }

  @Test
  public void testToFieldIdReservedMetadataRange() {
    // 9,000 to 10,000 (exclusive) is reserved for metadata column stats
    assertThatThrownBy(() -> StatsUtil.toFieldId(9_400))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Unsupported metadata stats field ID: 9400");
  }

  @Test
  public void testStatOffset() {
    assertThat(StatsUtil.statOffset(10_000)).isEqualTo(0);
    assertThat(StatsUtil.statOffset(10_201)).isEqualTo(1);
    assertThat(StatsUtil.statOffset(10_211)).isEqualTo(11);
    assertThat(StatsUtil.statOffset(10_399)).isEqualTo(199);
    assertThat(StatsUtil.statOffset(10_400)).isEqualTo(0);
    assertThat(StatsUtil.statOffset(199_999_999)).isEqualTo(199);
  }

  @Test
  public void testStatOffsetOutsideRange() {
    assertThatThrownBy(() -> StatsUtil.statOffset(200_000_000))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 200000000");
    assertThatThrownBy(() -> StatsUtil.statOffset(200_000_001))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 200000001");
    assertThatThrownBy(() -> StatsUtil.statOffset(8_800))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 8800");
    assertThatThrownBy(() -> StatsUtil.statOffset(8_801))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid stats field ID: 8801");
  }

  private static final List<Type> FIXED_WIDTH_TYPES =
      List.of(
          Types.BooleanType.get(),
          Types.IntegerType.get(),
          Types.LongType.get(),
          Types.DateType.get(),
          Types.TimeType.get(),
          Types.TimestampType.withoutZone(),
          Types.TimestampType.withZone(),
          Types.TimestampNanoType.withoutZone(),
          Types.TimestampNanoType.withZone(),
          Types.DecimalType.of(9, 2),
          Types.UUIDType.get(),
          Types.FixedType.ofLength(16));

  @ParameterizedTest
  @FieldSource("FIXED_WIDTH_TYPES")
  public void testFixedWidthPrimitiveStruct(Type type) {
    // fixed-width, non-floating-point types track bounds (including tight bounds) but have no NaN
    // count or total bytes
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final List<Type> VARIABLE_WIDTH_TYPES =
      List.of(Types.StringType.get(), Types.BinaryType.get());

  @ParameterizedTest
  @FieldSource("VARIABLE_WIDTH_TYPES")
  public void testVariableWidthPrimitiveStruct(Type type) {
    // variable-width types track bounds (including tight bounds) and total bytes, but no
    // NaN count
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "total_bytes", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final List<Type> GEO_TYPES =
      List.of(
          Types.GeometryType.crs84(),
          Types.GeometryType.of("srid:3857"),
          Types.GeographyType.crs84(),
          Types.GeographyType.of("srid:4269"));

  @ParameterizedTest
  @FieldSource("GEO_TYPES")
  public void testGeoStruct(Type type) {
    // geometry and geography use bounding-box structs for their bounds, do not track tight bounds,
    // and record total bytes
    Types.StructType lowerBound =
        Types.StructType.of(
            Types.NestedField.required(30_010, "x", Types.DoubleType.get()),
            Types.NestedField.required(30_011, "y", Types.DoubleType.get()),
            Types.NestedField.optional(30_012, "z", Types.DoubleType.get()),
            Types.NestedField.optional(30_013, "m", Types.DoubleType.get()));
    Types.StructType upperBound =
        Types.StructType.of(
            Types.NestedField.required(30_014, "x", Types.DoubleType.get()),
            Types.NestedField.required(30_015, "y", Types.DoubleType.get()),
            Types.NestedField.optional(30_016, "z", Types.DoubleType.get()),
            Types.NestedField.optional(30_017, "m", Types.DoubleType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", lowerBound),
            Types.NestedField.optional(30_002, "upper_bound", upperBound),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "total_bytes", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final List<Type> FLOATING_POINT_TYPES =
      List.of(Types.FloatType.get(), Types.DoubleType.get());

  @ParameterizedTest
  @FieldSource("FLOATING_POINT_TYPES")
  public void testFloatingPointStruct(Type type) {
    // floating-point types track bounds (including tight bounds) and a NaN count, but have no
    // total bytes
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_006, "nan_value_count", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final Types.StructType POINT =
      Types.StructType.of(
          Types.NestedField.required(1, "x", Types.FloatType.get()),
          Types.NestedField.required(2, "y", Types.FloatType.get()));

  private static final List<Type> NESTED_TYPES =
      List.of(
          Types.ListType.ofRequired(1, Types.IntegerType.get()),
          Types.ListType.ofOptional(1, POINT),
          Types.MapType.ofRequired(1, 2, Types.StringType.get(), Types.StringType.get()),
          Types.MapType.ofOptional(1, 2, Types.StringType.get(), POINT));

  @ParameterizedTest
  @FieldSource("NESTED_TYPES")
  public void testNestedTypesHaveNoStats(Type type) {
    // list and map types are not tracked and produce no stats struct
    assertThat(StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get())).isNull();
  }

  @Test
  public void testRequiredField() {
    // a required column produces a null_value_count field like any other column
    Type type = Types.IntegerType.get();
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  @Test
  public void testStringNoneMode() {
    // none mode produces no stats struct
    assertThat(StatsUtil.fieldStatsStruct(Types.StringType.get(), 30_000, MetricsModes.None.get()))
        .isNull();
  }

  @Test
  public void testStringCountsMode() {
    // counts mode drops the bounds (and tight_bounds) but keeps counts and the value size
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "total_bytes", Types.LongType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(Types.StringType.get(), 30_000, MetricsModes.Counts.get());

    assertSameStructure(expected, actual);
  }

  private static final List<MetricsModes.MetricsMode> BOUNDS_MODES =
      List.of(MetricsModes.Truncate.withLength(16), MetricsModes.Full.get());

  @ParameterizedTest
  @FieldSource("BOUNDS_MODES")
  public void testStringBoundsModes(MetricsModes.MetricsMode mode) {
    // truncate and full both retain the full bounds struct
    Type string = Types.StringType.get();
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", string),
            Types.NestedField.optional(30_002, "upper_bound", string),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "total_bytes", Types.LongType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(string, 30_000, mode);

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsReadSchema() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Types.StructType idStats =
        Types.StructType.of(
            Types.NestedField.optional(10_201, "lower_bound", Types.LongType.get()),
            Types.NestedField.optional(10_202, "upper_bound", Types.LongType.get()),
            Types.NestedField.optional(10_203, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_205, "null_value_count", Types.LongType.get()));
    Types.StructType dataStats =
        Types.StructType.of(
            Types.NestedField.optional(10_401, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_402, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_403, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_404, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_405, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_407, "total_bytes", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_400, "data", dataStats));

    Types.StructType actual = StatsUtil.statsReadSchema(schema, List.of(1, 2));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsReadSchemaSubset() {
    // a filter that only reads id and category produces a read schema for just those columns
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "category", Types.StringType.get()));

    Types.StructType idStats =
        Types.StructType.of(
            Types.NestedField.optional(10_201, "lower_bound", Types.LongType.get()),
            Types.NestedField.optional(10_202, "upper_bound", Types.LongType.get()),
            Types.NestedField.optional(10_203, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_205, "null_value_count", Types.LongType.get()));
    Types.StructType categoryStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_607, "total_bytes", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_600, "category", categoryStats));

    Types.StructType actual = StatsUtil.statsReadSchema(schema, List.of(1, 3));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsReadSchemaNestedStruct() {
    // fields nested in a struct are represented by their flattened name and nested base ID
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(3, "lat", Types.DoubleType.get()),
                    Types.NestedField.optional(4, "lon", Types.DoubleType.get()))));

    Types.StructType latStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_606, "nan_value_count", Types.LongType.get()));
    Types.StructType lonStats =
        Types.StructType.of(
            Types.NestedField.optional(10_801, "lower_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_802, "upper_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_803, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_804, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_805, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_806, "nan_value_count", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_600, "location_lat", latStats),
            Types.NestedField.optional(10_800, "location_lon", lonStats));

    Types.StructType actual = StatsUtil.statsReadSchema(schema, List.of(3, 4));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsReadSchemaOmitsListAndMap() {
    // list and map columns do not track stats, so requesting them yields an empty read schema
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "tags", Types.ListType.ofRequired(2, Types.StringType.get())),
            Types.NestedField.optional(
                3,
                "props",
                Types.MapType.ofOptional(4, 5, Types.StringType.get(), Types.StringType.get())));

    Types.StructType actual = StatsUtil.statsReadSchema(schema, List.of(1, 3));

    assertThat(actual.fields()).isEmpty();
  }

  @Test
  public void testContentStatsWriteSchema() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()));

    Types.StructType idStats =
        Types.StructType.of(
            Types.NestedField.optional(10_201, "lower_bound", Types.LongType.get()),
            Types.NestedField.optional(10_202, "upper_bound", Types.LongType.get()),
            Types.NestedField.optional(10_203, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_205, "null_value_count", Types.LongType.get()));
    Types.StructType dataStats =
        Types.StructType.of(
            Types.NestedField.optional(10_401, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_402, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_403, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_404, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_405, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_407, "total_bytes", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_400, "data", dataStats));

    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsTestUtil.from(ImmutableMap.of(), schema));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsWriteSchemaSubset() {
    // a filter that only reads id and category produces a read schema for just those columns
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "category", Types.StringType.get()));

    Types.StructType idStats =
        Types.StructType.of(
            Types.NestedField.optional(10_201, "lower_bound", Types.LongType.get()),
            Types.NestedField.optional(10_202, "upper_bound", Types.LongType.get()),
            Types.NestedField.optional(10_203, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_205, "null_value_count", Types.LongType.get()));
    Types.StructType categoryStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_607, "total_bytes", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_600, "category", categoryStats));

    Map<String, String> properties =
        ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "none");
    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsTestUtil.from(properties, schema));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsWriteSchemaNestedStruct() {
    // fields nested in a struct are represented by their flattened name and nested base ID
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(3, "lat", Types.DoubleType.get()),
                    Types.NestedField.optional(4, "lon", Types.DoubleType.get()))));

    Types.StructType latStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_606, "nan_value_count", Types.LongType.get()));
    Types.StructType lonStats =
        Types.StructType.of(
            Types.NestedField.optional(10_801, "lower_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_802, "upper_bound", Types.DoubleType.get()),
            Types.NestedField.optional(10_803, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_804, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_805, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_806, "nan_value_count", Types.LongType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_600, "location_lat", latStats),
            Types.NestedField.optional(10_800, "location_lon", lonStats));

    Map<String, String> properties =
        ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id", "none");
    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsTestUtil.from(properties, schema));

    assertSameStructure(expected, actual);
  }

  @Test
  public void testContentStatsWriteSchemaOmitsListAndMap() {
    // list and map columns do not track stats, so requesting them yields an empty read schema
    Schema schema =
        new Schema(
            Types.NestedField.optional(
                1, "tags", Types.ListType.ofRequired(2, Types.StringType.get())),
            Types.NestedField.optional(
                3,
                "props",
                Types.MapType.ofOptional(4, 5, Types.StringType.get(), Types.StringType.get())));

    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsTestUtil.from(ImmutableMap.of(), schema));

    assertThat(actual.fields()).isEmpty();
  }

  private static Stream<Partition> identityValues() {
    return Stream.of(
        identity(Types.BooleanType.get(), true),
        identity(Types.IntegerType.get(), 34),
        identity(Types.LongType.get(), 34L),
        identity(Types.FloatType.get(), 1.5F),
        identity(Types.DoubleType.get(), 1.5D),
        identity(Types.DecimalType.of(9, 2), new BigDecimal("12.34")),
        identity(Types.DateType.get(), days("2024-02-29")),
        identity(Types.TimeType.get(), DateTimeUtil.isoTimeToMicros("05:00:00.000000")),
        identity(
            Types.TimestampType.withoutZone(),
            DateTimeUtil.isoTimestampToMicros("2024-02-29T05:00:00.000000")),
        identity(Types.TimestampType.withZone(), micros("2024-02-29T05:00:00.000000+00:00")),
        identity(
            Types.TimestampNanoType.withoutZone(),
            DateTimeUtil.isoTimestampToNanos("2024-02-29T05:00:00.000000000")),
        identity(Types.TimestampNanoType.withZone(), nanos("2024-02-29T05:00:00.000000000+00:00")),
        identity(Types.StringType.get(), "books"),
        identity(Types.UUIDType.get(), UUID.fromString("f79c3e09-677c-4bbd-a479-3f349cb785e7")),
        identity(Types.FixedType.ofLength(2), ByteBuffer.wrap(new byte[] {1, 2})),
        identity(Types.BinaryType.get(), ByteBuffer.wrap(new byte[] {1, 2, 3})));
  }

  @ParameterizedTest
  @MethodSource("identityValues")
  void identityBounds(Partition partition) {
    assertThat(sourceBounds(partition)).isEqualTo(partition.expected());
  }

  @Test
  void identityNaN() {
    assertThat(sourceBounds(Types.FloatType.get(), Transforms.identity(), Float.NaN)).isNull();
    assertThat(sourceBounds(Types.DoubleType.get(), Transforms.identity(), Double.NaN)).isNull();
  }

  private static Stream<Partition> truncatedNumbers() {
    Type decimal = Types.DecimalType.of(9, 2);
    return Stream.of(
        partition(Types.IntegerType.get(), Transforms.truncate(10), 20).withExpectedBounds(20, 29),
        partition(Types.IntegerType.get(), Transforms.truncate(10), -10)
            .withExpectedBounds(-10, -1),
        partition(Types.LongType.get(), Transforms.truncate(100), 12_300L)
            .withExpectedBounds(12_300L, 12_399L),
        // decimal widths apply to the unscaled value, so truncate(50) on scale 2 is a width of 0.50
        partition(decimal, Transforms.truncate(50), new BigDecimal("12.00"))
            .withExpectedBounds(new BigDecimal("12.00"), new BigDecimal("12.49")),
        partition(decimal, Transforms.truncate(50), new BigDecimal("-0.50"))
            .withExpectedBounds(new BigDecimal("-0.50"), new BigDecimal("-0.01")));
  }

  @ParameterizedTest
  @MethodSource("truncatedNumbers")
  void truncateNumberBounds(Partition partition) {
    assertThat(sourceBounds(partition)).isEqualTo(partition.expected());
  }

  @Test
  void truncateNumberOverflowAtMin() {
    // truncating MIN_VALUE through MIN_VALUE + 7 to a multiple of 10 overflows and wraps around
    // to a positive partition value that is not a multiple of 10
    Transform<Integer, Integer> intTruncate = Transforms.truncate(10);
    int intOverflowPartition = intTruncate.bind(Types.IntegerType.get()).apply(Integer.MIN_VALUE);
    assertThat(sourceBounds(Types.IntegerType.get(), intTruncate, intOverflowPartition))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(Integer.MIN_VALUE, Integer.MIN_VALUE + 7));

    Transform<Long, Long> longTruncate = Transforms.truncate(10);
    long longOverflowPartition = longTruncate.bind(Types.LongType.get()).apply(Long.MIN_VALUE);
    assertThat(sourceBounds(Types.LongType.get(), longTruncate, longOverflowPartition))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(Long.MIN_VALUE, Long.MIN_VALUE + 7));
  }

  @Test
  void truncateNumberWithoutOverflowAtMin() {
    // MIN_VALUE is a multiple of 8, so truncating values near it does not overflow
    Transform<Integer, Integer> intTruncate = Transforms.truncate(8);
    int intMinPartition = intTruncate.bind(Types.IntegerType.get()).apply(Integer.MIN_VALUE);
    assertThat(sourceBounds(Types.IntegerType.get(), intTruncate, intMinPartition))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(Integer.MIN_VALUE, Integer.MIN_VALUE + 7));

    Transform<Long, Long> longTruncate = Transforms.truncate(8);
    long longMinPartition = longTruncate.bind(Types.LongType.get()).apply(Long.MIN_VALUE);
    assertThat(sourceBounds(Types.LongType.get(), longTruncate, longMinPartition))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(Long.MIN_VALUE, Long.MIN_VALUE + 7));
  }

  @Test
  void truncateNumberUpperBoundDoesNotOverflowAtMax() {
    // the upper bound would be 2_147_483_649, which is past Integer.MAX_VALUE
    assertThat(sourceBounds(Types.IntegerType.get(), Transforms.truncate(10), 2_147_483_640))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(2_147_483_640, Integer.MAX_VALUE));

    // the upper bound would be 9_223_372_036_854_775_809, which is past Long.MAX_VALUE
    assertThat(
            sourceBounds(Types.LongType.get(), Transforms.truncate(10), 9_223_372_036_854_775_800L))
        .isEqualTo(
            new StatsUtil.PartitionSourceBounds<>(9_223_372_036_854_775_800L, Long.MAX_VALUE));
  }

  @Test
  void truncateDecimalUpperBoundDoesNotExceedPrecision() {
    // the upper bound would be 10.19, which is past the largest decimal(3, 2) value. Widths that
    // divide 10^3, like 10, never produce an upper bound past that value, so 30 is used.
    assertThat(
            sourceBounds(
                Types.DecimalType.of(3, 2), Transforms.truncate(30), new BigDecimal("9.90")))
        .isEqualTo(
            new StatsUtil.PartitionSourceBounds<>(new BigDecimal("9.90"), new BigDecimal("9.99")));
  }

  @Test
  void truncateStringShorterThanWidth() {
    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(3), "ab"))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>("ab", "ab"));
  }

  @Test
  void truncateStringShorterThanWidthCountsCodePoints() {
    // "ab" + U+1F4B0 is 3 code points but 4 Java chars, so it is only shorter than width 4 if the
    // width is counted in code points
    String value = "ab" + new String(Character.toChars(0x1F4B0));

    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(4), value))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(value, value));
  }

  @Test
  void truncateStringUpperBoundIsNextPrefix() {
    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(3), "abc"))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>("abc", "abd"));
  }

  @Test
  void truncateStringUpperBoundIncrementsSurrogatePair() {
    // U+1F7FF is stored as two Java chars, and incrementing it to U+1F800 changes both chars,
    // so the upper bound is only correct if the code point is incremented rather than the last char
    String prefix = "ab" + new String(Character.toChars(0x1F7FF));
    String nextPrefix = "ab" + new String(Character.toChars(0x1F800));

    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(3), prefix))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(prefix, nextPrefix));
  }

  @Test
  void truncateStringUpperBoundSkipsMaxCodePoint() {
    // MAX_CODE_POINT has no next code point, so "a" is incremented
    String prefix = "a" + new String(Character.toChars(Character.MAX_CODE_POINT));

    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(2), prefix))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(prefix, "b"));
  }

  @Test
  void truncateStringNoUpperBoundAtMaxCodePoint() {
    String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));

    assertThat(sourceBounds(Types.StringType.get(), Transforms.truncate(1), maxCodePoint))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(maxCodePoint, null));
  }

  @Test
  void truncateBinaryBounds() {
    ByteBuffer prefix = ByteBuffer.wrap(new byte[] {1, 2});

    assertThat(sourceBounds(Types.BinaryType.get(), Transforms.truncate(2), prefix))
        .isEqualTo(
            new StatsUtil.PartitionSourceBounds<>(prefix, ByteBuffer.wrap(new byte[] {1, 3})));
  }

  @Test
  void truncateShortBinary() {
    ByteBuffer value = ByteBuffer.wrap(new byte[] {1});

    assertThat(sourceBounds(Types.BinaryType.get(), Transforms.truncate(2), value))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(value, value));
  }

  private static final List<Transform<?, ?>> BINARY_TRANSFORMS =
      List.of(Transforms.identity(), Transforms.truncate(1), Transforms.truncate(2));

  @ParameterizedTest
  @FieldSource("BINARY_TRANSFORMS")
  void binaryBoundsDoNotSharePosition(Transform<?, ?> transform) {
    ByteBuffer value = ByteBuffer.wrap(new byte[] {1});
    StatsUtil.PartitionSourceBounds<?> bounds =
        sourceBounds(Types.BinaryType.get(), transform, value);
    ByteBuffer lower = (ByteBuffer) bounds.lower();
    ByteBuffer upper = (ByteBuffer) bounds.upper();

    lower.get();
    upper.get();

    assertThat(lower.position()).isOne();
    assertThat(upper.position()).isOne();
    assertThat(value.position()).isZero();
  }

  @Test
  void truncateBinaryUpperBoundSkipsMaxByte() {
    ByteBuffer prefix = ByteBuffer.wrap(new byte[] {1, (byte) 0xFF});

    assertThat(sourceBounds(Types.BinaryType.get(), Transforms.truncate(2), prefix))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(prefix, ByteBuffer.wrap(new byte[] {2})));
  }

  @Test
  void truncateBinaryNoUpperBoundAtMaxBytes() {
    ByteBuffer prefix = ByteBuffer.wrap(new byte[] {(byte) 0xFF, (byte) 0xFF});

    assertThat(sourceBounds(Types.BinaryType.get(), Transforms.truncate(2), prefix))
        .isEqualTo(new StatsUtil.PartitionSourceBounds<>(prefix, null));
  }

  private static Stream<Partition> timePartitionValues() {
    Type date = Types.DateType.get();
    Type timestamp = Types.TimestampType.withZone();
    Type timestampNano = Types.TimestampNanoType.withZone();
    return Stream.of(
        partition(date, Transforms.year(), years("2024-01-01"))
            .withExpectedBounds(days("2024-01-01"), days("2024-12-31")),
        partition(date, Transforms.month(), months("2024-02-01"))
            .withExpectedBounds(days("2024-02-01"), days("2024-02-29")),
        partition(date, Transforms.day(), days("2024-02-10"))
            .withExpectedBounds(days("2024-02-10"), days("2024-02-10")),
        partition(timestamp, Transforms.year(), years("2024-01-01"))
            .withExpectedBounds(
                micros("2024-01-01T00:00:00.000000+00:00"),
                micros("2024-12-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.month(), months("2024-02-01"))
            .withExpectedBounds(
                micros("2024-02-01T00:00:00.000000+00:00"),
                micros("2024-02-29T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.day(), days("2024-02-10"))
            .withExpectedBounds(
                micros("2024-02-10T00:00:00.000000+00:00"),
                micros("2024-02-10T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.hour(), hours("2024-02-10T05:00:00.000000+00:00"))
            .withExpectedBounds(
                micros("2024-02-10T05:00:00.000000+00:00"),
                micros("2024-02-10T05:59:59.999999+00:00")),
        partition(timestampNano, Transforms.year(), years("2024-01-01"))
            .withExpectedBounds(
                nanos("2024-01-01T00:00:00.000000000+00:00"),
                nanos("2024-12-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.month(), months("2024-02-01"))
            .withExpectedBounds(
                nanos("2024-02-01T00:00:00.000000000+00:00"),
                nanos("2024-02-29T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.day(), days("2024-02-10"))
            .withExpectedBounds(
                nanos("2024-02-10T00:00:00.000000000+00:00"),
                nanos("2024-02-10T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.hour(), hours("2024-02-10T05:00:00.000000000+00:00"))
            .withExpectedBounds(
                nanos("2024-02-10T05:00:00.000000000+00:00"),
                nanos("2024-02-10T05:59:59.999999999+00:00")));
  }

  @ParameterizedTest
  @MethodSource("timePartitionValues")
  void timeBounds(Partition partition) {
    assertThat(sourceBounds(partition)).isEqualTo(partition.expected());
  }

  private static Stream<Partition> timePartitionValuesAtEpoch() {
    Type date = Types.DateType.get();
    Type timestamp = Types.TimestampType.withZone();
    Type timestampNano = Types.TimestampNanoType.withZone();
    return Stream.of(
        partition(date, Transforms.year(), 0)
            .withExpectedBounds(days("1969-01-01"), days("1970-12-31")),
        partition(date, Transforms.month(), 0)
            .withExpectedBounds(days("1969-12-01"), days("1970-01-31")),
        partition(date, Transforms.day(), 0)
            .withExpectedBounds(days("1970-01-01"), days("1970-01-01")),
        partition(timestamp, Transforms.year(), 0)
            .withExpectedBounds(
                micros("1969-01-01T00:00:00.000000+00:00"),
                micros("1970-12-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.month(), 0)
            .withExpectedBounds(
                micros("1969-12-01T00:00:00.000000+00:00"),
                micros("1970-01-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.day(), 0)
            .withExpectedBounds(
                micros("1969-12-31T00:00:00.000000+00:00"),
                micros("1970-01-01T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.hour(), 0)
            .withExpectedBounds(
                micros("1969-12-31T23:00:00.000000+00:00"),
                micros("1970-01-01T00:59:59.999999+00:00")),
        partition(timestampNano, Transforms.year(), 0)
            .withExpectedBounds(
                nanos("1969-01-01T00:00:00.000000000+00:00"),
                nanos("1970-12-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.month(), 0)
            .withExpectedBounds(
                nanos("1969-12-01T00:00:00.000000000+00:00"),
                nanos("1970-01-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.day(), 0)
            .withExpectedBounds(
                nanos("1969-12-31T00:00:00.000000000+00:00"),
                nanos("1970-01-01T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.hour(), 0)
            .withExpectedBounds(
                nanos("1969-12-31T23:00:00.000000000+00:00"),
                nanos("1970-01-01T00:59:59.999999999+00:00")));
  }

  @ParameterizedTest
  @MethodSource("timePartitionValuesAtEpoch")
  void timeBoundsAtEpoch(Partition partition) {
    assertThat(sourceBounds(partition)).isEqualTo(partition.expected());
  }

  private static Stream<Partition> timePartitionValuesBefore1970() {
    Type date = Types.DateType.get();
    Type timestamp = Types.TimestampType.withZone();
    Type timestampNano = Types.TimestampNanoType.withZone();
    return Stream.of(
        partition(date, Transforms.year(), -1)
            .withExpectedBounds(days("1968-01-01"), days("1969-12-31")),
        partition(date, Transforms.month(), -1)
            .withExpectedBounds(days("1969-11-01"), days("1969-12-31")),
        partition(date, Transforms.day(), -1)
            .withExpectedBounds(days("1969-12-31"), days("1969-12-31")),
        partition(timestamp, Transforms.year(), -1)
            .withExpectedBounds(
                micros("1968-01-01T00:00:00.000000+00:00"),
                micros("1969-12-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.month(), -1)
            .withExpectedBounds(
                micros("1969-11-01T00:00:00.000000+00:00"),
                micros("1969-12-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.day(), -1)
            .withExpectedBounds(
                micros("1969-12-30T00:00:00.000000+00:00"),
                micros("1969-12-31T23:59:59.999999+00:00")),
        partition(timestamp, Transforms.hour(), -1)
            .withExpectedBounds(
                micros("1969-12-31T22:00:00.000000+00:00"),
                micros("1969-12-31T23:59:59.999999+00:00")),
        partition(timestampNano, Transforms.year(), -1)
            .withExpectedBounds(
                nanos("1968-01-01T00:00:00.000000000+00:00"),
                nanos("1969-12-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.month(), -1)
            .withExpectedBounds(
                nanos("1969-11-01T00:00:00.000000000+00:00"),
                nanos("1969-12-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.day(), -1)
            .withExpectedBounds(
                nanos("1969-12-30T00:00:00.000000000+00:00"),
                nanos("1969-12-31T23:59:59.999999999+00:00")),
        partition(timestampNano, Transforms.hour(), -1)
            .withExpectedBounds(
                nanos("1969-12-31T22:00:00.000000000+00:00"),
                nanos("1969-12-31T23:59:59.999999999+00:00")));
  }

  @ParameterizedTest
  @MethodSource("timePartitionValuesBefore1970")
  void timeBoundsBefore1970(Partition partition) {
    assertThat(sourceBounds(partition)).isEqualTo(partition.expected());
  }

  @Test
  void nullPartitionValueHasNoBounds() {
    assertThat(sourceBounds(Types.TimestampType.withZone(), Transforms.hour(), null)).isNull();
  }

  @Test
  void nonOrderPreservingTransformHasNoBounds() {
    assertThat(sourceBounds(Types.StringType.get(), Transforms.bucket(16), 3)).isNull();
  }

  private static StatsUtil.PartitionSourceBounds<?> sourceBounds(
      Type type, Transform<?, ?> transform, Object partitionValue) {
    Schema schema = new Schema(Types.NestedField.optional(1, "col", type));
    PartitionField field =
        PartitionSpec.builderFor(schema).add(1, "part", transform).build().fields().get(0);
    return StatsUtil.sourceBoundsFor(schema, field, partitionValue);
  }

  private static StatsUtil.PartitionSourceBounds<?> sourceBounds(Partition partition) {
    return sourceBounds(partition.type(), partition.transform(), partition.value());
  }

  private static Partition partition(Type type, Transform<?, ?> transform, Object value) {
    return new Partition(type, transform, value, null);
  }

  private static Partition identity(Type type, Object value) {
    return partition(type, Transforms.identity(), value).withExpectedBounds(value, value);
  }

  private record Partition(
      Type type,
      Transform<?, ?> transform,
      Object value,
      StatsUtil.PartitionSourceBounds<?> expected) {
    Partition withExpectedBounds(Object lower, Object upper) {
      return new Partition(
          type, transform, value, new StatsUtil.PartitionSourceBounds<>(lower, upper));
    }
  }

  private static int years(String isoDate) {
    return DateTimeUtil.daysToYears(days(isoDate));
  }

  private static int months(String isoDate) {
    return DateTimeUtil.daysToMonths(days(isoDate));
  }

  private static int days(String isoDate) {
    return DateTimeUtil.isoDateToDays(isoDate);
  }

  private static int hours(String isoTimestamp) {
    return DateTimeUtil.nanosToHours(nanos(isoTimestamp));
  }

  private static long micros(String isoTimestamp) {
    return DateTimeUtil.isoTimestamptzToMicros(isoTimestamp);
  }

  private static long nanos(String isoTimestamp) {
    return DateTimeUtil.isoTimestamptzToNanos(isoTimestamp);
  }

  /**
   * Assert that two struct types match in field IDs, names, optionality, and types, ignoring docs.
   */
  private static void assertSameStructure(Types.StructType expected, Types.StructType actual) {
    assertThat(actual.fields()).as("Number of fields").hasSameSizeAs(expected.fields());

    for (int i = 0; i < expected.fields().size(); i += 1) {
      Types.NestedField expectedField = expected.fields().get(i);
      Types.NestedField actualField = actual.fields().get(i);

      assertThat(actualField.fieldId()).as("Field ID").isEqualTo(expectedField.fieldId());
      assertThat(actualField.name()).as("Field name").isEqualTo(expectedField.name());
      assertThat(actualField.isOptional())
          .as("Field optionality for %s", expectedField.name())
          .isEqualTo(expectedField.isOptional());

      if (expectedField.type().isStructType()) {
        assertThat(actualField.type().isStructType())
            .as("Field %s should be a struct", expectedField.name())
            .isTrue();
        assertSameStructure(expectedField.type().asStructType(), actualField.type().asStructType());
      } else {
        assertThat(actualField.type())
            .as("Field type for %s", expectedField.name())
            .isEqualTo(expectedField.type());
      }
    }
  }
}
