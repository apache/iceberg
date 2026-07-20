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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

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
    // count or average value size
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(true, type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final List<Type> VARIABLE_WIDTH_TYPES =
      List.of(Types.StringType.get(), Types.BinaryType.get());

  @ParameterizedTest
  @FieldSource("VARIABLE_WIDTH_TYPES")
  public void testVariableWidthPrimitiveStruct(Type type) {
    // variable-width types track bounds (including tight bounds) and an average value size, but no
    // NaN count
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "avg_value_size_in_bytes", Types.IntegerType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(true, type, 30_000, MetricsModes.Full.get());

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
    // and record an average value size
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
            Types.NestedField.optional(30_007, "avg_value_size_in_bytes", Types.IntegerType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(true, type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  private static final List<Type> FLOATING_POINT_TYPES =
      List.of(Types.FloatType.get(), Types.DoubleType.get());

  @ParameterizedTest
  @FieldSource("FLOATING_POINT_TYPES")
  public void testFloatingPointStruct(Type type) {
    // floating-point types track bounds (including tight bounds) and a NaN count, but have no
    // average value size
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_006, "nan_value_count", Types.LongType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(true, type, 30_000, MetricsModes.Full.get());

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
    assertThat(StatsUtil.fieldStatsStruct(true, type, 30_000, MetricsModes.Full.get())).isNull();
  }

  @Test
  public void testRequiredField() {
    // a required column does not produce a null_value_count field
    Type type = Types.IntegerType.get();
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_001, "lower_bound", type),
            Types.NestedField.optional(30_002, "upper_bound", type),
            Types.NestedField.optional(30_003, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(false, type, 30_000, MetricsModes.Full.get());

    assertSameStructure(expected, actual);
  }

  @Test
  public void testStringNoneMode() {
    // none mode produces no stats struct
    assertThat(
            StatsUtil.fieldStatsStruct(
                true, Types.StringType.get(), 30_000, MetricsModes.None.get()))
        .isNull();
  }

  @Test
  public void testStringCountsMode() {
    // counts mode drops the bounds (and tight_bounds) but keeps counts and the value size
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(30_004, "value_count", Types.LongType.get()),
            Types.NestedField.optional(30_005, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(30_007, "avg_value_size_in_bytes", Types.IntegerType.get()));

    Types.StructType actual =
        StatsUtil.fieldStatsStruct(true, Types.StringType.get(), 30_000, MetricsModes.Counts.get());

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
            Types.NestedField.optional(30_007, "avg_value_size_in_bytes", Types.IntegerType.get()));

    Types.StructType actual = StatsUtil.fieldStatsStruct(true, string, 30_000, mode);

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
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()));
    Types.StructType dataStats =
        Types.StructType.of(
            Types.NestedField.optional(10_401, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_402, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_403, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_404, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_405, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_407, "avg_value_size_in_bytes", Types.IntegerType.get()));
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
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()));
    Types.StructType categoryStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_607, "avg_value_size_in_bytes", Types.IntegerType.get()));
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
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()));
    Types.StructType dataStats =
        Types.StructType.of(
            Types.NestedField.optional(10_401, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_402, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_403, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_404, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_405, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_407, "avg_value_size_in_bytes", Types.IntegerType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_400, "data", dataStats));

    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsConfig.from(ImmutableMap.of(), schema, null));

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
            Types.NestedField.optional(10_204, "value_count", Types.LongType.get()));
    Types.StructType categoryStats =
        Types.StructType.of(
            Types.NestedField.optional(10_601, "lower_bound", Types.StringType.get()),
            Types.NestedField.optional(10_602, "upper_bound", Types.StringType.get()),
            Types.NestedField.optional(10_603, "tight_bounds", Types.BooleanType.get()),
            Types.NestedField.optional(10_604, "value_count", Types.LongType.get()),
            Types.NestedField.optional(10_605, "null_value_count", Types.LongType.get()),
            Types.NestedField.optional(10_607, "avg_value_size_in_bytes", Types.IntegerType.get()));
    Types.StructType expected =
        Types.StructType.of(
            Types.NestedField.optional(10_200, "id", idStats),
            Types.NestedField.optional(10_600, "category", categoryStats));

    Map<String, String> properties =
        ImmutableMap.of(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "data", "none");
    Types.StructType actual =
        StatsUtil.statsWriteSchema(schema, MetricsConfig.from(properties, schema, null));

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
        StatsUtil.statsWriteSchema(schema, MetricsConfig.from(properties, schema, null));

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
        StatsUtil.statsWriteSchema(schema, MetricsConfig.from(ImmutableMap.of(), schema, null));

    assertThat(actual.fields()).isEmpty();
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

  @Test
  public void testTracksStat() {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "req", Types.LongType.get()),
            Types.NestedField.optional(2, "opt", Types.LongType.get()));
    Types.StructType stats = StatsUtil.statsReadSchema(schema, List.of(1, 2));
    Types.StructType reqStats = stats.field("req").type().asStructType();
    Types.StructType optStats = stats.field("opt").type().asStructType();

    // value_count is tracked for every column
    assertThat(StatsUtil.tracksStat(reqStats, 1, StatsUtil.VALUE_COUNT_OFFSET)).isTrue();
    assertThat(StatsUtil.tracksStat(optStats, 2, StatsUtil.VALUE_COUNT_OFFSET)).isTrue();

    // null_value_count is tracked only for optional (nullable) columns
    assertThat(StatsUtil.tracksStat(reqStats, 1, StatsUtil.NULL_VALUE_COUNT_OFFSET)).isFalse();
    assertThat(StatsUtil.tracksStat(optStats, 2, StatsUtil.NULL_VALUE_COUNT_OFFSET)).isTrue();
  }
}
