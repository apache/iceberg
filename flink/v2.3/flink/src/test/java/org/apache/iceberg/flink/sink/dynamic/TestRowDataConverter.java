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
package org.apache.iceberg.flink.sink.dynamic;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.stream.Stream;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestRowDataConverter {

  static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  static final Schema SCHEMA2 =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()),
          Types.NestedField.optional(3, "onemore", Types.DoubleType.get()));

  @Test
  void testPrimitiveTypes() {
    DataGenerator generator = new DataGenerators.Primitives();
    assertThat(
            convert(
                generator.generateFlinkRowData(),
                generator.icebergSchema(),
                generator.icebergSchema()))
        .isEqualTo(generator.generateFlinkRowData());
  }

  @Test
  void testAddColumn() {
    assertThat(convert(SimpleDataUtil.createRowData(1, "a"), SCHEMA, SCHEMA2))
        .isEqualTo(GenericRowData.of(1, StringData.fromString("a"), null));
  }

  @Test
  void testAddRequiredColumn() {
    Schema currentSchema = new Schema(Types.NestedField.optional(1, "id", Types.IntegerType.get()));
    Schema targetSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));

    assertThatThrownBy(() -> convert(GenericRowData.of(42), currentSchema, targetSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is non-nullable but does not exist in source schema");
  }

  @Test
  void testIntToLong() {
    Schema schemaWithLong =
        new Schema(
            Types.NestedField.optional(2, "id", Types.LongType.get()),
            Types.NestedField.optional(4, "data", Types.StringType.get()));

    assertThat(convert(SimpleDataUtil.createRowData(1, "a"), SimpleDataUtil.SCHEMA, schemaWithLong))
        .isEqualTo(GenericRowData.of(1L, StringData.fromString("a")));
  }

  @Test
  void testFloatToDouble() {
    Schema schemaWithFloat =
        new Schema(Types.NestedField.optional(1, "float2double", Types.FloatType.get()));
    Schema schemaWithDouble =
        new Schema(Types.NestedField.optional(2, "float2double", Types.DoubleType.get()));

    assertThat(convert(GenericRowData.of(1.5f), schemaWithFloat, schemaWithDouble))
        .isEqualTo(GenericRowData.of(1.5d));
  }

  @Test
  void testDateToTimestamp() {
    Schema schemaWithDate =
        new Schema(Types.NestedField.optional(1, "date2timestamp", Types.DateType.get()));
    Schema schemaWithTimestamp =
        new Schema(
            Types.NestedField.optional(2, "date2timestamp", Types.TimestampType.withoutZone()));

    LocalDate date = LocalDate.of(2022, 1, 10);

    assertThat(
            convert(
                GenericRowData.of((int) date.toEpochDay()), schemaWithDate, schemaWithTimestamp))
        .isEqualTo(GenericRowData.of(TimestampData.fromLocalDateTime(date.atStartOfDay())));
  }

  @Test
  void testIncreasePrecision() {
    Schema before =
        new Schema(Types.NestedField.required(14, "decimal_field", Types.DecimalType.of(9, 2)));
    Schema after =
        new Schema(Types.NestedField.required(14, "decimal_field", Types.DecimalType.of(10, 2)));

    assertThat(
            convert(
                GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("-1.50"), 9, 2)),
                before,
                after))
        .isEqualTo(GenericRowData.of(DecimalData.fromBigDecimal(new BigDecimal("-1.50"), 10, 2)));
  }

  @ParameterizedTest
  @MethodSource("dataConversionCases")
  void testConversionsDeclaredByCompareSchemasVisitorAreSupported(
      Type.PrimitiveType dataType, Type.PrimitiveType tableType, Object input, Object expected) {
    Schema dataSchema = new Schema(optional(1, "field", dataType));
    Schema tableSchema = new Schema(optional(2, "field", tableType));

    assertThat(CompareSchemasVisitor.isDataConversionPossible(dataType, tableType)).isTrue();
    assertThat(convert(GenericRowData.of(input), dataSchema, tableSchema))
        .isEqualTo(GenericRowData.of(expected));
  }

  private static Stream<Arguments> dataConversionCases() {
    LocalDate date = LocalDate.of(2022, 1, 10);
    return Stream.of(
        Arguments.of(Types.IntegerType.get(), Types.LongType.get(), 1, 1L),
        Arguments.of(Types.FloatType.get(), Types.DoubleType.get(), 1.5f, 1.5d),
        Arguments.of(
            Types.DateType.get(),
            Types.TimestampType.withoutZone(),
            (int) date.toEpochDay(),
            TimestampData.fromLocalDateTime(date.atStartOfDay())),
        Arguments.of(
            Types.DecimalType.of(9, 2),
            Types.DecimalType.of(10, 2),
            DecimalData.fromBigDecimal(new BigDecimal("-1.50"), 9, 2),
            DecimalData.fromBigDecimal(new BigDecimal("-1.50"), 10, 2)));
  }

  @Test
  void testStructAddOptionalFields() {
    DataGenerator generator = new DataGenerators.StructOfPrimitive();
    RowData oldData = generator.generateFlinkRowData();
    Schema oldSchema = generator.icebergSchema();
    Types.NestedField structField = oldSchema.columns().get(1);
    Schema newSchema =
        new Schema(
            oldSchema.columns().get(0),
            Types.NestedField.required(
                10,
                structField.name(),
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    optional(103, "optional", Types.StringType.get()),
                    required(102, "name", Types.StringType.get()))));
    RowData newData =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            GenericRowData.of(1, null, StringData.fromString("Jane")));

    assertThat(convert(oldData, oldSchema, newSchema)).isEqualTo(newData);
  }

  @Test
  void testStructAddRequiredFieldsWithOptionalRoot() {
    DataGenerator generator = new DataGenerators.StructOfPrimitive();
    RowData oldData = generator.generateFlinkRowData();
    Schema oldSchema = generator.icebergSchema();
    Types.NestedField structField = oldSchema.columns().get(1);
    Schema newSchema =
        new Schema(
            oldSchema.columns().get(0),
            Types.NestedField.optional(
                10,
                "newFieldOptionalField",
                Types.StructType.of(
                    Types.NestedField.optional(
                        structField.fieldId(),
                        structField.name(),
                        Types.StructType.of(
                            optional(101, "id", Types.IntegerType.get()),
                            // Required columns which leads to nulling the entire struct
                            required(103, "required", Types.StringType.get()),
                            required(102, "name", Types.StringType.get()))))));

    RowData expectedData = GenericRowData.of(StringData.fromString("row_id_value"), null);

    assertThat(convert(oldData, oldSchema, newSchema)).isEqualTo(expectedData);
  }

  @Test
  void testStructAddRequiredFields() {
    DataGenerator generator = new DataGenerators.StructOfPrimitive();
    RowData oldData = generator.generateFlinkRowData();
    Schema oldSchema = generator.icebergSchema();
    Types.NestedField structField = oldSchema.columns().get(1);
    Schema newSchema =
        new Schema(
            oldSchema.columns().get(0),
            Types.NestedField.required(
                10,
                structField.name(),
                Types.StructType.of(
                    required(101, "id", Types.IntegerType.get()),
                    required(103, "required", Types.StringType.get()),
                    required(102, "name", Types.StringType.get()))));

    assertThatThrownBy(() -> convert(oldData, oldSchema, newSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("is non-nullable but does not exist in source schema");
  }

  @Test
  void testMap() {
    DataGenerator generator = new DataGenerators.MapOfPrimitives();
    RowData oldData = generator.generateFlinkRowData();
    Schema oldSchema = generator.icebergSchema();
    Types.NestedField mapField = oldSchema.columns().get(1);
    Schema newSchema =
        new Schema(
            oldSchema.columns().get(0),
            Types.NestedField.optional(
                10,
                mapField.name(),
                Types.MapType.ofRequired(101, 102, Types.StringType.get(), Types.LongType.get())));
    RowData newData =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericMapData(
                ImmutableMap.of(
                    StringData.fromString("Jane"), 1L, StringData.fromString("Joe"), 2L)));

    assertThat(convert(oldData, oldSchema, newSchema)).isEqualTo(newData);
  }

  @Test
  void testArray() {
    DataGenerator generator = new DataGenerators.ArrayOfPrimitive();
    RowData oldData = generator.generateFlinkRowData();
    Schema oldSchema = generator.icebergSchema();
    Types.NestedField arrayField = oldSchema.columns().get(1);
    Schema newSchema =
        new Schema(
            oldSchema.columns().get(0),
            Types.NestedField.optional(
                10, arrayField.name(), Types.ListType.ofOptional(101, Types.LongType.get())));
    RowData newData =
        GenericRowData.of(
            StringData.fromString("row_id_value"), new GenericArrayData(new Long[] {1L, 2L, 3L}));

    assertThat(convert(oldData, oldSchema, newSchema)).isEqualTo(newData);
  }

  private static RowData convert(RowData sourceData, Schema sourceSchema, Schema targetSchema) {
    return (RowData)
        DataConverter.get(
                FlinkSchemaUtil.convert(sourceSchema), FlinkSchemaUtil.convert(targetSchema))
            .convert(sourceData);
  }
}
