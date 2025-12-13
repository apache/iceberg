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
package org.apache.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.connect.IcebergSinkConfig;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.BinaryType;
import org.apache.iceberg.types.Types.BooleanType;
import org.apache.iceberg.types.Types.DateType;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.FloatType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimeType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class TestSchemaUtils {

  private static final org.apache.iceberg.Schema SIMPLE_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "i", IntegerType.get()),
          NestedField.required(2, "f", FloatType.get()));

  private static final org.apache.iceberg.Schema NESTED_SCHEMA =
      new org.apache.iceberg.Schema(
          NestedField.required(3, "s", StringType.get()),
          NestedField.required(4, "st", StructType.of(SIMPLE_SCHEMA.columns())));

  private static final org.apache.iceberg.Schema SCHEMA_FOR_SPEC =
      new org.apache.iceberg.Schema(
          NestedField.required(1, "i", IntegerType.get()),
          NestedField.required(2, "s", StringType.get()),
          NestedField.required(3, "ts1", TimestampType.withZone()),
          NestedField.required(4, "ts2", TimestampType.withZone()),
          NestedField.required(5, "ts3", TimestampType.withZone()),
          NestedField.required(6, "ts4", TimestampType.withZone()));

  @Test
  public void testApplySchemaUpdates() {
    UpdateSchema updateSchema = mock(UpdateSchema.class);
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    when(table.updateSchema()).thenReturn(updateSchema);

    // the updates to "i" should be ignored as it already exists and is the same type
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    consumer.addColumn(null, "i", IntegerType.get());
    consumer.updateType("i", IntegerType.get());
    consumer.makeOptional("i");
    consumer.updateType("f", DoubleType.get());
    consumer.addColumn(null, "s", StringType.get());

    SchemaUtils.applySchemaUpdates(table, consumer);
    verify(table).refresh();
    verify(table).updateSchema();

    verify(updateSchema).addColumn(isNull(), eq("s"), isA(StringType.class), isNull(), isNull());
    verify(updateSchema).updateColumn(eq("f"), isA(DoubleType.class));
    verify(updateSchema).makeColumnOptional(eq("i"));
    verify(updateSchema).commit();

    // check that there are no unexpected invocations...
    verify(updateSchema).addColumn(isNull(), anyString(), any(), isNull(), any());
    verify(updateSchema).updateColumn(any(), any());
    verify(updateSchema).makeColumnOptional(any());
  }

  @Test
  public void testApplyNestedSchemaUpdates() {
    UpdateSchema updateSchema = mock(UpdateSchema.class);
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(NESTED_SCHEMA);
    when(table.updateSchema()).thenReturn(updateSchema);

    // the updates to "st.i" should be ignored as it already exists and is the same type
    SchemaUpdate.Consumer consumer = new SchemaUpdate.Consumer();
    consumer.addColumn("st", "i", IntegerType.get());
    consumer.updateType("st.i", IntegerType.get());
    consumer.makeOptional("st.i");
    consumer.updateType("st.f", DoubleType.get());
    consumer.addColumn("st", "s", StringType.get());

    SchemaUtils.applySchemaUpdates(table, consumer);
    verify(table).refresh();
    verify(table).updateSchema();

    verify(updateSchema).addColumn(eq("st"), eq("s"), isA(StringType.class), isNull(), isNull());
    verify(updateSchema).updateColumn(eq("st.f"), isA(DoubleType.class));
    verify(updateSchema).makeColumnOptional(eq("st.i"));
    verify(updateSchema).commit();

    // check that there are no unexpected invocations...
    verify(updateSchema).addColumn(anyString(), anyString(), any(), isNull(), any());
    verify(updateSchema).updateColumn(any(), any());
    verify(updateSchema).makeColumnOptional(any());
  }

  @Test
  public void testApplySchemaUpdatesNoUpdates() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    SchemaUtils.applySchemaUpdates(table, null);
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();

    SchemaUtils.applySchemaUpdates(table, new SchemaUpdate.Consumer());
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();
  }

  @Test
  public void testNeedsDataTypeUpdate() {
    // valid updates
    assertThat(SchemaUtils.needsDataTypeUpdate(FloatType.get(), Schema.FLOAT64_SCHEMA))
        .isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.needsDataTypeUpdate(IntegerType.get(), Schema.INT64_SCHEMA))
        .isInstanceOf(LongType.class);

    // other updates will be skipped
    assertThat(SchemaUtils.needsDataTypeUpdate(IntegerType.get(), Schema.STRING_SCHEMA)).isNull();
    assertThat(SchemaUtils.needsDataTypeUpdate(FloatType.get(), Schema.STRING_SCHEMA)).isNull();
    assertThat(SchemaUtils.needsDataTypeUpdate(StringType.get(), Schema.INT64_SCHEMA)).isNull();
  }

  @Test
  public void testCreatePartitionSpecUnpartitioned() {
    PartitionSpec spec = SchemaUtils.createPartitionSpec(SCHEMA_FOR_SPEC, ImmutableList.of());
    assertThat(spec.isPartitioned()).isFalse();
  }

  @Test
  public void testCreatePartitionSpec() {
    List<String> partitionFields =
        ImmutableList.of(
            "year(ts1)",
            "month(ts2)",
            "day(ts3)",
            "hour(ts4)",
            "bucket(i, 4)",
            "truncate(s, 10)",
            "s");
    PartitionSpec spec = SchemaUtils.createPartitionSpec(SCHEMA_FOR_SPEC, partitionFields);
    assertThat(spec.isPartitioned()).isTrue();
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.year()));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.month()));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.day()));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.hour()));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.bucket(4)));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.truncate(10)));
    assertThat(spec.fields()).anyMatch(val -> matchingTransform(val, Transforms.identity()));
  }

  boolean matchingTransform(PartitionField partitionField, Transform<?, ?> expectedTransform) {
    return partitionField.transform().equals(expectedTransform);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testToIcebergType(boolean forceOptional) {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaForceOptional()).thenReturn(forceOptional);

    int formatVersion = 2; // Use format version 2 for basic type conversion tests

    assertThat(SchemaUtils.toIcebergType(Schema.BOOLEAN_SCHEMA, config, formatVersion))
        .isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.BYTES_SCHEMA, config, formatVersion))
        .isInstanceOf(BinaryType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT8_SCHEMA, config, formatVersion))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT16_SCHEMA, config, formatVersion))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT32_SCHEMA, config, formatVersion))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT64_SCHEMA, config, formatVersion))
        .isInstanceOf(LongType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT32_SCHEMA, config, formatVersion))
        .isInstanceOf(FloatType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT64_SCHEMA, config, formatVersion))
        .isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.STRING_SCHEMA, config, formatVersion))
        .isInstanceOf(StringType.class);
    assertThat(SchemaUtils.toIcebergType(Date.SCHEMA, config, formatVersion))
        .isInstanceOf(DateType.class);
    assertThat(SchemaUtils.toIcebergType(Time.SCHEMA, config, formatVersion))
        .isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.toIcebergType(Timestamp.SCHEMA, config, formatVersion);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    Type decimalType = SchemaUtils.toIcebergType(Decimal.schema(4), config, formatVersion);
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(4);

    Type listType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.array(Schema.STRING_SCHEMA).build(), config, formatVersion);
    assertThat(listType).isInstanceOf(ListType.class);
    assertThat(listType.asListType().elementType()).isInstanceOf(StringType.class);
    assertThat(listType.asListType().isElementOptional()).isEqualTo(forceOptional);

    Type mapType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(),
            config,
            formatVersion);
    assertThat(mapType).isInstanceOf(MapType.class);
    assertThat(mapType.asMapType().keyType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().valueType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().isValueOptional()).isEqualTo(forceOptional);

    Type structType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).build(), config, formatVersion);
    assertThat(structType).isInstanceOf(StructType.class);
    assertThat(structType.asStructType().fieldType("i")).isInstanceOf(IntegerType.class);
    assertThat(structType.asStructType().field("i").isOptional()).isEqualTo(forceOptional);
  }

  @Test
  public void testInferIcebergType() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);

    assertThat(SchemaUtils.inferIcebergType(1, config)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1L, config)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1f, config)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1d, config)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType("foobar", config)).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(true, config)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalDate.now(), config)).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalTime.now(), config)).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.inferIcebergType(new java.util.Date(), config);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(OffsetDateTime.now(), config);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(LocalDateTime.now(), config);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isFalse();

    Type decimalType = SchemaUtils.inferIcebergType(new BigDecimal("12.345"), config);
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(3);

    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of("foobar"), config))
        .isInstanceOf(ListType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("foo", "bar"), config))
        .isInstanceOf(StructType.class);
  }

  @Test
  public void testInferIcebergTypeEmpty() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);

    // skip infer for null
    assertThat(SchemaUtils.inferIcebergType(null, config)).isNull();

    // skip infer for empty list
    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of(), config)).isNull();
    // skip infer for list if first element is null
    List<?> list = Lists.newArrayList();
    list.add(null);
    assertThat(SchemaUtils.inferIcebergType(list, config)).isNull();
    // skip infer for list if first element is an empty object
    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of(ImmutableMap.of()), config)).isNull();

    // skip infer for empty object
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of(), config)).isNull();
    // skip infer for object if values are null
    Map<String, ?> map = Maps.newHashMap();
    map.put("col", null);
    assertThat(SchemaUtils.inferIcebergType(map, config)).isNull();
    // skip infer for object if values are empty objects
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("nested", ImmutableMap.of()), config))
        .isNull();
  }

  @Test
  public void testConvertDefaultValuePrimitiveTypes() {
    // Test boolean
    org.apache.iceberg.expressions.Literal<?> boolLit =
        SchemaUtils.convertDefaultValue(true, BooleanType.get());
    assertThat(boolLit).isNotNull();
    assertThat(boolLit.value()).isEqualTo(true);

    // Test integer
    org.apache.iceberg.expressions.Literal<?> intLit =
        SchemaUtils.convertDefaultValue(42, IntegerType.get());
    assertThat(intLit).isNotNull();
    assertThat(intLit.value()).isEqualTo(42);

    // Test long
    org.apache.iceberg.expressions.Literal<?> longLit =
        SchemaUtils.convertDefaultValue(1234567890L, LongType.get());
    assertThat(longLit).isNotNull();
    assertThat(longLit.value()).isEqualTo(1234567890L);

    // Test float
    org.apache.iceberg.expressions.Literal<?> floatLit =
        SchemaUtils.convertDefaultValue(3.14f, FloatType.get());
    assertThat(floatLit).isNotNull();
    assertThat(floatLit.value()).isEqualTo(3.14f);

    // Test double
    org.apache.iceberg.expressions.Literal<?> doubleLit =
        SchemaUtils.convertDefaultValue(2.71828, DoubleType.get());
    assertThat(doubleLit).isNotNull();
    assertThat(doubleLit.value()).isEqualTo(2.71828);

    // Test string
    org.apache.iceberg.expressions.Literal<?> stringLit =
        SchemaUtils.convertDefaultValue("default_value", StringType.get());
    assertThat(stringLit).isNotNull();
    assertThat(stringLit.value()).isEqualTo("default_value");
  }

  @Test
  public void testConvertDefaultValueDecimal() {
    Schema decimalSchema =
        Decimal.builder(2).parameter(Decimal.SCALE_FIELD, "2").optional().build();
    BigDecimal value = new BigDecimal("12.34");

    org.apache.iceberg.expressions.Literal<?> decimalLit =
        SchemaUtils.convertDefaultValue(value, DecimalType.of(10, 2));
    assertThat(decimalLit).isNotNull();
    assertThat(decimalLit.value()).isEqualTo(value);
  }

  @Test
  public void testConvertDefaultValueNull() {
    org.apache.iceberg.expressions.Literal<?> nullLit =
        SchemaUtils.convertDefaultValue(null, StringType.get());
    assertThat(nullLit).isNull();
  }

  @Test
  public void testConvertDefaultValueNestedTypes() {
    // Nested types (LIST, MAP, STRUCT) should return null for non-null defaults
    org.apache.iceberg.expressions.Literal<?> listLit =
        SchemaUtils.convertDefaultValue(
            ImmutableList.of("value"), ListType.ofOptional(1, StringType.get()));
    assertThat(listLit).isNull();
  }

  @Test
  public void testConvertDefaultValueWithNumberConversion() {
    // Test that Number can be converted to different types
    org.apache.iceberg.expressions.Literal<?> intFromLong =
        SchemaUtils.convertDefaultValue(100L, IntegerType.get());
    assertThat(intFromLong).isNotNull();
    assertThat(intFromLong.value()).isEqualTo(100);

    org.apache.iceberg.expressions.Literal<?> longFromInt =
        SchemaUtils.convertDefaultValue(50, LongType.get());
    assertThat(longFromInt).isNotNull();
    assertThat(longFromInt.value()).isEqualTo(50L);

    org.apache.iceberg.expressions.Literal<?> floatFromDouble =
        SchemaUtils.convertDefaultValue(1.5, FloatType.get());
    assertThat(floatFromDouble).isNotNull();
    assertThat(floatFromDouble.value()).isEqualTo(1.5f);
  }

  @Test
  public void testConvertDefaultValueTemporalTypes() {
    // Test DATE - should convert to days from epoch
    java.util.Date date = new java.util.Date(0); // Epoch
    org.apache.iceberg.expressions.Literal<?> dateLit =
        SchemaUtils.convertDefaultValue(date, DateType.get());
    assertThat(dateLit).isNotNull();
    assertThat(dateLit.value()).isEqualTo(0); // Day 0 (1970-01-01)

    // Test DATE from LocalDate
    LocalDate localDate = LocalDate.of(2024, 1, 1);
    org.apache.iceberg.expressions.Literal<?> localDateLit =
        SchemaUtils.convertDefaultValue(localDate, DateType.get());
    assertThat(localDateLit).isNotNull();
    assertThat(localDateLit.value()).isEqualTo((int) localDate.toEpochDay());

    // Test TIME - should convert to microseconds from midnight
    java.util.Date time = new java.util.Date(3600000); // 1 hour in millis
    org.apache.iceberg.expressions.Literal<?> timeLit =
        SchemaUtils.convertDefaultValue(time, TimeType.get());
    assertThat(timeLit).isNotNull();
    assertThat(timeLit.value()).isEqualTo(3600000L * 1000); // Converted to micros

    // Test TIMESTAMP - should use micros() method
    java.util.Date timestamp = new java.util.Date(1000); // 1 second
    org.apache.iceberg.expressions.Literal<?> timestampLit =
        SchemaUtils.convertDefaultValue(timestamp, TimestampType.withZone());
    assertThat(timestampLit).isNotNull();
    // The value should be in microseconds (1000 millis = 1000000 micros)
    assertThat(timestampLit.value()).isEqualTo(1000L * 1000);
  }

  @Test
  public void testSchemaGeneratorExtractsDefaultsV3() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaForceOptional()).thenReturn(false);

    Schema kafkaSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", SchemaBuilder.string().defaultValue("unknown").build())
            .field("age", SchemaBuilder.int32().defaultValue(0).build())
            .field("active", SchemaBuilder.bool().defaultValue(true).build())
            .build();

    // Test with format version 3 - should extract defaults
    Type icebergType = SchemaUtils.toIcebergType(kafkaSchema, config, 3);
    assertThat(icebergType).isInstanceOf(StructType.class);

    StructType structType = (StructType) icebergType;
    assertThat(structType.fields()).hasSize(4);

    // Fields by name (not index)
    NestedField idField = structType.field("id");
    assertThat(idField.name()).isEqualTo("id");
    assertThat(idField.initialDefault()).isNull();
    assertThat(idField.writeDefault()).isNull();

    // Field with string default
    NestedField nameField = structType.field("name");
    assertThat(nameField.name()).isEqualTo("name");
    assertThat(nameField.initialDefault()).isEqualTo("unknown");
    assertThat(nameField.writeDefault()).isEqualTo("unknown");

    // Field with integer default
    NestedField ageField = structType.field("age");
    assertThat(ageField.name()).isEqualTo("age");
    assertThat(ageField.initialDefault()).isEqualTo(0);
    assertThat(ageField.writeDefault()).isEqualTo(0);

    // Field with boolean default
    NestedField activeField = structType.field("active");
    assertThat(activeField.name()).isEqualTo("active");
    assertThat(activeField.initialDefault()).isEqualTo(true);
    assertThat(activeField.writeDefault()).isEqualTo(true);
  }

  @Test
  public void testSchemaGeneratorIgnoresDefaultsV2() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaForceOptional()).thenReturn(false);

    Schema kafkaSchema =
        SchemaBuilder.struct()
            .field("id", Schema.INT32_SCHEMA)
            .field("name", SchemaBuilder.string().defaultValue("unknown").build())
            .field("age", SchemaBuilder.int32().defaultValue(0).build())
            .field("active", SchemaBuilder.bool().defaultValue(true).build())
            .build();

    // Test with format version 2 - should NOT extract defaults
    Type icebergType = SchemaUtils.toIcebergType(kafkaSchema, config, 2);
    assertThat(icebergType).isInstanceOf(StructType.class);

    StructType structType = (StructType) icebergType;
    assertThat(structType.fields()).hasSize(4);

    // All fields should have null defaults when using format version 2
    for (NestedField field : structType.fields()) {
      assertThat(field.initialDefault())
          .as("Field %s should not have initial default in format v2", field.name())
          .isNull();
      assertThat(field.writeDefault())
          .as("Field %s should not have write default in format v2", field.name())
          .isNull();
    }
  }

  @Test
  public void testSchemaGeneratorWithDifferentFormatVersions() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaForceOptional()).thenReturn(false);

    Schema kafkaSchema =
        SchemaBuilder.struct()
            .field("name", SchemaBuilder.string().defaultValue("test").build())
            .build();

    // Format version 1 - no defaults
    Type v1Type = SchemaUtils.toIcebergType(kafkaSchema, config, 1);
    assertThat(v1Type.asStructType().field("name").initialDefault()).isNull();
    assertThat(v1Type.asStructType().field("name").writeDefault()).isNull();

    // Format version 2 - no defaults
    Type v2Type = SchemaUtils.toIcebergType(kafkaSchema, config, 2);
    assertThat(v2Type.asStructType().field("name").initialDefault()).isNull();
    assertThat(v2Type.asStructType().field("name").writeDefault()).isNull();

    // Format version 3 - with defaults
    Type v3Type = SchemaUtils.toIcebergType(kafkaSchema, config, 3);
    assertThat(v3Type.asStructType().field("name").initialDefault()).isEqualTo("test");
    assertThat(v3Type.asStructType().field("name").writeDefault()).isEqualTo("test");
  }

  @Test
  public void testFormatVersionFromTableUsedForSchemaEvolution() {
    // This test verifies that the table's format version is used, not the config
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);
    when(config.schemaForceOptional()).thenReturn(false);

    Schema kafkaSchemaWithDefaults =
        SchemaBuilder.struct()
            .field("newColumn", SchemaBuilder.string().defaultValue("default_value").build())
            .build();

    // When table is v3, defaults should be included regardless of config
    Type typeFromV3Table = SchemaUtils.toIcebergType(kafkaSchemaWithDefaults, config, 3);
    assertThat(typeFromV3Table.asStructType().field("newColumn").initialDefault())
        .isEqualTo("default_value");

    // When table is v2, defaults should NOT be included
    Type typeFromV2Table = SchemaUtils.toIcebergType(kafkaSchemaWithDefaults, config, 2);
    assertThat(typeFromV2Table.asStructType().field("newColumn").initialDefault()).isNull();
  }
}
