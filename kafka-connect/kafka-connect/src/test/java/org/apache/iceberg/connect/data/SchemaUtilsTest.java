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

public class SchemaUtilsTest {

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

    verify(updateSchema).addColumn(isNull(), eq("s"), isA(StringType.class));
    verify(updateSchema).updateColumn(eq("f"), isA(DoubleType.class));
    verify(updateSchema).makeColumnOptional(eq("i"));
    verify(updateSchema).commit();

    // check that there are no unexpected invocations...
    verify(updateSchema).addColumn(isNull(), anyString(), any());
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

    verify(updateSchema).addColumn(eq("st"), eq("s"), isA(StringType.class));
    verify(updateSchema).updateColumn(eq("st.f"), isA(DoubleType.class));
    verify(updateSchema).makeColumnOptional(eq("st.i"));
    verify(updateSchema).commit();

    // check that there are no unexpected invocations...
    verify(updateSchema).addColumn(anyString(), anyString(), any());
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

    assertThat(SchemaUtils.toIcebergType(Schema.BOOLEAN_SCHEMA, config))
        .isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.BYTES_SCHEMA, config))
        .isInstanceOf(BinaryType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT8_SCHEMA, config))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT16_SCHEMA, config))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT32_SCHEMA, config))
        .isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT64_SCHEMA, config)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT32_SCHEMA, config))
        .isInstanceOf(FloatType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT64_SCHEMA, config))
        .isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.STRING_SCHEMA, config))
        .isInstanceOf(StringType.class);
    assertThat(SchemaUtils.toIcebergType(Date.SCHEMA, config)).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.toIcebergType(Time.SCHEMA, config)).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.toIcebergType(Timestamp.SCHEMA, config);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    Type decimalType = SchemaUtils.toIcebergType(Decimal.schema(4), config);
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(4);

    Type listType =
        SchemaUtils.toIcebergType(SchemaBuilder.array(Schema.STRING_SCHEMA).build(), config);
    assertThat(listType).isInstanceOf(ListType.class);
    assertThat(listType.asListType().elementType()).isInstanceOf(StringType.class);
    assertThat(listType.asListType().isElementOptional()).isEqualTo(forceOptional);

    Type mapType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(), config);
    assertThat(mapType).isInstanceOf(MapType.class);
    assertThat(mapType.asMapType().keyType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().valueType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().isValueOptional()).isEqualTo(forceOptional);

    Type structType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).build(), config);
    assertThat(structType).isInstanceOf(StructType.class);
    assertThat(structType.asStructType().fieldType("i")).isInstanceOf(IntegerType.class);
    assertThat(structType.asStructType().field("i").isOptional()).isEqualTo(forceOptional);
  }

  @Test
  public void testInferIcebergType() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);

    assertThat(SchemaUtils.inferIcebergType(1, config).get()).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1L, config).get()).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1f, config).get()).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1d, config).get()).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType("foobar", config).get()).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(true, config).get()).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalDate.now(), config).get())
        .isInstanceOf(DateType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalTime.now(), config).get())
        .isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.inferIcebergType(new java.util.Date(), config).get();
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(OffsetDateTime.now(), config).get();
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(LocalDateTime.now(), config).get();
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isFalse();

    Type decimalType = SchemaUtils.inferIcebergType(new BigDecimal("12.345"), config).get();
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(3);

    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of("foobar"), config).get())
        .isInstanceOf(ListType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("foo", "bar"), config).get())
        .isInstanceOf(StructType.class);
  }

  @Test
  public void testInferIcebergTypeEmpty() {
    IcebergSinkConfig config = mock(IcebergSinkConfig.class);

    // skip infer for null
    assertThat(SchemaUtils.inferIcebergType(null, config)).isNotPresent();

    // skip infer for empty list
    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of(), config)).isNotPresent();
    // skip infer for list if first element is null
    List<?> list = Lists.newArrayList();
    list.add(null);
    assertThat(SchemaUtils.inferIcebergType(list, config)).isNotPresent();
    // skip infer for list if first element is an empty object
    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of(ImmutableMap.of()), config))
        .isNotPresent();

    // skip infer for empty object
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of(), config)).isNotPresent();
    // skip infer for object if values are null
    Map<String, ?> map = Maps.newHashMap();
    map.put("col", null);
    assertThat(SchemaUtils.inferIcebergType(map, config)).isNotPresent();
    // skip infer for object if values are empty objects
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("nested", ImmutableMap.of()), config))
        .isNotPresent();
  }
}
