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
package io.tabular.iceberg.connect.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.tabular.iceberg.connect.data.SchemaUpdate.AddColumn;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
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

public class SchemaUtilsTest {

  private static final org.apache.iceberg.Schema SIMPLE_SCHEMA =
      new org.apache.iceberg.Schema(Types.NestedField.required(1, "i", Types.IntegerType.get()));

  @Test
  public void testApplySchemaUpdates() {
    UpdateSchema updateSchema = mock(UpdateSchema.class);
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);
    when(table.updateSchema()).thenReturn(updateSchema);

    List<AddColumn> updates =
        ImmutableList.of(
            new AddColumn(null, "i", Types.IntegerType.get()),
            new AddColumn(null, "s", Types.StringType.get()));

    SchemaUtils.applySchemaUpdates(table, updates);
    verify(table).refresh();
    verify(table).updateSchema();
    verify(updateSchema).addColumn(any(), any(String.class), any(Type.class));
    verify(updateSchema).commit();
  }

  @Test
  public void testApplySchemaUpdatesNoUpdates() {
    Table table = mock(Table.class);
    when(table.schema()).thenReturn(SIMPLE_SCHEMA);

    SchemaUtils.applySchemaUpdates(table, null);
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();

    SchemaUtils.applySchemaUpdates(table, ImmutableList.of());
    verify(table, times(0)).refresh();
    verify(table, times(0)).updateSchema();
  }

  @Test
  public void testToIcebergType() {
    assertThat(SchemaUtils.toIcebergType(Schema.BOOLEAN_SCHEMA)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.BYTES_SCHEMA)).isInstanceOf(BinaryType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT8_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT16_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT32_SCHEMA)).isInstanceOf(IntegerType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.INT64_SCHEMA)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT32_SCHEMA)).isInstanceOf(FloatType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.FLOAT64_SCHEMA)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.toIcebergType(Schema.STRING_SCHEMA)).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.toIcebergType(Date.SCHEMA)).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.toIcebergType(Time.SCHEMA)).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.toIcebergType(Timestamp.SCHEMA);
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    Type decimalType = SchemaUtils.toIcebergType(Decimal.schema(4));
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(4);

    Type listType = SchemaUtils.toIcebergType(SchemaBuilder.array(Schema.STRING_SCHEMA).build());
    assertThat(listType).isInstanceOf(ListType.class);
    assertThat(listType.asListType().elementType()).isInstanceOf(StringType.class);

    Type mapType =
        SchemaUtils.toIcebergType(
            SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build());
    assertThat(mapType).isInstanceOf(MapType.class);
    assertThat(mapType.asMapType().keyType()).isInstanceOf(StringType.class);
    assertThat(mapType.asMapType().valueType()).isInstanceOf(StringType.class);

    Type structType =
        SchemaUtils.toIcebergType(SchemaBuilder.struct().field("i", Schema.INT32_SCHEMA).build());
    assertThat(structType).isInstanceOf(StructType.class);
    assertThat(structType.asStructType().fieldType("i")).isInstanceOf(IntegerType.class);
  }

  @Test
  public void testInferIcebergType() {
    assertThatThrownBy(() -> SchemaUtils.inferIcebergType(null))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot infer type from null value");

    assertThat(SchemaUtils.inferIcebergType(1)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1L)).isInstanceOf(LongType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1f)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType(1.1d)).isInstanceOf(DoubleType.class);
    assertThat(SchemaUtils.inferIcebergType("foobar")).isInstanceOf(StringType.class);
    assertThat(SchemaUtils.inferIcebergType(true)).isInstanceOf(BooleanType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalDate.now())).isInstanceOf(DateType.class);
    assertThat(SchemaUtils.inferIcebergType(LocalTime.now())).isInstanceOf(TimeType.class);

    Type timestampType = SchemaUtils.inferIcebergType(new java.util.Date());
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(OffsetDateTime.now());
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isTrue();

    timestampType = SchemaUtils.inferIcebergType(LocalDateTime.now());
    assertThat(timestampType).isInstanceOf(TimestampType.class);
    assertThat(((TimestampType) timestampType).shouldAdjustToUTC()).isFalse();

    Type decimalType = SchemaUtils.inferIcebergType(new BigDecimal("12.345"));
    assertThat(decimalType).isInstanceOf(DecimalType.class);
    assertThat(((DecimalType) decimalType).scale()).isEqualTo(3);

    assertThat(SchemaUtils.inferIcebergType(ImmutableList.of("foobar")))
        .isInstanceOf(ListType.class);
    assertThat(SchemaUtils.inferIcebergType(ImmutableMap.of("foo", "bar")))
        .isInstanceOf(StructType.class);
  }
}
