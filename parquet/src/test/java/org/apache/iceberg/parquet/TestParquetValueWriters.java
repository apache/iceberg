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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class TestParquetValueWriters {

  @Test
  void geospatialValueSizeMetricsExcludeNulls() {
    Schema schema = new Schema(optional(2, "geom", Types.GeometryType.crs84()));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    Type parquetType = parquetSchema.getType("geom");
    ColumnDescriptor desc = parquetSchema.getColumnDescription(new String[] {"geom"});
    ParquetValueWriter<ByteBuffer> writer =
        ParquetValueWriters.option(
            parquetType,
            parquetSchema.getMaxDefinitionLevel(new String[] {"geom"}),
            ParquetValueWriters.geospatial(desc));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(desc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);
    writer.write(0, ByteBuffer.allocate(21));
    writer.write(0, ByteBuffer.allocate(42));
    writer.write(0, null);

    FieldMetrics<?> metrics = writer.metrics().findFirst().orElseThrow();
    assertThat(metrics.valueCount()).isEqualTo(3);
    assertThat(metrics.nullValueCount()).isEqualTo(1);
    assertThat(metrics.avgValueSizeInBytes()).isEqualTo(31);
  }

  @Test
  void nullStructCountsNullsForNestedFields() {
    // a null struct is also null for the fields it contains, but those columns are written by the
    // struct's writer and never see the value, so the struct must count the nulls for them
    Types.StructType struct =
        Types.StructType.of(
            optional(2, "d", Types.DoubleType.get()), required(3, "f", Types.FloatType.get()));
    Schema schema = new Schema(optional(1, "s", struct));

    ParquetValueWriter<Record> writer = writerFor(schema);
    Record inner = GenericRecord.create(struct);
    inner.set(0, 2.0D);
    inner.set(1, 1.0F);

    writer.write(0, record(schema, inner));
    writer.write(0, record(schema, null));
    writer.write(0, record(schema, null));

    Map<Integer, FieldMetrics<?>> metrics = metricsById(writer);
    // both fields have one non-null value and two nulls from the null structs
    assertThat(metrics.get(2).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(2).valueCount()).isEqualTo(3);
    assertThat(metrics.get(3).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(3).valueCount()).isEqualTo(3);
  }

  @Test
  void nullStructAddsToNullsCountedByNestedField() {
    Types.StructType struct = Types.StructType.of(optional(2, "d", Types.DoubleType.get()));
    Schema schema = new Schema(optional(1, "s", struct));

    ParquetValueWriter<Record> writer = writerFor(schema);
    Record present = GenericRecord.create(struct);
    present.set(0, 2.0D);
    Record nullField = GenericRecord.create(struct);
    nullField.set(0, null);

    writer.write(0, record(schema, present));
    // the field is null while the struct is present, so the field's own writer counts it
    writer.write(0, record(schema, nullField));
    writer.write(0, record(schema, null));

    Map<Integer, FieldMetrics<?>> metrics = metricsById(writer);
    assertThat(metrics.get(2).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(2).valueCount()).isEqualTo(3);
  }

  @Test
  void nullStructCountsNullsForDeeplyNestedFields() {
    Types.StructType inner = Types.StructType.of(optional(3, "d", Types.DoubleType.get()));
    Types.StructType outer = Types.StructType.of(optional(2, "inner", inner));
    Schema schema = new Schema(optional(1, "s", outer));

    ParquetValueWriter<Record> writer = writerFor(schema);
    Record innerRecord = GenericRecord.create(inner);
    innerRecord.set(0, 2.0D);
    Record withInner = GenericRecord.create(outer);
    withInner.set(0, innerRecord);
    Record withoutInner = GenericRecord.create(outer);
    withoutInner.set(0, null);

    writer.write(0, record(schema, withInner));
    // a null at either level is a null for the leaf
    writer.write(0, record(schema, withoutInner));
    writer.write(0, record(schema, null));

    Map<Integer, FieldMetrics<?>> metrics = metricsById(writer);
    assertThat(metrics.get(3).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(3).valueCount()).isEqualTo(3);
  }

  @Test
  void nullStructCountsNullsForNestedGeospatialField() {
    // geospatial writers also report metrics, so they are affected in the same way
    Types.StructType struct = Types.StructType.of(optional(2, "g", Types.GeometryType.crs84()));
    Schema schema = new Schema(optional(1, "s", struct));

    ParquetValueWriter<Record> writer = writerFor(schema);
    Record present = GenericRecord.create(struct);
    present.set(0, ByteBuffer.allocate(21));

    writer.write(0, record(schema, present));
    writer.write(0, record(schema, null));

    Map<Integer, FieldMetrics<?>> metrics = metricsById(writer);
    assertThat(metrics.get(2).nullValueCount()).isEqualTo(1);
    assertThat(metrics.get(2).valueCount()).isEqualTo(2);
    // the size of the one non-null value is still reported
    assertThat(metrics.get(2).avgValueSizeInBytes()).isEqualTo(21);
  }

  private static Record record(Schema schema, Record struct) {
    Record record = GenericRecord.create(schema);
    record.set(0, struct);
    return record;
  }

  /** Returns a writer for the given schema, with a mocked column store. */
  private static ParquetValueWriter<Record> writerFor(Schema schema) {
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    ParquetValueWriter<Record> writer = InternalWriter.createWriter(schema, parquetSchema);

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(any())).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);

    return writer;
  }

  private static Map<Integer, FieldMetrics<?>> metricsById(ParquetValueWriter<?> writer) {
    return writer.metrics().collect(Collectors.toMap(FieldMetrics::id, Function.identity()));
  }
}
