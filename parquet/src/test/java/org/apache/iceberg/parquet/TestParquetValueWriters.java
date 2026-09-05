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
  void nullStructCountsNullsForNestedDoubleFields() {
    Types.StructType struct =
        Types.StructType.of(
            optional(2, "optional", Types.DoubleType.get()),
            required(3, "required", Types.DoubleType.get()));

    // a null struct contributes a null to the null count of every field it contains. The leaf
    // columns are written directly by the struct writer and never see the struct, so the struct
    // writer must add these nulls to the leaf metrics, whether the leaf is optional or required.
    Record inner = GenericRecord.create(struct);
    inner.setField("optional", 2.0D);
    inner.setField("required", 3.0D);

    Map<Integer, FieldMetrics<?>> metrics = writeNullStructs(struct, inner);

    // each field has one value from the present struct and two nulls from the null structs
    assertThat(metrics.get(2).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(2).valueCount()).isEqualTo(3);
    assertThat(metrics.get(3).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(3).valueCount()).isEqualTo(3);
  }

  @Test
  void nullStructCountsNullsForNestedFloatFields() {
    Types.StructType struct =
        Types.StructType.of(
            optional(2, "optional", Types.FloatType.get()),
            required(3, "required", Types.FloatType.get()));

    Record inner = GenericRecord.create(struct);
    inner.setField("optional", 2.0F);
    inner.setField("required", 3.0F);

    Map<Integer, FieldMetrics<?>> metrics = writeNullStructs(struct, inner);

    assertThat(metrics.get(2).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(2).valueCount()).isEqualTo(3);
    assertThat(metrics.get(3).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(3).valueCount()).isEqualTo(3);
  }

  @Test
  void nullStructCountsNullsForNestedGeometryFields() {
    Types.StructType struct =
        Types.StructType.of(
            optional(2, "optional", Types.GeometryType.crs84()),
            required(3, "required", Types.GeometryType.crs84()));

    Record inner = GenericRecord.create(struct);
    inner.setField("optional", ByteBuffer.allocate(21));
    inner.setField("required", ByteBuffer.allocate(21));

    Map<Integer, FieldMetrics<?>> metrics = writeNullStructs(struct, inner);

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
    present.setField("d", 2.0D);
    Record nullField = GenericRecord.create(struct);
    nullField.setField("d", null);

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
    innerRecord.setField("d", 2.0D);
    Record withInner = GenericRecord.create(outer);
    withInner.setField("inner", innerRecord);
    Record withoutInner = GenericRecord.create(outer);
    withoutInner.setField("inner", null);

    writer.write(0, record(schema, withInner));
    // a null at either level is a null for the leaf
    writer.write(0, record(schema, withoutInner));
    writer.write(0, record(schema, null));

    Map<Integer, FieldMetrics<?>> metrics = metricsById(writer);
    assertThat(metrics.get(3).nullValueCount()).isEqualTo(2);
    assertThat(metrics.get(3).valueCount()).isEqualTo(3);
  }

  /**
   * Writes an optional struct with the given populated value followed by two null structs, and
   * returns the resulting field metrics by id.
   */
  private static Map<Integer, FieldMetrics<?>> writeNullStructs(
      Types.StructType struct, Record populated) {
    Schema schema = new Schema(optional(1, "s", struct));
    ParquetValueWriter<Record> writer = writerFor(schema);

    writer.write(0, record(schema, populated));
    writer.write(0, record(schema, null));
    writer.write(0, record(schema, null));

    return metricsById(writer);
  }

  private static Record record(Schema schema, Record struct) {
    Record record = GenericRecord.create(schema);
    record.setField("s", struct);
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
