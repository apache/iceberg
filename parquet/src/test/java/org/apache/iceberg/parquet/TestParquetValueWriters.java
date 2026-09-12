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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ColumnWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

class TestParquetValueWriters {

  @Test
  void customMetricsIncludeNullParents() {
    Types.StructType statsType =
        Types.StructType.of(
            required(2, "float_value", Types.FloatType.get()),
            required(3, "double_value", Types.DoubleType.get()),
            required(4, "geometry_value", Types.GeometryType.crs84()),
            required(5, "geography_value", Types.GeographyType.crs84()));
    Schema schema = new Schema(optional(1, "stats", statsType));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");

    ColumnDescriptor floatDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "float_value"});
    ColumnDescriptor doubleDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "double_value"});
    ColumnDescriptor geometryDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "geometry_value"});
    ColumnDescriptor geographyDesc =
        parquetSchema.getColumnDescription(new String[] {"stats", "geography_value"});

    List<ParquetValueWriter<?>> fieldWriters =
        ImmutableList.of(
            ParquetValueWriters.floats(floatDesc),
            ParquetValueWriters.doubles(doubleDesc),
            ParquetValueWriters.geospatial(geometryDesc),
            ParquetValueWriters.geospatial(geographyDesc));
    ParquetValueWriter<StructLike> writer =
        ParquetValueWriters.option(
            parquetSchema.getType("stats"),
            parquetSchema.getMaxDefinitionLevel(new String[] {"stats"}),
            ParquetValueWriters.recordWriter(statsType, fieldWriters));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(floatDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(doubleDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(geometryDesc)).thenReturn(mock(ColumnWriter.class));
    when(columnStore.getColumnWriter(geographyDesc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);

    GenericRecord stats = GenericRecord.create(statsType);
    stats.setField("float_value", 12.0F);
    stats.setField("double_value", 34.0D);
    stats.setField("geometry_value", ByteBuffer.allocate(21));
    stats.setField("geography_value", ByteBuffer.allocate(42));
    writer.write(0, stats);
    writer.write(0, null);

    Map<Integer, FieldMetrics<?>> metricsById =
        writer.metrics().collect(Collectors.toMap(FieldMetrics::id, Function.identity()));
    assertThat(metricsById).containsOnlyKeys(2, 3, 4, 5);
    assertThat(metricsById.values())
        .allSatisfy(
            metrics -> {
              assertThat(metrics.valueCount()).isEqualTo(2);
              assertThat(metrics.nullValueCount()).isEqualTo(1);
            });
    assertThat(metricsById.get(4).avgValueSizeInBytes()).isEqualTo(21);
    assertThat(metricsById.get(5).avgValueSizeInBytes()).isEqualTo(42);
  }

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
  void geometryValueSizeMetricsExcludeNulls() {
    Schema schema = new Schema(optional(2, "geom", Types.GeometryType.crs84()));
    MessageType parquetSchema = ParquetSchemaUtil.convert(schema, "table");
    Type parquetType = parquetSchema.getType("geom");
    ColumnDescriptor desc = parquetSchema.getColumnDescription(new String[] {"geom"});
    ParquetValueWriter<ByteBuffer> writer =
        ParquetValueWriters.option(
            parquetType,
            parquetSchema.getMaxDefinitionLevel(new String[] {"geom"}),
            ParquetValueWriters.geometry(desc, Types.GeometryType.crs84()));

    ColumnWriteStore columnStore = mock(ColumnWriteStore.class);
    when(columnStore.getColumnWriter(desc)).thenReturn(mock(ColumnWriter.class));
    writer.setColumnStore(columnStore);
    writer.write(0, ByteBuffer.allocate(21));
    writer.write(0, ByteBuffer.allocate(42));
    writer.write(0, null);

    // the geometry writer adds bounds but must keep the same average WKB size metric as the
    // counts-only geospatial writer: the average is over the two non-null values, (21 + 42) / 2
    FieldMetrics<?> metrics = writer.metrics().findFirst().orElseThrow();
    assertThat(metrics.valueCount()).isEqualTo(3);
    assertThat(metrics.nullValueCount()).isEqualTo(1);
    assertThat(metrics.avgValueSizeInBytes()).isEqualTo(31);
  }
}
