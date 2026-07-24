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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
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
}
