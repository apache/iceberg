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
package org.apache.iceberg.spark.source;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.FieldMetrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.TripleWriter;
import org.apache.iceberg.parquet.WriterLazyInitializable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.data.ParquetWithSparkSchemaVisitor;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.parquet.column.ColumnWriteStore;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.ColumnChunkPageWriteStore;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

/**
 * A Parquet output writer that performs variant shredding with schema inference. This is similar to
 * Spark's ParquetOutputWriterWithVariantShredding but adapted for Iceberg.
 *
 * <p>The writer works in two phases: 1. Schema inference phase: Buffers initial rows and analyzes
 * variant data to infer schemas 2. Writing phase: Creates the actual Parquet writer with inferred
 * schemas and writes all data
 */
public class SparkParquetWriterWithVariantShredding
    implements ParquetValueWriter<InternalRow>, WriterLazyInitializable {
  private final StructType sparkSchema;
  private final MessageType parquetType;

  private final List<BufferedRow> bufferedRows;
  private ParquetValueWriter<InternalRow> actualWriter;
  private boolean writerInitialized = false;
  private final int bufferSize;

  private static class BufferedRow {
    private final int repetitionLevel;
    private final InternalRow row;

    BufferedRow(int repetitionLevel, InternalRow row) {
      this.repetitionLevel = repetitionLevel;
      this.row = row;
    }
  }

  public SparkParquetWriterWithVariantShredding(
      StructType sparkSchema, MessageType parquetType, Map<String, String> properties) {
    this.sparkSchema = sparkSchema;
    this.parquetType = parquetType;

    this.bufferSize =
        Integer.parseInt(
            properties.getOrDefault(
                SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE,
                String.valueOf(SparkSQLProperties.VARIANT_INFERENCE_BUFFER_SIZE_DEFAULT)));
    this.bufferedRows = Lists.newArrayList();
  }

  @Override
  public void write(int repetitionLevel, InternalRow row) {
    if (!writerInitialized) {
      bufferedRows.add(
          new BufferedRow(
              repetitionLevel, row.copy())); /* Make a copy of the object since row gets reused */

      if (bufferedRows.size() >= bufferSize) {
        writerInitialized = true;
      }
    } else {
      actualWriter.write(repetitionLevel, row);
    }
  }

  @Override
  public List<TripleWriter<?>> columns() {
    if (actualWriter != null) {
      return actualWriter.columns();
    }
    return Collections.emptyList();
  }

  @Override
  public void setColumnStore(ColumnWriteStore columnStore) {
    // Ignored for lazy initialization - will be set on actualWriter after initialization
  }

  @Override
  public Stream<FieldMetrics<?>> metrics() {
    if (actualWriter != null) {
      return actualWriter.metrics();
    }
    return Stream.empty();
  }

  @Override
  public boolean needsInitialization() {
    return !writerInitialized;
  }

  @Override
  public InitializationResult initialize(
      ParquetProperties props,
      CompressionCodecFactory.BytesInputCompressor compressor,
      int rowGroupOrdinal) {
    if (bufferedRows.isEmpty()) {
      throw new IllegalStateException("No buffered rows available for schema inference");
    }

    List<InternalRow> rows = Lists.newLinkedList();
    for (BufferedRow bufferedRow : bufferedRows) {
      rows.add(bufferedRow.row);
    }

    MessageType shreddedSchema =
        (MessageType)
            ParquetWithSparkSchemaVisitor.visit(
                sparkSchema, parquetType, new SchemaInferenceVisitor(rows, sparkSchema));

    actualWriter = SparkParquetWriters.buildWriter(sparkSchema, shreddedSchema);

    ColumnChunkPageWriteStore pageStore =
        new ColumnChunkPageWriteStore(
            compressor,
            shreddedSchema,
            props.getAllocator(),
            64,
            ParquetProperties.DEFAULT_PAGE_WRITE_CHECKSUM_ENABLED,
            null,
            rowGroupOrdinal);

    ColumnWriteStore columnStore = props.newColumnWriteStore(shreddedSchema, pageStore, pageStore);

    actualWriter.setColumnStore(columnStore);

    for (BufferedRow bufferedRow : bufferedRows) {
      actualWriter.write(bufferedRow.repetitionLevel, bufferedRow.row);
      columnStore.endRecord();
    }

    bufferedRows.clear();
    writerInitialized = true;

    return new InitializationResult(shreddedSchema, pageStore, columnStore);
  }

  public static boolean shouldUseVariantShredding(Map<String, String> properties, Schema schema) {
    boolean shreddingEnabled =
        properties.containsKey(SparkSQLProperties.SHRED_VARIANTS)
            && Boolean.parseBoolean(properties.get(SparkSQLProperties.SHRED_VARIANTS));

    boolean hasVariantFields =
        schema.columns().stream().anyMatch(field -> field.type() instanceof Types.VariantType);

    return shreddingEnabled && hasVariantFields;
  }
}
