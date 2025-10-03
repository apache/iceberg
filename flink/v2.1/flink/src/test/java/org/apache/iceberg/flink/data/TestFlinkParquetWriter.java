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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.RowDataConverter;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkParquetWriter extends DataTestBase {
  private static final int NUM_RECORDS = 100;

  @TempDir private Path temp;

  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  private void writeAndValidate(Iterable<RowData> iterable, Schema schema) throws IOException {
    OutputFile outputFile = new InMemoryOutputFile();

    LogicalType logicalType = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
            .build()) {
      writer.addAll(iterable);
    }

    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(schema)
            .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
            .build()) {
      Iterator<RowData> expected = iterable.iterator();
      Iterator<Record> actual = reader.iterator();
      LogicalType rowType = FlinkSchemaUtil.convert(schema);
      for (int i = 0; i < NUM_RECORDS; i += 1) {
        assertThat(actual).hasNext();
        TestHelpers.assertRowData(schema.asStruct(), rowType, actual.next(), expected.next());
      }
      assertThat(actual).isExhausted();
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(RandomRowData.generate(schema, NUM_RECORDS, 19981), schema);

    writeAndValidate(
        RandomRowData.convert(
            schema,
            RandomGenericData.generateDictionaryEncodableRecords(schema, NUM_RECORDS, 21124)),
        schema);

    writeAndValidate(
        RandomRowData.convert(
            schema,
            RandomGenericData.generateFallbackRecords(
                schema, NUM_RECORDS, 21124, NUM_RECORDS / 20)),
        schema);
  }

  @Override
  protected void writeAndValidate(Schema schema, List<Record> expectedData) throws IOException {
    RowDataSerializer rowDataSerializer = new RowDataSerializer(FlinkSchemaUtil.convert(schema));
    List<RowData> binaryRowList = Lists.newArrayList();
    for (Record record : expectedData) {
      RowData rowData = RowDataConverter.convert(schema, record);
      BinaryRowData binaryRow = rowDataSerializer.toBinaryRow(rowData);
      binaryRowList.add(binaryRow);
    }

    writeAndValidate(binaryRowList, schema);
  }

  /**
   * Test that nanosecond precision timestamps are preserved when writing to Parquet files. This
   */
  @Test
  public void testNanosecondTimestampPrecision() throws IOException {
    // Create a schema with nanosecond timestamp
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "timestamp_ns", Types.TimestampNanoType.withoutZone()),
            Types.NestedField.required(2, "timestamp_ns_tz", Types.TimestampNanoType.withZone()));

    List<RowData> testData = Lists.newArrayList(RandomRowData.generate(schema, 1, 42L));

    // Write to Parquet file
    OutputFile outputFile = new InMemoryOutputFile();
    LogicalType logicalType = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
            .build()) {
      writer.addAll(testData);
    }

    // Read back from Parquet file and verify nanosecond precision
    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(schema)
            .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
            .build()) {
      Iterator<Record> records = reader.iterator();
      assertThat(records).hasNext();

      Record record = records.next();
      Object timestampValue = record.get(0);
      Object timestampTzValue = record.get(1);

      // Verify that nanosecond precision is preserved
      // The first value should be LocalDateTime, the second should be OffsetDateTime
      // (timezone-aware)
      assertThat(timestampValue).isInstanceOf(LocalDateTime.class);
      assertThat(timestampTzValue).isInstanceOf(java.time.OffsetDateTime.class);

      LocalDateTime timestamp = (LocalDateTime) timestampValue;
      java.time.OffsetDateTime timestampTz = (java.time.OffsetDateTime) timestampTzValue;

      // Verify that nanosecond precision is preserved (nano field should have meaningful values)
      assertThat(timestamp.getNano()).isGreaterThan(0);
      assertThat(timestampTz.getNano()).isGreaterThan(0);
    }
  }

  /** Test that microsecond precision timestamps work correctly (regression test). */
  @Test
  public void testMicrosecondTimestampPrecision() throws IOException {
    // Create a schema with microsecond timestamp
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "timestamp_micros", Types.TimestampType.withoutZone()));

    List<RowData> testData = Lists.newArrayList(RandomRowData.generate(schema, 1, 42L));

    // Write to Parquet file
    OutputFile outputFile = new InMemoryOutputFile();
    LogicalType logicalType = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer =
        Parquet.write(outputFile)
            .schema(schema)
            .createWriterFunc(msgType -> FlinkParquetWriters.buildWriter(logicalType, msgType))
            .build()) {
      writer.addAll(testData);
    }

    // Read back and verify microsecond precision
    try (CloseableIterable<Record> reader =
        Parquet.read(outputFile.toInputFile())
            .project(schema)
            .createReaderFunc(msgType -> GenericParquetReaders.buildReader(schema, msgType))
            .build()) {
      Iterator<Record> records = reader.iterator();
      assertThat(records).hasNext();

      Record record = records.next();
      Object timestampValue = record.get(0);

      // Verify that microsecond precision is preserved
      assertThat(timestampValue).isInstanceOf(LocalDateTime.class);
      LocalDateTime timestamp = (LocalDateTime) timestampValue;

      // For microsecond precision, the nanosecond component should be a multiple of 1000
      assertThat(timestamp.getNano() % 1000).isEqualTo(0);
    }
  }
}
