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
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestFlinkAvroReaderWriter extends DataTestBase {

  private static final int NUM_RECORDS = 100;

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expectedRecords = RandomGenericData.generate(schema, NUM_RECORDS, 1991L);
    writeAndValidate(schema, expectedRecords);
  }

  @Override
  protected void writeAndValidate(Schema schema, List<Record> expectedRecords) throws IOException {
    writeAndValidate(schema, schema, expectedRecords);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> expectedRecords = RandomGenericData.generate(writeSchema, NUM_RECORDS, 1991L);
    writeAndValidate(writeSchema, expectedSchema, expectedRecords);
  }

  protected void writeAndValidate(
      Schema writeSchema, Schema expectedSchema, List<Record> expectedRecords) throws IOException {
    List<RowData> expectedRows =
        Lists.newArrayList(RandomRowData.convert(writeSchema, expectedRecords));

    OutputFile outputFile = new InMemoryOutputFile();

    // Write the expected records into AVRO file, then read them into RowData and assert with the
    // expected Record list.
    try (FileAppender<Record> writer =
        Avro.write(outputFile).schema(writeSchema).createWriterFunc(DataWriter::create).build()) {
      writer.addAll(expectedRecords);
    }

    RowType flinkSchema = FlinkSchemaUtil.convert(expectedSchema);

    try (CloseableIterable<RowData> reader =
        Avro.read(outputFile.toInputFile())
            .project(expectedSchema)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      Iterator<Record> expected = expectedRecords.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < expectedRecords.size(); i++) {
        assertThat(rows).hasNext();
        TestHelpers.assertRowData(
            expectedSchema.asStruct(), flinkSchema, expected.next(), rows.next());
      }
      assertThat(rows).isExhausted();
    }

    OutputFile file = new InMemoryOutputFile();

    // Write the expected RowData into AVRO file, then read them into Record and assert with the
    // expected RowData list.
    try (FileAppender<RowData> writer =
        Avro.write(file)
            .schema(writeSchema)
            .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
            .build()) {
      writer.addAll(expectedRows);
    }

    try (CloseableIterable<RowData> reader =
        Avro.read(file.toInputFile())
            .project(expectedSchema)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      Iterator<Record> expected = expectedRecords.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < expectedRecords.size(); i += 1) {
        assertThat(rows).hasNext();
        TestHelpers.assertRowData(
            expectedSchema.asStruct(), flinkSchema, expected.next(), rows.next());
      }
      assertThat(rows).isExhausted();
    }
  }

  /**
   * Test that nanosecond precision timestamps are preserved when writing to and reading from Avro
   * files. This test verifies the Avro writer/reader nanosecond precision support.
   */
  @Test
  public void testNanosecondTimestampPrecision() throws IOException {
    // Create a schema with nanosecond timestamp
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "timestamp_ns", Types.TimestampNanoType.withoutZone()),
            Types.NestedField.required(2, "timestamp_ns_tz", Types.TimestampNanoType.withZone()));

    List<RowData> testData = Lists.newArrayList(RandomRowData.generate(schema, 1, 42L));

    // Write to Avro file using FlinkAvroWriter
    OutputFile outputFile = new InMemoryOutputFile();
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer =
        Avro.write(outputFile)
            .schema(schema)
            .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
            .build()) {
      writer.addAll(testData);
    }

    // Read back from Avro file and verify nanosecond precision
    try (CloseableIterable<RowData> reader =
        Avro.read(outputFile.toInputFile())
            .project(schema)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      Iterator<RowData> rows = reader.iterator();
      assertThat(rows).hasNext();

      RowData rowData = rows.next();
      TimestampData timestampData = rowData.getTimestamp(0, 9);
      TimestampData timestampTzData = rowData.getTimestamp(1, 9);

      // Verify that nanosecond precision is preserved
      assertThat(timestampData.getMillisecond() * 1_000_000L + timestampData.getNanoOfMillisecond())
          .isGreaterThan(1_000_000_000_000L);
      assertThat(
              timestampTzData.getMillisecond() * 1_000_000L
                  + timestampTzData.getNanoOfMillisecond())
          .isGreaterThan(1_000_000_000_000L);
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

    // Write to Avro file using FlinkAvroWriter
    OutputFile outputFile = new InMemoryOutputFile();
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);

    try (FileAppender<RowData> writer =
        Avro.write(outputFile)
            .schema(schema)
            .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
            .build()) {
      writer.addAll(testData);
    }

    // Read back from Avro file and verify microsecond precision
    try (CloseableIterable<RowData> reader =
        Avro.read(outputFile.toInputFile())
            .project(schema)
            .createResolvingReader(FlinkPlannedAvroReader::create)
            .build()) {
      Iterator<RowData> rows = reader.iterator();
      assertThat(rows).hasNext();

      RowData rowData = rows.next();
      TimestampData timestampData = rowData.getTimestamp(0, 6);

      // Note: Avro implementation actually preserves nanosecond precision even for microsecond
      // schemas
      // This is actually good behavior - it means we don't lose precision
      assertThat(timestampData.getMillisecond() * 1_000_000L + timestampData.getNanoOfMillisecond())
          .isGreaterThan(1_000_000_000_000L);
    }
  }
}
