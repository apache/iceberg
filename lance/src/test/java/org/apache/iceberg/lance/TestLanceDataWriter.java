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
package org.apache.iceberg.lance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link LanceFileAppender} and {@link Lance.WriteBuilder}. */
public class TestLanceDataWriter {

  @TempDir Path tempDir;

  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "value", Types.DoubleType.get()));

  @Test
  public void testWriteSimpleRecords() throws IOException {
    File testFile = tempDir.resolve("test_write.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA)) {

      GenericRecord record1 = GenericRecord.create(SIMPLE_SCHEMA);
      record1.setField("id", 1);
      record1.setField("name", "Alice");
      record1.setField("value", 3.14);
      appender.add(record1);

      GenericRecord record2 = GenericRecord.create(SIMPLE_SCHEMA);
      record2.setField("id", 2);
      record2.setField("name", "Bob");
      record2.setField("value", 2.72);
      appender.add(record2);
    }

    // Verify file was created
    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);

    // Verify content contains header
    String content = new String(Files.readAllBytes(testFile.toPath()));
    assertThat(content).startsWith("LANCE_V1");
    assertThat(content).contains("records:2");
  }

  @Test
  public void testWriteViaBuilder() throws IOException {
    File testFile = tempDir.resolve("test_builder.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        Lance.write(outputFile).schema(SIMPLE_SCHEMA).build()) {

      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", 1);
      record.setField("name", "Charlie");
      record.setField("value", 1.41);
      appender.add(record);
    }

    assertThat(testFile).exists();
    assertThat(testFile.length()).isGreaterThan(0);
  }

  @Test
  public void testWriteMetrics() throws IOException {
    File testFile = tempDir.resolve("test_metrics.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    FileAppender<GenericRecord> appender = new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA);

    for (int i = 0; i < 10; i++) {
      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", i);
      record.setField("name", "name_" + i);
      record.setField("value", i * 1.5);
      appender.add(record);
    }

    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(10);
    assertThat(metrics.columnSizes()).isNotNull();
    assertThat(metrics.valueCounts()).isNotNull();
    assertThat(metrics.nullValueCounts()).isNotNull();
  }

  @Test
  public void testWriteWithNulls() throws IOException {
    File testFile = tempDir.resolve("test_nulls.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA)) {

      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", 1);
      record.setField("name", null);
      record.setField("value", null);
      appender.add(record);
    }

    assertThat(testFile).exists();
    String content = new String(Files.readAllBytes(testFile.toPath()));
    assertThat(content).contains("null");
  }

  @Test
  public void testWriteEmptyFile() throws IOException {
    File testFile = tempDir.resolve("test_empty.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    FileAppender<GenericRecord> appender = new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA);
    appender.close();

    Metrics metrics = appender.metrics();
    assertThat(metrics.recordCount()).isEqualTo(0);
    assertThat(testFile).exists();
  }

  @Test
  public void testBuilderRequiresSchema() {
    File testFile = tempDir.resolve("test_no_schema.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    assertThatThrownBy(() -> Lance.write(outputFile).build())
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Schema is required");
  }

  @Test
  public void testWriteFileLength() throws IOException {
    File testFile = tempDir.resolve("test_length.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    FileAppender<GenericRecord> appender = new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA);

    GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
    record.setField("id", 42);
    record.setField("name", "test");
    record.setField("value", 9.99);
    appender.add(record);
    appender.close();

    assertThat(appender.length()).isGreaterThan(0);
    assertThat(appender.length()).isEqualTo(testFile.length());
  }

  @Test
  public void testCannotAddAfterClose() throws IOException {
    File testFile = tempDir.resolve("test_closed.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    LanceFileAppender<GenericRecord> appender = new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA);
    appender.close();

    GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
    record.setField("id", 1);
    record.setField("name", "test");
    record.setField("value", 1.0);

    assertThatThrownBy(() -> appender.add(record))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("closed");
  }
}
