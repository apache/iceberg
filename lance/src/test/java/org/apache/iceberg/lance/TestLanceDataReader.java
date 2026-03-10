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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Unit tests for {@link LanceIterable} and {@link Lance.ReadBuilder}. */
public class TestLanceDataReader {

  @TempDir Path tempDir;

  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "value", Types.DoubleType.get()));

  /** Helper to write test data and return the file. */
  private File writeTestData(String filename, int numRecords) throws IOException {
    File testFile = tempDir.resolve(filename).toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA)) {
      for (int i = 0; i < numRecords; i++) {
        GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
        record.setField("id", i + 1);
        record.setField("name", "name_" + (i + 1));
        record.setField("value", (i + 1) * 1.5);
        appender.add(record);
      }
    }

    return testFile;
  }

  @Test
  public void testReadSimpleRecords() throws IOException {
    File testFile = writeTestData("test_read.lance", 5);
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);

    List<GenericRecord> records = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, SIMPLE_SCHEMA, SIMPLE_SCHEMA)) {
      iterable.forEach(records::add);
    }

    assertThat(records).hasSize(5);
    assertThat(records.get(0).getField("id")).isEqualTo(1);
    assertThat(records.get(0).getField("name")).isEqualTo("name_1");
    assertThat(records.get(4).getField("id")).isEqualTo(5);
  }

  @Test
  public void testReadViaBuilder() throws IOException {
    File testFile = writeTestData("test_read_builder.lance", 3);
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);

    List<GenericRecord> records = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        Lance.read(inputFile).schema(SIMPLE_SCHEMA).build()) {
      iterable.forEach(records::add);
    }

    assertThat(records).hasSize(3);
    assertThat(records.get(0).getField("name")).isEqualTo("name_1");
    assertThat(records.get(2).getField("name")).isEqualTo("name_3");
  }

  @Test
  public void testReadWriteRoundTrip() throws IOException {
    File testFile = tempDir.resolve("test_roundtrip.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    // Write
    GenericRecord expected1 = GenericRecord.create(SIMPLE_SCHEMA);
    expected1.setField("id", 42);
    expected1.setField("name", "hello");
    expected1.setField("value", 3.14);

    GenericRecord expected2 = GenericRecord.create(SIMPLE_SCHEMA);
    expected2.setField("id", 100);
    expected2.setField("name", "world");
    expected2.setField("value", 2.72);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA)) {
      appender.add(expected1);
      appender.add(expected2);
    }

    // Read
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, SIMPLE_SCHEMA, SIMPLE_SCHEMA)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(2);
    assertThat(results.get(0).getField("id")).isEqualTo(42);
    assertThat(results.get(0).getField("name")).isEqualTo("hello");
    assertThat(results.get(1).getField("id")).isEqualTo(100);
    assertThat(results.get(1).getField("name")).isEqualTo("world");
  }

  @Test
  public void testReadWithNulls() throws IOException {
    File testFile = tempDir.resolve("test_read_nulls.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, SIMPLE_SCHEMA)) {
      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", 1);
      record.setField("name", null);
      record.setField("value", null);
      appender.add(record);
    }

    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, SIMPLE_SCHEMA, SIMPLE_SCHEMA)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getField("id")).isEqualTo(1);
    assertThat(results.get(0).getField("name")).isNull();
    assertThat(results.get(0).getField("value")).isNull();
  }

  @Test
  public void testReadEmptyFile() throws IOException {
    File testFile = writeTestData("test_read_empty.lance", 0);
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);

    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, SIMPLE_SCHEMA, SIMPLE_SCHEMA)) {
      iterable.forEach(results::add);
    }

    assertThat(results).isEmpty();
  }

  @Test
  public void testReadLargeDataset() throws IOException {
    int numRecords = 1000;
    File testFile = writeTestData("test_read_large.lance", numRecords);
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);

    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, SIMPLE_SCHEMA, SIMPLE_SCHEMA)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(numRecords);
    assertThat(results.get(0).getField("id")).isEqualTo(1);
    assertThat(results.get(numRecords - 1).getField("id")).isEqualTo(numRecords);
  }
}
