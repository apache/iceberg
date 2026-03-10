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

/** Unit tests for read projection (column pruning) with Lance format. */
public class TestLanceReadProjection {

  @TempDir Path tempDir;

  private static final Schema FULL_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "value", Types.DoubleType.get()),
          Types.NestedField.optional(4, "category", Types.StringType.get()));

  @Test
  public void testProjectSubsetOfColumns() throws IOException {
    File testFile = tempDir.resolve("test_projection.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    // Write with full schema
    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, FULL_SCHEMA)) {
      GenericRecord record = GenericRecord.create(FULL_SCHEMA);
      record.setField("id", 1);
      record.setField("name", "Alice");
      record.setField("value", 3.14);
      record.setField("category", "A");
      appender.add(record);

      GenericRecord record2 = GenericRecord.create(FULL_SCHEMA);
      record2.setField("id", 2);
      record2.setField("name", "Bob");
      record2.setField("value", 2.72);
      record2.setField("category", "B");
      appender.add(record2);
    }

    // Read with projected schema (only id and name)
    Schema projectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, FULL_SCHEMA, projectedSchema)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(2);

    // Projected records should only have id and name
    GenericRecord first = results.get(0);
    assertThat(first.getField("id")).isEqualTo(1);
    assertThat(first.getField("name")).isEqualTo("Alice");

    GenericRecord second = results.get(1);
    assertThat(second.getField("id")).isEqualTo(2);
    assertThat(second.getField("name")).isEqualTo("Bob");
  }

  @Test
  public void testProjectSingleColumn() throws IOException {
    File testFile = tempDir.resolve("test_single_col.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, FULL_SCHEMA)) {
      for (int i = 0; i < 5; i++) {
        GenericRecord record = GenericRecord.create(FULL_SCHEMA);
        record.setField("id", i);
        record.setField("name", "name_" + i);
        record.setField("value", i * 1.0);
        record.setField("category", "cat_" + i);
        appender.add(record);
      }
    }

    // Project only the "value" column
    Schema projectedSchema =
        new Schema(Types.NestedField.optional(3, "value", Types.DoubleType.get()));

    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, FULL_SCHEMA, projectedSchema)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(5);
    // All records should have the value field
    for (int i = 0; i < 5; i++) {
      assertThat(results.get(i).getField("value")).isEqualTo(i * 1.0);
    }
  }

  @Test
  public void testProjectFullSchema() throws IOException {
    File testFile = tempDir.resolve("test_full_proj.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        new LanceFileAppender<>(outputFile, FULL_SCHEMA)) {
      GenericRecord record = GenericRecord.create(FULL_SCHEMA);
      record.setField("id", 99);
      record.setField("name", "test");
      record.setField("value", 42.0);
      record.setField("category", "X");
      appender.add(record);
    }

    // Project full schema (same as file schema)
    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        new LanceIterable<>(inputFile, FULL_SCHEMA, FULL_SCHEMA)) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getField("id")).isEqualTo(99);
    assertThat(results.get(0).getField("name")).isEqualTo("test");
    assertThat(results.get(0).getField("value")).isEqualTo(42.0);
    assertThat(results.get(0).getField("category")).isEqualTo("X");
  }

  @Test
  public void testProjectViaReadBuilder() throws IOException {
    File testFile = tempDir.resolve("test_proj_builder.lance").toFile();
    OutputFile outputFile = org.apache.iceberg.Files.localOutput(testFile);

    try (FileAppender<GenericRecord> appender =
        Lance.write(outputFile).schema(FULL_SCHEMA).build()) {
      GenericRecord record = GenericRecord.create(FULL_SCHEMA);
      record.setField("id", 1);
      record.setField("name", "hello");
      record.setField("value", 9.99);
      record.setField("category", "Z");
      appender.add(record);
    }

    Schema projectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(4, "category", Types.StringType.get()));

    InputFile inputFile = org.apache.iceberg.Files.localInput(testFile);
    List<GenericRecord> results = new ArrayList<>();
    try (CloseableIterable<GenericRecord> iterable =
        Lance.read(inputFile).schema(FULL_SCHEMA).project(projectedSchema).build()) {
      iterable.forEach(results::add);
    }

    assertThat(results).hasSize(1);
    assertThat(results.get(0).getField("id")).isEqualTo(1);
    assertThat(results.get(0).getField("category")).isEqualTo("Z");
  }
}
