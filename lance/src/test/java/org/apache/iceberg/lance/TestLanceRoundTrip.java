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
import java.util.List;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.formats.ModelWriteBuilder;
import org.apache.iceberg.formats.ReadBuilder;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Integration tests for Lance format round-trip read/write via LanceFormatModel.
 *
 * <p>Requires the Lance JNI native library to be available on the classpath (built from
 * lance/java/lance-jni).
 */
class TestLanceRoundTrip {

  @TempDir Path tempDir;

  private static final Schema SIMPLE_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.required(3, "age", Types.IntegerType.get()),
          Types.NestedField.optional(4, "score", Types.DoubleType.get()));

  private LanceFormatModel<Record, Void> createModel() {
    return LanceFormatModel.create(
        Record.class,
        Void.class,
        (icebergSchema, arrowSchema, engineSchema) ->
            GenericLanceWriter.create(icebergSchema, arrowSchema),
        (icebergSchema, arrowSchema, engineSchema, idToConstant) ->
            GenericLanceReader.buildReader(icebergSchema, arrowSchema, idToConstant));
  }

  private File lanceFile(String name) {
    return tempDir.resolve(name + ".lance").toFile();
  }

  private List<Record> writeRecords(
      LanceFormatModel<Record, Void> model, File file, Schema schema, List<Record> records)
      throws IOException {
    EncryptedOutputFile encryptedOutput =
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(file));

    ModelWriteBuilder<Record, Void> writeBuilder =
        model.writeBuilder(encryptedOutput).schema(schema).content(FileContent.DATA);

    try (FileAppender<Record> appender = writeBuilder.build()) {
      for (Record record : records) {
        appender.add(record);
      }
    }
    return records;
  }

  private List<Record> readRecords(
      LanceFormatModel<Record, Void> model, File file, Schema projectedSchema) throws IOException {
    InputFile inputFile = Files.localInput(file);
    ReadBuilder<Record, Void> readBuilder = model.readBuilder(inputFile);

    if (projectedSchema != null) {
      readBuilder.project(projectedSchema);
    }

    List<Record> result = Lists.newArrayList();
    try (CloseableIterable<Record> iterable = readBuilder.build()) {
      for (Record record : iterable) {
        result.add(record);
      }
    }
    return result;
  }

  @Test
  void testBasicPrimitiveRoundTrip() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("basic-roundtrip");

    List<Record> written = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", i);
      record.setField("name", "user_" + i);
      record.setField("age", 20 + (i % 50));
      record.setField("score", i * 1.5);
      written.add(record);
    }

    writeRecords(model, file, SIMPLE_SCHEMA, written);
    List<Record> read = readRecords(model, file, SIMPLE_SCHEMA);

    assertThat(read).hasSize(100);
    for (int i = 0; i < 100; i++) {
      assertThat(read.get(i).getField("id")).isEqualTo(written.get(i).getField("id"));
      assertThat(read.get(i).getField("name")).isEqualTo(written.get(i).getField("name"));
      assertThat(read.get(i).getField("age")).isEqualTo(written.get(i).getField("age"));
      assertThat(read.get(i).getField("score")).isEqualTo(written.get(i).getField("score"));
    }
  }

  @Test
  void testEmptyFile() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("empty");

    writeRecords(model, file, SIMPLE_SCHEMA, Lists.newArrayList());
    List<Record> read = readRecords(model, file, SIMPLE_SCHEMA);

    assertThat(read).isEmpty();
  }

  @Test
  void testNullHandling() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("nulls");

    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "value", Types.DoubleType.get()));

    List<Record> written = Lists.newArrayList();
    for (int i = 0; i < 50; i++) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("id", i);
      record.setField("name", i % 3 == 0 ? null : "name_" + i);
      record.setField("value", i % 5 == 0 ? null : i * 2.0);
      written.add(record);
    }

    writeRecords(model, file, schema, written);
    List<Record> read = readRecords(model, file, schema);

    assertThat(read).hasSize(50);
    for (int i = 0; i < 50; i++) {
      assertThat(read.get(i).getField("id")).isEqualTo(i);
      if (i % 3 == 0) {
        assertThat(read.get(i).getField("name")).isNull();
      } else {
        assertThat(read.get(i).getField("name")).isEqualTo("name_" + i);
      }
      if (i % 5 == 0) {
        assertThat(read.get(i).getField("value")).isNull();
      } else {
        assertThat(read.get(i).getField("value")).isEqualTo(i * 2.0);
      }
    }
  }

  @Test
  void testColumnProjection() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("projection");

    Schema fullSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "city", Types.StringType.get()),
            Types.NestedField.optional(4, "age", Types.IntegerType.get()),
            Types.NestedField.optional(5, "score", Types.DoubleType.get()));

    List<Record> written = Lists.newArrayList();
    for (int i = 0; i < 20; i++) {
      GenericRecord record = GenericRecord.create(fullSchema);
      record.setField("id", i);
      record.setField("name", "user_" + i);
      record.setField("city", "city_" + i);
      record.setField("age", 25 + i);
      record.setField("score", i * 3.14);
      written.add(record);
    }

    writeRecords(model, file, fullSchema, written);

    // Read back only id and name
    Schema projectedSchema =
        new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    List<Record> read = readRecords(model, file, projectedSchema);

    assertThat(read).hasSize(20);
    for (int i = 0; i < 20; i++) {
      assertThat(read.get(i).getField("id")).isEqualTo(i);
      assertThat(read.get(i).getField("name")).isEqualTo("user_" + i);
    }
  }

  @Test
  void testBatchSizeConfiguration() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("batch-size");

    List<Record> written = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
      record.setField("id", i);
      record.setField("name", "user_" + i);
      record.setField("age", 20 + (i % 60));
      record.setField("score", i * 0.1);
      written.add(record);
    }

    writeRecords(model, file, SIMPLE_SCHEMA, written);

    // Read with small batch size
    InputFile inputFile = Files.localInput(file);
    ReadBuilder<Record, Void> readBuilder =
        model.readBuilder(inputFile).project(SIMPLE_SCHEMA).recordsPerBatch(100);

    List<Record> read = Lists.newArrayList();
    try (CloseableIterable<Record> iterable = readBuilder.build()) {
      for (Record record : iterable) {
        read.add(record);
      }
    }

    assertThat(read).hasSize(1000);
    for (int i = 0; i < 1000; i++) {
      assertThat(read.get(i).getField("id")).isEqualTo(written.get(i).getField("id"));
    }
  }

  @Test
  void testSchemaPreservation() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("schema-preservation");

    Schema schema =
        new Schema(
            Types.NestedField.required(10, "pk", Types.LongType.get()),
            Types.NestedField.optional(20, "label", Types.StringType.get()),
            Types.NestedField.optional(30, "active", Types.BooleanType.get()),
            Types.NestedField.required(40, "ratio", Types.FloatType.get()));

    List<Record> written = Lists.newArrayList();
    for (int i = 0; i < 5; i++) {
      GenericRecord record = GenericRecord.create(schema);
      record.setField("pk", (long) i);
      record.setField("label", "label_" + i);
      record.setField("active", i % 2 == 0);
      record.setField("ratio", (float) (i * 0.5));
      written.add(record);
    }

    writeRecords(model, file, schema, written);

    // Read back and verify schema was preserved via metadata
    List<Record> read = readRecords(model, file, schema);
    assertThat(read).hasSize(5);

    // Verify field types round-tripped correctly
    for (int i = 0; i < 5; i++) {
      assertThat(read.get(i).getField("pk")).isInstanceOf(Long.class);
      assertThat(read.get(i).getField("pk")).isEqualTo((long) i);
      assertThat(read.get(i).getField("label")).isEqualTo("label_" + i);
      assertThat(read.get(i).getField("active")).isEqualTo(i % 2 == 0);
      assertThat(read.get(i).getField("ratio")).isInstanceOf(Float.class);
    }
  }

  @Test
  void testFileLength() throws IOException {
    LanceFormatModel<Record, Void> model = createModel();
    File file = lanceFile("file-length");

    EncryptedOutputFile encryptedOutput =
        EncryptedFiles.plainAsEncryptedOutput(Files.localOutput(file));

    ModelWriteBuilder<Record, Void> writeBuilder =
        model.writeBuilder(encryptedOutput).schema(SIMPLE_SCHEMA).content(FileContent.DATA);

    FileAppender<Record> appender = writeBuilder.build();
    try {
      // Write enough records to trigger at least one batch flush (default batch size = 1024)
      for (int i = 0; i < 2000; i++) {
        GenericRecord record = GenericRecord.create(SIMPLE_SCHEMA);
        record.setField("id", i);
        record.setField("name", "user_" + i);
        record.setField("age", 20 + i);
        record.setField("score", i * 1.1);
        appender.add(record);
      }

      // After flushing at least one batch, the estimate should be > 0
      long midWriteLength = appender.length();
      assertThat(midWriteLength).isGreaterThan(0);
    } finally {
      appender.close();
    }

    // After close, length() returns exact file size from storage
    long postCloseLength = appender.length();
    assertThat(postCloseLength).isGreaterThan(0);
    assertThat(postCloseLength).isEqualTo(file.length());
  }
}
