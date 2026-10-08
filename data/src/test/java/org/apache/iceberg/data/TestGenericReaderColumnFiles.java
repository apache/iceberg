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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.ColumnFile;
import org.apache.iceberg.ColumnFileTestHelpers;
import org.apache.iceberg.ColumnFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestGenericReaderColumnFiles {
  private static final Types.NestedField ID = required(1, "id", Types.IntegerType.get());
  private static final Types.NestedField DATA = optional(2, "data", Types.StringType.get());
  private static final Types.NestedField COUNT = optional(3, "count", Types.IntegerType.get());
  private static final Types.NestedField POINT =
      optional(
          4,
          "point",
          Types.StructType.of(
              required(5, "x", Types.IntegerType.get()),
              required(6, "y", Types.IntegerType.get())));
  private static final Schema SCHEMA = new Schema(ID, DATA, COUNT, POINT);

  @TempDir private Path temp;

  private Table table;

  @BeforeEach
  void createTable() {
    this.table =
        TestTables.create(temp.toFile(), "column_files", SCHEMA, PartitionSpec.unpartitioned(), 4);
  }

  @AfterEach
  void clearTables() {
    TestTables.clearTables();
  }

  @Test
  void readsFieldsFromColumnFiles() throws IOException {
    DataFile file =
        dataFile(
            columnFile("data", DATA, "A", "B", "C", "D"),
            columnFile("count", COUNT, 10, 20, 30, 40));

    assertThat(read(task(file), SCHEMA))
        .extracting(
            row -> row.getField("id"),
            row -> row.getField("data"),
            row -> row.getField("count"),
            row -> row.getField("point"))
        .containsExactly(
            tuple(1, "A", 10, point(1)),
            tuple(2, "B", 20, point(2)),
            tuple(3, "C", 30, point(3)),
            tuple(4, "D", 40, point(4)));
  }

  @Test
  void readsOnlyDataFileFields() throws IOException {
    DataFile file = dataFile(columnFile("data", DATA, "A", "B", "C", "D"));

    assertThat(read(task(file), SCHEMA.select("id", "count")))
        .extracting(row -> row.getField("id"), row -> row.getField("count"))
        .containsExactly(tuple(1, 0), tuple(2, 0), tuple(3, 0), tuple(4, 0));
  }

  @Test
  void appliesDeletesByPosition() throws IOException {
    DataFile file = dataFile(columnFile("data", DATA, "A", "B", "C", "D"));
    List<Pair<CharSequence, Long>> deletes =
        List.of(Pair.of(file.location(), 1L), Pair.of(file.location(), 3L));
    OutputFile out = Files.localOutput(temp.resolve("dv.puffin").toFile());
    DeleteFile deleteFile = FileHelpers.writeDeleteFile(table, out, deletes, 4).first();

    assertThat(read(task(file, deleteFile), SCHEMA.select("data")))
        .extracting(row -> row.getField("data"))
        .containsExactly("A", "C");
  }

  private List<Record> read(FileScanTask task, Schema projection) throws IOException {
    try (CloseableIterable<Record> rows =
        new GenericReader(table.newScan().project(projection), false).open(task)) {
      return Lists.newArrayList(rows);
    }
  }

  private FileScanTask task(DataFile file, DeleteFile... deletes) {
    return new BaseFileScanTask(
        file,
        deletes,
        SchemaParser.toJson(table.schema()),
        PartitionSpecParser.toJson(table.spec()),
        ResidualEvaluator.unpartitioned(Expressions.alwaysTrue()));
  }

  private DataFile dataFile(ColumnFile... columnFiles) throws IOException {
    DataFile file =
        write(
            "base",
            SCHEMA,
            List.of(baseRow(1, "a"), baseRow(2, "b"), baseRow(3, "c"), baseRow(4, "d")));

    return ColumnFileTestHelpers.withColumnFiles(
        file, PartitionSpec.unpartitioned(), Arrays.asList(columnFiles));
  }

  private static Record baseRow(int id, String data) {
    return GenericRecord.create(SCHEMA)
        .copy(ImmutableMap.of("id", id, "data", data, "count", 0, "point", point(id)));
  }

  private static Record point(int value) {
    return GenericRecord.create(POINT.type().asStructType()).copy("x", value, "y", value);
  }

  private ColumnFile columnFile(String name, Types.NestedField field, Object... values)
      throws IOException {
    Schema schema = new Schema(field);
    GenericRecord record = GenericRecord.create(schema);
    DataFile file =
        write(
            name,
            schema,
            Arrays.stream(values).map(value -> record.copy(field.name(), value)).toList());

    return ColumnFiles.builder()
        .withFormatVersion(4)
        .withFieldIds(List.of(field.fieldId()))
        .withLocation(file.location())
        .withFileFormat(file.format())
        .withFileSizeInBytes(file.fileSizeInBytes())
        .build();
  }

  private DataFile write(String name, Schema schema, List<Record> records) throws IOException {
    OutputFile out =
        Files.localOutput(temp.resolve(FileFormat.PARQUET.addExtension(name)).toFile());
    DataWriter<Record> writer =
        FormatModelRegistry.<Record, Object>dataWriteBuilder(
                FileFormat.PARQUET, Record.class, EncryptedFiles.plainAsEncryptedOutput(out))
            .schema(schema)
            .spec(PartitionSpec.unpartitioned())
            .build();

    try (writer) {
      records.forEach(writer::write);
    }

    return writer.toDataFile();
  }
}
