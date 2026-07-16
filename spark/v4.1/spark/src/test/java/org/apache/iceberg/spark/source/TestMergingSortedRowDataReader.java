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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.iceberg.BaseScanTaskGroup;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestMergingSortedRowDataReader extends TestBase {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();

  private Table table;

  @TempDir private Path temp;

  @BeforeEach
  void before() {
    table = catalog.createTable(TableIdentifier.of("default", "test_merging_reader"), SCHEMA, SPEC);
    table.replaceSortOrder().asc("id").commit();
  }

  @AfterEach
  void after() {
    catalog.dropTable(TableIdentifier.of("default", "test_merging_reader"));
  }

  @Test
  void mergeTwoSortedFiles() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4, 5, 6);
  }

  @Test
  void mergeWithDuplicateKeys() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(2, "b"));
    DataFile file2 = writeDataFile(record(1, "c"), record(2, "d"));
    DataFile file3 = writeDataFile(record(1, "e"), record(3, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 1, 1, 2, 2, 3);
  }

  @Test
  void mergeDescendingOrder() throws IOException {
    catalog.dropTable(TableIdentifier.of("default", "test_merging_reader"));
    table = catalog.createTable(TableIdentifier.of("default", "test_merging_reader"), SCHEMA, SPEC);
    table.replaceSortOrder().desc("id").commit();

    DataFile file1 = writeDataFile(record(6, "f"), record(4, "d"));
    DataFile file2 = writeDataFile(record(5, "e"), record(3, "c"), record(1, "a"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(6, 5, 4, 3, 1);
  }

  @Test
  void mergeWithNulls() throws IOException {
    Schema nullableSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));

    catalog.dropTable(TableIdentifier.of("default", "test_merging_reader"));
    table =
        catalog.createTable(
            TableIdentifier.of("default", "test_merging_reader"), nullableSchema, SPEC);
    table.replaceSortOrder().asc("id").commit();

    DataFile file1 = writeDataFile(nullRecord("x"), record(3, "c"));
    DataFile file2 = writeDataFile(nullRecord("y"), record(1, "a"), record(2, "b"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(rows).hasSize(5);
    assertThat(rows.get(0).isNullAt(0)).isTrue();
    assertThat(rows.get(1).isNullAt(0)).isTrue();
    assertThat(extractIds(rows.subList(2, 5))).containsExactly(1, 2, 3);
  }

  @Test
  void mergeThreeFiles() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(4, "d"), record(7, "g"));
    DataFile file2 = writeDataFile(record(2, "b"), record(5, "e"), record(8, "h"));
    DataFile file3 = writeDataFile(record(3, "c"), record(6, "f"), record(9, "i"));

    table.newAppend().appendFile(file1).appendFile(file2).appendFile(file3).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9);
  }

  @Test
  void mergeWithSortKeyNotInProjection() throws IOException {
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // Project only "data". The sort key "id" is missing from the projection, so it is added to
    // the read schema for the merge comparator and stripped from the rows returned to Spark.
    Schema dataOnly = table.schema().select("data");
    List<InternalRow> rows = readMerged(table, dataOnly);

    // Rows come back ordered by id even though id is not projected.
    assertThat(extractData(rows, 0)).containsExactly("a", "b", "c", "d", "e", "f");
    // Only the projected column is present in the returned rows.
    assertThat(rows.get(0).numFields()).isEqualTo(1);
  }

  @Test
  void mergeAfterSortOrderEvolution() throws IOException {
    // Evolve the sort order from "id" to "data". The reader should merge by the current order.
    table.replaceSortOrder().asc("data").commit();

    DataFile file1 = writeDataFile(record(5, "a"), record(3, "c"), record(1, "e"));
    DataFile file2 = writeDataFile(record(6, "b"), record(4, "d"), record(2, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    List<InternalRow> rows = readMerged(table);

    // Ordered by data, not by id.
    assertThat(extractData(rows, 1)).containsExactly("a", "b", "c", "d", "e", "f");
  }

  @Test
  void mergeWithPositionDeletes() throws IOException {
    // File1: [1, 3, 5], File2: [2, 4, 6]
    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // Delete the row at position 1 in file1 (value 3).
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
                Lists.newArrayList(Pair.of(file1.location(), 1L)),
                TableUtil.formatVersion(table))
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    List<InternalRow> rows = readMerged(table);

    assertThat(extractIds(rows)).containsExactly(1, 2, 4, 5, 6);
  }

  @Test
  void mergeWithSortOrderReferencingSameColumnMultipleTimes() throws IOException {
    table
        .replaceSortOrder()
        .asc(org.apache.iceberg.expressions.Expressions.bucket("id", 16))
        .asc("id")
        .commit();

    DataFile file1 = writeDataFile(record(1, "a"), record(3, "c"), record(5, "e"));
    DataFile file2 = writeDataFile(record(2, "b"), record(4, "d"), record(6, "f"));

    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // the sort key "id" is not in the projection and is referenced by two sort fields.
    Schema dataOnly = table.schema().select("data");
    List<InternalRow> rows = readMerged(table, dataOnly);

    assertThat(rows).hasSize(6);
    assertThat(rows.get(0).numFields()).isEqualTo(1);
    assertThat(extractData(rows, 0)).containsExactlyInAnyOrder("a", "b", "c", "d", "e", "f");
  }

  private List<InternalRow> readMerged(Table tbl) throws IOException {
    return readMerged(tbl, tbl.schema());
  }

  private List<InternalRow> readMerged(Table tbl, Schema projection) throws IOException {
    tbl.refresh();

    List<FileScanTask> fileTasks = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = tbl.newScan().planFiles()) {
      tasks.forEach(fileTasks::add);
    }

    assertThat(fileTasks).hasSizeGreaterThan(1);

    BaseScanTaskGroup<FileScanTask> taskGroup = new BaseScanTaskGroup<>(fileTasks);

    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(tbl));
    Broadcast<org.apache.iceberg.io.FileIO> fileIOBroadcast =
        sparkContext.broadcast(SerializableFileIOWithSize.wrap(tbl.io()));

    SparkInputPartition partition =
        new SparkInputPartition(
            Types.StructType.of(),
            taskGroup,
            tableBroadcast,
            fileIOBroadcast,
            SchemaParser.toJson(projection),
            true,
            new String[0],
            false);

    List<InternalRow> rows = Lists.newArrayList();
    try (MergingSortedRowDataReader reader = new MergingSortedRowDataReader(partition)) {
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
    }

    return rows;
  }

  private List<Integer> extractIds(List<InternalRow> rows) {
    return rows.stream().map(row -> row.isNullAt(0) ? null : row.getInt(0)).toList();
  }

  private List<String> extractData(List<InternalRow> rows, int ordinal) {
    return rows.stream().map(row -> row.getUTF8String(ordinal).toString()).toList();
  }

  private Record record(int id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.set(0, id);
    record.set(1, data);
    return record;
  }

  private Record nullRecord(String data) {
    Schema nullableSchema =
        new Schema(
            Types.NestedField.optional(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));
    GenericRecord record = GenericRecord.create(nullableSchema);
    record.set(0, null);
    record.set(1, data);
    return record;
  }

  private DataFile writeDataFile(Record... records) throws IOException {
    return FileHelpers.writeDataFile(
        table,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        Lists.newArrayList(records));
  }
}
