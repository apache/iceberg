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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.IncrementalChangelogScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestChangelogReader extends TestBase {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final Schema SCHEMA2 =
      new Schema(
          ImmutableList.of(
              required(1, "id", Types.IntegerType.get()),
              optional(2, "data", Types.StringType.get())),
          ImmutableSet.of(1));
  private static final PartitionSpec SPEC2 = PartitionSpec.unpartitioned();

  private final List<Record> records1 = Lists.newArrayList();
  private final List<Record> records2 = Lists.newArrayList();
  private final List<Record> records3 = Lists.newArrayList();

  private Table table;
  private Table table2;
  private DataFile dataFile1;
  private DataFile dataFile2;

  @TempDir private Path temp;

  @BeforeEach
  public void before() throws IOException {
    table = catalog.createTable(TableIdentifier.of("default", "test"), SCHEMA, SPEC);
    table2 = catalog.createTable(TableIdentifier.of("default", "test2"), SCHEMA2, SPEC2);

    // create some data
    GenericRecord record = GenericRecord.create(table.schema());
    records1.add(record.copy("id", 29, "data", "a"));
    records1.add(record.copy("id", 43, "data", "b"));
    records1.add(record.copy("id", 61, "data", "c"));
    records1.add(record.copy("id", 89, "data", "d"));

    records2.add(record.copy("id", 100, "data", "e"));
    records2.add(record.copy("id", 121, "data", "f"));
    records2.add(record.copy("id", 122, "data", "g"));

    // write data to files
    dataFile1 = writeDataFile(records1);
    dataFile2 = writeDataFile(records2);

    // records to be deleted
    records3.add(record.copy("id", 29, "data", "a"));
    records3.add(record.copy("id", 89, "data", "d"));
    records3.add(record.copy("id", 122, "data", "g"));
  }

  @AfterEach
  public void after() {
    catalog.dropTable(TableIdentifier.of("default", "test"));
    catalog.dropTable(TableIdentifier.of("default", "test2"));
  }

  @Test
  public void testInsert() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups = newScan().planTasks();

    List<InternalRow> rows = Lists.newArrayList();

    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      ChangelogRowReader reader =
          new ChangelogRowReader(table, taskGroup, table.schema(), table.schema(), false);
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
      reader.close();
    }

    rows.sort((r1, r2) -> r1.getInt(0) - r2.getInt(0));

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, records2);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testDelete() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newDelete().deleteFile(dataFile1).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups =
        newScan().fromSnapshotExclusive(snapshotId1).planTasks();

    List<InternalRow> rows = Lists.newArrayList();

    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      ChangelogRowReader reader =
          new ChangelogRowReader(table, taskGroup, table.schema(), table.schema(), false);
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
      reader.close();
    }

    rows.sort((r1, r2) -> r1.getInt(0) - r2.getInt(0));

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId2, 0, records1);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testDataFileRewrite() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    table
        .newRewrite()
        .rewriteFiles(ImmutableSet.of(dataFile1), ImmutableSet.of(dataFile2))
        .commit();

    // the rewrite operation should generate no Changelog rows
    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups =
        newScan().fromSnapshotExclusive(snapshotId2).planTasks();

    List<InternalRow> rows = Lists.newArrayList();

    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      ChangelogRowReader reader =
          new ChangelogRowReader(table, taskGroup, table.schema(), table.schema(), false);
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
      reader.close();
    }

    assertThat(rows).as("Should have no rows").isEmpty();
  }

  @Test
  public void testMixDeleteAndInsert() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newDelete().deleteFile(dataFile1).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table);

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId2, 1, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId3, 2, records2);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testPositionDeletes() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile1.path(), 0L), // id = 29
            Pair.of(dataFile1.path(), 3L), // id = 89
            Pair.of(dataFile2.path(), 2L) // id = 122
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table);

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, records2);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId3, 2, records3);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testEqualityDeletes() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "d"), // id = 89
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table.newRowDelta().addDeletes(eqDeletes).commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table);

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, records2);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId3, 2, records3);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testMixOfPositionAndEqualityDeletes() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile1.path(), 0L), // id = 29
            Pair.of(dataFile1.path(), 3L) // id = 89
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "a"), // id = 29
            dataDelete.copy("data", "g") // id = 122
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    table
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table);

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, records2);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId3, 2, records3);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testAddingAndDeletingInSameCommit() throws IOException {
    GenericRecord record = GenericRecord.create(table.schema());
    List<Record> records1b = Lists.newArrayList();
    records1b.add(record.copy("id", 28, "data", "a"));
    records1b.add(record.copy("id", 29, "data", "a"));
    records1b.add(record.copy("id", 43, "data", "b"));
    records1b.add(record.copy("id", 44, "data", "b"));
    records1b.add(record.copy("id", 61, "data", "c"));
    records1b.add(record.copy("id", 89, "data", "d"));
    DataFile dataFile1b = writeDataFile(records1b);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile1b.path(), 0L), // id = 28
            Pair.of(dataFile1b.path(), 3L) // id = 44
            );

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table
        .newRowDelta()
        .addRows(dataFile1b)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();
    // the resulting records in the table are the same as records1
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table);

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, records2);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testRepeatedEqualityDelete() throws IOException {
    List<Record> rl1 = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(table2.schema());
    rl1.add(record.copy("id", 1, "data", "a"));
    rl1.add(record.copy("id", 2, "data", "b"));
    rl1.add(record.copy("id", 3, "data", "c"));
    DataFile df1 = writeDataFile(rl1);
    table2.newAppend().appendFile(df1).commit();
    long snapshotId1 = table2.currentSnapshot().snapshotId();

    // update row with id=2 by deleting it using an equality delete and writing a new row with id=2
    Schema deleteRowSchema = table2.schema().select("id");
    Record idDelete = GenericRecord.create(deleteRowSchema);
    List<Record> idDeletes = Lists.newArrayList(idDelete.copy("id", 2));

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table2,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            idDeletes,
            deleteRowSchema);

    List<Record> rl2 = Lists.newArrayList(record.copy("id", 2, "data", "bb"));
    DataFile df2 = writeDataFile(rl2);

    table2.newRowDelta().addDeletes(eqDeletes).addRows(df2).commit();
    long snapshotId2 = table2.currentSnapshot().snapshotId();

    // update row with id=2 again the same way
    List<Record> idDeletes2 = Lists.newArrayList(idDelete.copy("id", 2));

    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table2,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            idDeletes2,
            deleteRowSchema);

    List<Record> rl3 = Lists.newArrayList(record.copy("id", 2, "data", "bbb"));
    DataFile df3 = writeDataFile(rl3);

    table2.newRowDelta().addDeletes(eqDeletes2).addRows(df3).commit();
    long snapshotId3 = table2.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table2);

    List<Record> deleted1 = Lists.newArrayList(record.copy("id", 2, "data", "b"));
    List<Record> deleted2 = Lists.newArrayList(record.copy("id", 2, "data", "bb"));

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, rl1);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId2, 1, deleted1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, rl2);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId3, 2, deleted2);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId3, 2, rl3);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  @Test
  public void testRepeatedUpdateInSameCommit() throws IOException {
    List<Record> rl1 = Lists.newArrayList();
    GenericRecord record = GenericRecord.create(table2.schema());
    rl1.add(record.copy("id", 1, "data", "a"));
    rl1.add(record.copy("id", 2, "data", "b"));
    rl1.add(record.copy("id", 3, "data", "c"));
    DataFile df1 = writeDataFile(rl1);
    table2.newAppend().appendFile(df1).commit();
    long snapshotId1 = table2.currentSnapshot().snapshotId();

    // delete row with id=2 with an equality delete and write a new row for it, but then delete the
    // new row with a positional delete and write yet another new row for it
    Schema deleteRowSchema = table2.schema().select("id");
    Record idDelete = GenericRecord.create(deleteRowSchema);
    List<Record> idDeletes = Lists.newArrayList(idDelete.copy("id", 2));

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table2,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            idDeletes,
            deleteRowSchema);

    List<Record> rl2 = Lists.newArrayList();
    rl2.add(record.copy("id", 2, "data", "bb"));
    rl2.add(record.copy("id", 4, "data", "d"));
    rl2.add(record.copy("id", 5, "data", "e"));
    rl2.add(record.copy("id", 2, "data", "bbb"));
    DataFile df2 = writeDataFile(rl2);

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList(Pair.of(df2.path(), 0L));

    Pair<DeleteFile, CharSequenceSet> posDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            deletes);

    table2
        .newRowDelta()
        .addDeletes(eqDeletes)
        .addRows(df2)
        .addDeletes(posDeletes.first())
        .validateDataFilesExist(posDeletes.second())
        .commit();
    long snapshotId2 = table2.currentSnapshot().snapshotId();

    List<InternalRow> rows = getChangelogRows(table2);

    // for snapshot 2, we should record only one delete for id=2 (from the equality delete)
    // and one insert (the final value)
    List<Record> deleted = Lists.newArrayList();
    deleted.add(record.copy("id", 2, "data", "b"));
    List<Record> inserted = Lists.newArrayList();
    inserted.add(record.copy("id", 2, "data", "bbb"));
    inserted.add(record.copy("id", 4, "data", "d"));
    inserted.add(record.copy("id", 5, "data", "e"));

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, rl1);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId2, 1, deleted);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId2, 1, inserted);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  private IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
  }

  private List<InternalRow> getChangelogRows(Table tbl) throws IOException {
    CloseableIterable<ScanTaskGroup<ChangelogScanTask>> taskGroups =
        tbl.newIncrementalChangelogScan().planTasks();

    List<InternalRow> rows = Lists.newArrayList();

    for (ScanTaskGroup<ChangelogScanTask> taskGroup : taskGroups) {
      ChangelogRowReader reader =
          new ChangelogRowReader(tbl, taskGroup, tbl.schema(), tbl.schema(), false);
      while (reader.next()) {
        rows.add(reader.get().copy());
      }
      reader.close();
    }

    // order by change ordinal, change type, data, id
    rows.sort(
        Comparator.comparingInt((InternalRow r) -> r.getInt(3))
            .thenComparing((InternalRow r) -> r.getUTF8String(2))
            .thenComparing((InternalRow r) -> r.getUTF8String(1))
            .thenComparingInt((InternalRow r) -> r.getInt(0)));

    return rows;
  }

  private List<Object[]> addExpectedRows(
      List<Object[]> expectedRows,
      ChangelogOperation operation,
      long snapshotId,
      int changeOrdinal,
      List<Record> records) {
    records.forEach(
        r ->
            expectedRows.add(row(r.get(0), r.get(1), operation.name(), changeOrdinal, snapshotId)));
    return expectedRows;
  }

  protected List<Object[]> internalRowsToJava(List<InternalRow> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(InternalRow row) {
    Object[] values = new Object[row.numFields()];
    values[0] = row.getInt(0);
    values[1] = row.getString(1);
    values[2] = row.getString(2);
    values[3] = row.getInt(3);
    values[4] = row.getLong(4);
    return values;
  }

  private DataFile writeDataFile(List<Record> records) throws IOException {
    // records all use IDs that are in bucket id_bucket=0
    return FileHelpers.writeDataFile(
        table,
        Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
        TestHelpers.Row.of(0),
        records);
  }
}
