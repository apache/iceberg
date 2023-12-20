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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.DataFile;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
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
  private final List<Record> records1 = Lists.newArrayList();
  private final List<Record> records2 = Lists.newArrayList();

  private Table table;
  private DataFile dataFile1;
  private DataFile dataFile2;

  @TempDir private Path temp;

  @BeforeEach
  public void before() throws IOException {
    table = catalog.createTable(TableIdentifier.of("default", "test"), SCHEMA, SPEC);
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
  }

  @AfterEach
  public void after() {
    catalog.dropTable(TableIdentifier.of("default", "test"));
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

    assertThat(rows).as("Should have no rows").hasSize(0);
  }

  @Test
  public void testMixDeleteAndInsert() throws IOException {
    table.newAppend().appendFile(dataFile1).commit();
    long snapshotId1 = table.currentSnapshot().snapshotId();

    table.newDelete().deleteFile(dataFile1).commit();
    long snapshotId2 = table.currentSnapshot().snapshotId();

    table.newAppend().appendFile(dataFile2).commit();
    long snapshotId3 = table.currentSnapshot().snapshotId();

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

    // order by the change ordinal
    rows.sort(
        (r1, r2) -> {
          if (r1.getInt(3) != r2.getInt(3)) {
            return r1.getInt(3) - r2.getInt(3);
          } else {
            return r1.getInt(0) - r2.getInt(0);
          }
        });

    List<Object[]> expectedRows = Lists.newArrayList();
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId1, 0, records1);
    addExpectedRows(expectedRows, ChangelogOperation.DELETE, snapshotId2, 1, records1);
    addExpectedRows(expectedRows, ChangelogOperation.INSERT, snapshotId3, 2, records2);

    assertEquals("Should have expected rows", expectedRows, internalRowsToJava(rows));
  }

  private IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
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
        table, Files.localOutput(temp.toFile()), TestHelpers.Row.of(0), records);
  }
}
