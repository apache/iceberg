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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestRowLineageMetadata {

  private static final String TEST_LOCATION = "s3://bucket/test/location";

  @TempDir protected File tableDir = null;

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private TableMetadata.Builder builderFor(int formatVersion) {
    return TableMetadata.buildFromEmpty(formatVersion).enableRowLineage();
  }

  private TableMetadata baseMetadata(int formatVersion) {
    return builderFor(formatVersion)
        .addSchema(TEST_SCHEMA)
        .setLocation(TEST_LOCATION)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .build();
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testRowLineageSupported(int formatVersion) {
    if (formatVersion == TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE) {
      assertThat(builderFor(formatVersion)).isNotNull();
    } else {
      IllegalArgumentException notSupported =
          assertThrows(
              IllegalArgumentException.class,
              () -> TableMetadata.buildFromEmpty(formatVersion).enableRowLineage());
      assertThat(notSupported.getMessage()).contains("Cannot use row lineage");
    }
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testSnapshotAddition(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    Long newRows = 30L;

    TableMetadata base = baseMetadata(formatVersion);

    Snapshot addRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId(), newRows);

    TableMetadata firstAddition = TableMetadata.buildFrom(base).addSnapshot(addRows).build();

    assertThat(firstAddition.lastRowId()).isEqualTo(newRows);

    Snapshot addMoreRows =
        new BaseSnapshot(
            1, 2, 1L, 0, DataOperations.APPEND, null, 1, "foo", firstAddition.lastRowId(), newRows);

    TableMetadata secondAddition =
        TableMetadata.buildFrom(firstAddition).addSnapshot(addMoreRows).build();

    assertThat(secondAddition.lastRowId()).isEqualTo(newRows * 2);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testInvalidSnapshotAddition(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    Long newRows = 30L;

    TableMetadata base = baseMetadata(formatVersion);

    Snapshot invalidLastRow =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId() - 3, newRows);

    ValidationException invalidLastRowId =
        assertThrows(
            ValidationException.class,
            () -> TableMetadata.buildFrom(base).addSnapshot(invalidLastRow));
    assertThat(invalidLastRowId.getMessage()).contains("Cannot add a snapshot whose first-row-id");

    Snapshot invalidNewRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.lastRowId(), null);

    ValidationException nullNewRows =
        assertThrows(
            ValidationException.class,
            () -> TableMetadata.buildFrom(base).addSnapshot(invalidNewRows));
    assertThat(nullNewRows.getMessage())
        .contains(
            "Cannot add a snapshot with a null `addedRows` field when row lineage is enabled");
  }

  private AtomicInteger fileNum = new AtomicInteger(0);

  private DataFile fileWithRows(int numRows) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(numRows)
        .withFileSizeInBytes(numRows * 100)
        .withPath("file://file_" + fileNum.incrementAndGet() + ".parquet")
        .build();
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testFastAppend(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.ops().current().lastRowId()).isEqualTo(0L);

    table.newFastAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30);

    table.newFastAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30 + 17 + 11);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testAppend(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.ops().current().lastRowId()).isEqualTo(0L);

    table.newAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30);

    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30 + 17 + 11);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testAppendBranch(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);
    // Appends to a branch should still change last-row-id even if not on main, these changes
    // should also affect commits to main

    String branch = "some_branch";

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.ops().current().lastRowId()).isEqualTo(0L);

    // Write to Branch
    table.newAppend().appendFile(fileWithRows(30)).toBranch(branch).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(0L);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30);

    // Write to Main
    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30 + 17 + 11);

    // Write again to branch
    table.newAppend().appendFile(fileWithRows(21)).toBranch(branch).commit();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(30 + 17 + 11);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30 + 17 + 11 + 21);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testDeletes(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.ops().current().lastRowId()).isEqualTo(0L);

    DataFile file = fileWithRows(30);

    table.newAppend().appendFile(file).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30);

    table.newDelete().deleteFile(file).commit();

    // Deleting a file should create a new snapshot which should inherit last-row-id from the
    // previous metadata and not
    // change last-row-id for this metadata.
    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().lastRowId()).isEqualTo(30);
  }

  @ParameterizedTest
  @FieldSource("org.apache.iceberg.TestHelpers#ALL_VERSIONS")
  public void testReplace(int formatVersion) {
    assumeTrue(formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();

    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.ops().current().lastRowId()).isEqualTo(0L);

    DataFile filePart1 = fileWithRows(30);
    DataFile filePart2 = fileWithRows(30);
    DataFile fileCompacted = fileWithRows(60);

    table.newAppend().appendFile(filePart1).appendFile(filePart2).commit();

    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().lastRowId()).isEqualTo(60);

    table
        .newRewrite()
        .deleteFile(filePart1)
        .deleteFile(filePart2)
        .addFile(fileCompacted)
        .commit();

    // Rewrites are currently just treated as appends. In the future we could treat these as no-ops
    assertThat(table.ops().current().rowLineage()).isTrue();
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(60);
    assertThat(table.ops().current().lastRowId()).isEqualTo(120);
  }
}
