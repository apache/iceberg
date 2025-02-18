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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRowLineageMetadata {

  @Parameters(name = "formatVersion = {0}")
  private static List<Integer> formatVersion() {
    return Ints.asList(TestHelpers.ALL_VERSIONS);
  }

  @Parameter private int formatVersion;

  private static final String TEST_LOCATION = "s3://bucket/test/location";

  private static final Schema TEST_SCHEMA =
      new Schema(
          7,
          Types.NestedField.required(1, "x", Types.LongType.get()),
          Types.NestedField.required(2, "y", Types.LongType.get(), "comment"),
          Types.NestedField.required(3, "z", Types.LongType.get()));

  private TableMetadata baseMetadata() {
    return TableMetadata.buildFromEmpty(formatVersion)
        .enableRowLineage()
        .addSchema(TEST_SCHEMA)
        .setLocation(TEST_LOCATION)
        .addPartitionSpec(PartitionSpec.unpartitioned())
        .addSortOrder(SortOrder.unsorted())
        .build();
  }

  @TempDir private File tableDir = null;

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @TestTemplate
  public void testRowLineageSupported() {
    if (formatVersion >= TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE) {
      assertThat(TableMetadata.buildFromEmpty(formatVersion).enableRowLineage()).isNotNull();
    } else {
      assertThatThrownBy(() -> TableMetadata.buildFromEmpty(formatVersion).enableRowLineage())
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot use row lineage");
    }
  }

  @TestTemplate
  public void testSnapshotAddition() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    long newRows = 30L;

    TableMetadata base = baseMetadata();

    Snapshot addRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.nextRowId(), newRows);

    TableMetadata firstAddition = TableMetadata.buildFrom(base).addSnapshot(addRows).build();

    assertThat(firstAddition.nextRowId()).isEqualTo(newRows);

    Snapshot addMoreRows =
        new BaseSnapshot(
            1, 2, 1L, 0, DataOperations.APPEND, null, 1, "foo", firstAddition.nextRowId(), newRows);

    TableMetadata secondAddition =
        TableMetadata.buildFrom(firstAddition).addSnapshot(addMoreRows).build();

    assertThat(secondAddition.nextRowId()).isEqualTo(newRows * 2);
  }

  @TestTemplate
  public void testInvalidSnapshotAddition() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    Long newRows = 30L;

    TableMetadata base = baseMetadata();

    Snapshot invalidLastRow =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.nextRowId() - 3, newRows);

    assertThatThrownBy(() -> TableMetadata.buildFrom(base).addSnapshot(invalidLastRow))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot add a snapshot whose 'first-row-id'");

    Snapshot invalidNewRows =
        new BaseSnapshot(
            0, 1, null, 0, DataOperations.APPEND, null, 1, "foo", base.nextRowId(), null);

    assertThatThrownBy(() -> TableMetadata.buildFrom(base).addSnapshot(invalidNewRows))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "Cannot add a snapshot with a null 'added-rows' field when row lineage is enabled");
  }

  @TestTemplate
  public void testFastAppend() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    table.newFastAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newFastAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);
  }

  @TestTemplate
  public void testAppend() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    table.newAppend().appendFile(fileWithRows(30)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);
  }

  @TestTemplate
  public void testAppendBranch() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);
    // Appends to a branch should still change last-row-id even if not on main, these changes
    // should also affect commits to main

    String branch = "some_branch";

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    // Write to Branch
    table.newAppend().appendFile(fileWithRows(30)).toBranch(branch).commit();

    assertThat(table.currentSnapshot()).isNull();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(0L);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    // Write to Main
    table.newAppend().appendFile(fileWithRows(17)).appendFile(fileWithRows(11)).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11);

    // Write again to branch
    table.newAppend().appendFile(fileWithRows(21)).toBranch(branch).commit();
    assertThat(table.snapshot(branch).firstRowId()).isEqualTo(30 + 17 + 11);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30 + 17 + 11 + 21);
  }

  @TestTemplate
  public void testDeletes() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();
    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile file = fileWithRows(30);

    table.newAppend().appendFile(file).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);

    table.newDelete().deleteFile(file).commit();

    // Deleting a file should create a new snapshot which should inherit last-row-id from the
    // previous metadata and not
    // change last-row-id for this metadata.
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(30);
    assertThat(table.currentSnapshot().addedRows()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(30);
  }

  @TestTemplate
  public void testReplace() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    TableMetadata base = table.ops().current();

    table.ops().commit(base, TableMetadata.buildFrom(base).enableRowLineage().build());

    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
    assertThat(table.ops().current().nextRowId()).isEqualTo(0L);

    DataFile filePart1 = fileWithRows(30);
    DataFile filePart2 = fileWithRows(30);
    DataFile fileCompacted = fileWithRows(60);

    table.newAppend().appendFile(filePart1).appendFile(filePart2).commit();

    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(0);
    assertThat(table.ops().current().nextRowId()).isEqualTo(60);

    table.newRewrite().deleteFile(filePart1).deleteFile(filePart2).addFile(fileCompacted).commit();

    // Rewrites are currently just treated as appends. In the future we could treat these as no-ops
    assertThat(table.currentSnapshot().firstRowId()).isEqualTo(60);
    assertThat(table.ops().current().nextRowId()).isEqualTo(120);
  }

  @TestTemplate
  public void testEnableRowLineageViaProperty() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir, "test", TEST_SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(table.ops().current().rowLineageEnabled()).isFalse();

    // No-op
    table.updateProperties().set(TableProperties.ROW_LINEAGE, "false").commit();
    assertThat(table.ops().current().rowLineageEnabled()).isFalse();

    // Enable row lineage
    table.updateProperties().set(TableProperties.ROW_LINEAGE, "true").commit();
    assertThat(table.ops().current().rowLineageEnabled()).isTrue();

    // Disabling row lineage is not allowed
    assertThatThrownBy(
            () -> table.updateProperties().set(TableProperties.ROW_LINEAGE, "false").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot disable row lineage once it has been enabled");

    // No-op
    table.updateProperties().set(TableProperties.ROW_LINEAGE, "true").commit();
    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
  }

  @TestTemplate
  public void testEnableRowLineageViaPropertyAtTableCreation() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(TableMetadata.MIN_FORMAT_VERSION_ROW_LINEAGE);

    TestTables.TestTable table =
        TestTables.create(
            tableDir,
            "test",
            TEST_SCHEMA,
            ImmutableMap.of(TableProperties.ROW_LINEAGE, "true"),
            formatVersion);
    assertThat(table.ops().current().rowLineageEnabled()).isTrue();
  }

  private final AtomicInteger fileNum = new AtomicInteger(0);

  private DataFile fileWithRows(long numRows) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(numRows)
        .withFileSizeInBytes(numRows * 100)
        .withPath("file://file_" + fileNum.incrementAndGet() + ".parquet")
        .build();
  }
}
