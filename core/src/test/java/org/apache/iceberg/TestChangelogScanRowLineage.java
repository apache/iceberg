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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests that {@link IncrementalChangelogScan} exposes row lineage ({@code _row_id} and {@code
 * _last_updated_sequence_number}) in the planned tasks for v3+ tables.
 *
 * <p>These tests FAIL today because:
 *
 * <ul>
 *   <li>{@code ChangelogScanTask} has no {@code rowId()} method — row lineage is not surfaced
 *       through the task API even though the underlying {@code DataFile} carries {@code
 *       firstRowId()}.
 *   <li>{@code ChangelogUtil.changelogSchema()} only joins {@code CHANGE_TYPE}, {@code
 *       CHANGE_ORDINAL}, and {@code COMMIT_SNAPSHOT_ID} — {@code ROW_ID} and {@code
 *       LAST_UPDATED_SEQUENCE_NUMBER} are never included in the changelog schema.
 * </ul>
 *
 * <p>After the fix:
 *
 * <ul>
 *   <li>{@code ChangelogScanTask} (or its sub-interfaces {@code AddedRowsScanTask} and {@code
 *       DeletedDataFileScanTask}) must expose {@code rowId()} returning the {@code firstRowId} of
 *       the underlying data file.
 *   <li>Row IDs assigned across consecutive snapshots must be non-overlapping and monotonically
 *       increasing.
 *   <li>On a v3+ table, the changelog schema returned by {@code IncrementalChangelogScan.schema()}
 *       must include {@code _row_id} and {@code _last_updated_sequence_number}.
 * </ul>
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestChangelogScanRowLineage extends TestBase {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return TestHelpers.ALL_VERSIONS;
  }

  // ---------------------------------------------------------------------------
  // Test 1: AddedRowsScanTask exposes non-null firstRowId on a v3+ table.
  //
  // FAILS today because:
  //   - ChangelogScanTask / AddedRowsScanTask have no rowId() method
  //   - Even if accessed via task.file().firstRowId(), the DataFile carries a null firstRowId
  //     at planning time because row IDs are only assigned when the manifest list is written,
  //     and the changelog scan reads the manifest entries after that assignment — so
  //     task.file().firstRowId() is actually non-null. But the public API doesn't expose it.
  //
  // The test verifies that the API (not just the internal DataFile) exposes the row ID.
  // ---------------------------------------------------------------------------
  @TestTemplate
  public void testAddedRowsScanTaskExposesRowId() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    List<ChangelogScanTask> tasks = plan(table.newIncrementalChangelogScan());

    assertThat(tasks).as("must have one task for FILE_A").hasSize(1);

    AddedRowsScanTask task = (AddedRowsScanTask) tasks.get(0);
    assertThat(task.commitSnapshotId()).isEqualTo(snap1.snapshotId());

    // After the fix: rowId() must be exposed through the API and be non-null.
    // Today this fails because the method does not exist on ChangelogScanTask / AddedRowsScanTask.
    assertThat(task.rowId())
        .as(
            "_row_id (firstRowId) must be exposed via the changelog task API for v3+ tables. "
                + "It is currently inaccessible through the public interface even though "
                + "task.file().firstRowId() is non-null after manifest-list assignment.")
        .isNotNull();
  }

  // ---------------------------------------------------------------------------
  // Test 2: Row IDs are non-overlapping across consecutive snapshots.
  //
  // Each snapshot appends a data file with 1 record. Since nextRowId advances by the number
  // of added rows per snapshot, the two tasks must have different, non-overlapping row ID bases.
  //
  // FAILS today because rowId() does not exist on the task API.
  // ---------------------------------------------------------------------------
  @TestTemplate
  public void testRowIdsAreMonotonicallyIncreasingAcrossSnapshots() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    // FILE_A has recordCount=1, FILE_B has recordCount=1
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ChangelogScanTask> tasks = plan(table.newIncrementalChangelogScan());

    assertThat(tasks).as("must have two tasks").hasSize(2);

    // Sort by snapshot ordinal so snap1 task comes first
    tasks.sort((a, b) -> Long.compare(a.commitSnapshotId(), b.commitSnapshotId()));

    AddedRowsScanTask taskSnap1 = (AddedRowsScanTask) tasks.get(0);
    AddedRowsScanTask taskSnap2 = (AddedRowsScanTask) tasks.get(1);

    assertThat(taskSnap1.commitSnapshotId()).isEqualTo(snap1.snapshotId());
    assertThat(taskSnap2.commitSnapshotId()).isEqualTo(snap2.snapshotId());

    long rowIdSnap1 = taskSnap1.rowId();
    long rowIdSnap2 = taskSnap2.rowId();

    // The first snapshot gets row IDs starting from 0 (table starts at nextRowId=0).
    assertThat(rowIdSnap1)
        .as("First snapshot row IDs must start at 0 for a fresh table")
        .isEqualTo(0L);

    // The second snapshot must start at nextRowId after the first snapshot consumed 1 ID.
    assertThat(rowIdSnap2)
        .as(
            "Second snapshot row IDs must follow immediately after first snapshot. "
                + "FILE_A has recordCount=1, so nextRowId advances by 1 after snap1.")
        .isEqualTo(rowIdSnap1 + FILE_A.recordCount());
  }

  // ---------------------------------------------------------------------------
  // Test 3: DeletedDataFileScanTask also exposes rowId for the deleted file.
  //
  // When a data file is removed from the table, the changelog scan produces a
  // DeletedDataFileScanTask. The file's original firstRowId must be accessible
  // through the task API — this lets downstream consumers know which row ID range
  // was logically deleted.
  //
  // FAILS today because rowId() does not exist on the task API.
  // ---------------------------------------------------------------------------
  @TestTemplate
  public void testDeletedDataFileScanTaskExposesRowId() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();

    List<ChangelogScanTask> tasks =
        plan(
            table
                .newIncrementalChangelogScan()
                .fromSnapshotExclusive(snap1.snapshotId())
                .toSnapshot(snap2.snapshotId()));

    assertThat(tasks).as("must have one delete task for FILE_A").hasSize(1);
    assertThat(tasks.get(0)).isInstanceOf(DeletedDataFileScanTask.class);

    DeletedDataFileScanTask task = (DeletedDataFileScanTask) tasks.get(0);

    // After the fix: deleted file's row ID must be accessible through the task API.
    assertThat(task.rowId())
        .as(
            "DeletedDataFileScanTask must expose the original firstRowId of the deleted file. "
                + "This allows downstream consumers to know which row ID range was removed.")
        .isNotNull();

    // The row ID must match what was assigned to FILE_A when it was appended.
    // For a fresh table starting at nextRowId=0, FILE_A gets rowId=0.
    assertThat(task.rowId())
        .as("Deleted file row ID must match the row ID assigned during the append snapshot")
        .isEqualTo(0L);
  }

  // ---------------------------------------------------------------------------
  // Test 4: Changelog schema includes _row_id and _last_updated_sequence_number on v3+ tables.
  //
  // ChangelogUtil.changelogSchema() only adds CHANGE_TYPE, CHANGE_ORDINAL, COMMIT_SNAPSHOT_ID.
  // For v3+ tables, ROW_ID and LAST_UPDATED_SEQUENCE_NUMBER must also be in the schema so that
  // Spark and other engines know to request and emit them.
  //
  // FAILS today because ChangelogUtil.CHANGELOG_METADATA is missing ROW_ID and
  // LAST_UPDATED_SEQUENCE_NUMBER.
  // ---------------------------------------------------------------------------
  @TestTemplate
  public void testChangelogSchemaIncludesRowLineageColumnsOnV3Table() throws Exception {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    Schema changelogSchema = ChangelogUtil.changelogSchema(table.schema(), formatVersion);

    assertThat(changelogSchema.findField(MetadataColumns.ROW_ID.name()))
        .as(
            "Changelog schema must include _row_id for v3+ tables. "
                + "Currently ChangelogUtil.changelogSchema() only joins CHANGE_TYPE, "
                + "CHANGE_ORDINAL, COMMIT_SNAPSHOT_ID — ROW_ID is never added.")
        .isNotNull();

    assertThat(changelogSchema.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as(
            "Changelog schema must include _last_updated_sequence_number for v3+ tables. "
                + "This column allows consumers to identify which rows were touched in each "
                + "snapshot without full row comparisons.")
        .isNotNull();
  }

  // ---------------------------------------------------------------------------
  // Test 5: On v1/v2 tables, row lineage columns are absent from the changelog schema.
  //
  // Row lineage is a v3+ concept. The changelog schema for v1/v2 tables must NOT include
  // _row_id or _last_updated_sequence_number. This test guards against accidentally
  // emitting null row ID columns on tables that don't support row lineage.
  //
  // PASSES today (both columns absent for all versions) — becomes a regression guard
  // after the fix to ensure v3-only behaviour is not applied to v1/v2 tables.
  // ---------------------------------------------------------------------------
  @TestTemplate
  public void testChangelogSchemaExcludesRowLineageColumnsOnPreV3Table() throws Exception {
    assumeThat(formatVersion).isLessThan(3);

    Schema changelogSchema = ChangelogUtil.changelogSchema(table.schema(), formatVersion);

    assertThat(changelogSchema.findField(MetadataColumns.ROW_ID.name()))
        .as("v1/v2 changelog schema must NOT include _row_id")
        .isNull();

    assertThat(changelogSchema.findField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
        .as("v1/v2 changelog schema must NOT include _last_updated_sequence_number")
        .isNull();
  }

  // ---- helpers ----

  private List<ChangelogScanTask> plan(IncrementalChangelogScan scan) throws Exception {
    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      return Lists.newArrayList(tasks).stream().collect(Collectors.toList());
    }
  }
}
