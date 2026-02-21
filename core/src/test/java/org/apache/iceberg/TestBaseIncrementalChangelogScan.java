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

import static org.apache.iceberg.TableProperties.MANIFEST_MERGE_ENABLED;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ComparisonChain;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseIncrementalChangelogScan
    extends ScanTestBase<
        IncrementalChangelogScan, ChangelogScanTask, ScanTaskGroup<ChangelogScanTask>> {

  @Override
  protected IncrementalChangelogScan newScan() {
    return table.newIncrementalChangelogScan();
  }

  @TestTemplate
  public void testDataFilters() {
    table.newFastAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();
    ManifestFile snap1DataManifest = Iterables.getOnlyElement(snap1.dataManifests(table.io()));

    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    assertThat(snap2.dataManifests(table.io())).as("Must be 2 data manifests").hasSize(2);

    withUnavailableLocations(
        ImmutableList.of(snap1DataManifest.path()),
        () -> {
          // bucket(k, 16) is 1 which is supposed to match only FILE_B
          IncrementalChangelogScan scan = newScan().filter(Expressions.equal("data", "k"));

          List<ChangelogScanTask> tasks = plan(scan);

          assertThat(tasks).as("Must have 1 task").hasSize(1);

          AddedRowsScanTask t1 = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
          assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
          assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
          assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
          assertThat(t1.deletes()).as("Must be no deletes").isEmpty();
        });
  }

  @TestTemplate
  public void testOverwrites() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newOverwrite().addFile(FILE_A2).deleteFile(FILE_A).commit();

    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    DeletedDataFileScanTask t2 = (DeletedDataFileScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(t2.existingDeletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testFileDeletes() {
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedDataFileScanTask t1 = (DeletedDataFileScanTask) Iterables.getOnlyElement(tasks);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(t1.existingDeletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testExistingEntriesInNewDataManifestsAreIgnored() {
    table
        .updateProperties()
        .set(MANIFEST_MIN_MERGE_COUNT, "1")
        .set(MANIFEST_MERGE_ENABLED, "true")
        .commit();

    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    Snapshot snap3 = table.currentSnapshot();

    ManifestFile manifest = Iterables.getOnlyElement(snap3.dataManifests(table.io()));
    assertThat(manifest.hasExistingFiles()).as("Manifest must have existing files").isTrue();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotInclusive(snap3.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    AddedRowsScanTask t1 = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_C.location());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testManifestRewritesAreIgnored() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    ManifestFile newManifest =
        writeManifest(
            "manifest-file.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, snap1.snapshotId(), FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, snap2.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();

    for (ManifestFile manifest : snap2.dataManifests(table.io())) {
      rewriteManifests.deleteManifest(manifest);
    }

    rewriteManifests.addManifest(newManifest);

    rewriteManifests.commit();

    table.newAppend().appendFile(FILE_C).commit();

    Snapshot snap4 = table.currentSnapshot();

    List<ChangelogScanTask> tasks = plan(newScan());

    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap1.snapshotId());
    assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
    assertThat(t2.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t3 = (AddedRowsScanTask) tasks.get(2);
    assertThat(t3.changeOrdinal()).as("Ordinal must match").isEqualTo(2);
    assertThat(t3.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap4.snapshotId());
    assertThat(t3.file().location()).as("Data file must match").isEqualTo(FILE_C.location());
    assertThat(t3.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testDataFileRewrites() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot snap1 = table.currentSnapshot();

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot snap2 = table.currentSnapshot();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A2)).commit();

    List<ChangelogScanTask> tasks = plan(newScan());

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask t1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(t1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(t1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap1.snapshotId());
    assertThat(t1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(t1.deletes()).as("Must be no deletes").isEmpty();

    AddedRowsScanTask t2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(t2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(t2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(t2.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
    assertThat(t2.deletes()).as("Must be no deletes").isEmpty();
  }

  @TestTemplate
  public void testPositionDeletesOnExistingFile() {
    assumeThat(formatVersion).isEqualTo(2);

    // Add initial data files
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Add position deletes for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have one DeletedRowsScanTask for FILE_A
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have added deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
    assertThat(task.existingDeletes()).as("Must have no existing deletes").isEmpty();
  }

  @TestTemplate
  public void testEqualityDeletesOnExistingFile() {
    assumeThat(formatVersion).isEqualTo(2);

    // Add initial data files
    table.newFastAppend().appendFile(FILE_A2).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Add equality deletes for FILE_A2
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have one DeletedRowsScanTask for FILE_A2
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task.addedDeletes())
        .as("Must have added deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    assertThat(task.existingDeletes()).as("Must have no existing deletes").isEmpty();
  }

  @TestTemplate
  public void testAddedFileWithExistingDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Add FILE_A with deletes
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Add FILE_B in the changelog range
    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have one AddedRowsScanTask for FILE_B (no deletes apply to FILE_B)
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    AddedRowsScanTask task = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
    assertThat(task.deletes()).as("Must have no deletes").isEmpty();
  }

  @TestTemplate
  public void testDeletedFileWithExistingDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();

    // Add deletes for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Delete FILE_A in the changelog range
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have one DeletedDataFileScanTask for FILE_A with existing deletes
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedDataFileScanTask task = (DeletedDataFileScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.existingDeletes())
        .as("Must have existing deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testMultipleSnapshotsWithDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A and FILE_B
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add deletes for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add FILE_C
    table.newFastAppend().appendFile(FILE_C).commit();
    Snapshot snap3 = table.currentSnapshot();

    // Snapshot 4: Add deletes for FILE_B
    DeleteFile fileBDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileBDeletes).commit();
    Snapshot snap4 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap4.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have:
    // 1. DeletedRowsScanTask for FILE_A (snap2)
    // 2. AddedRowsScanTask for FILE_C (snap3)
    // 3. DeletedRowsScanTask for FILE_B (snap4)
    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task1.addedDeletes()).as("Must have added deletes").hasSize(1);

    AddedRowsScanTask task2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_C.location());

    DeletedRowsScanTask task3 = (DeletedRowsScanTask) tasks.get(2);
    assertThat(task3.changeOrdinal()).as("Ordinal must match").isEqualTo(2);
    assertThat(task3.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
    assertThat(task3.addedDeletes()).as("Must have added deletes").hasSize(1);
  }

  @TestTemplate
  public void testInsertDeleteReinsert() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Insert FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add equality delete for FILE_A
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Re-insert FILE_A (same file, new snapshot)
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have:
    // 1. DeletedRowsScanTask for FILE_A affected by delete (snap2)
    // 2. AddedRowsScanTask for FILE_A re-insert (snap3)
    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task1.addedDeletes()).as("Must have added deletes").hasSize(1);

    AddedRowsScanTask task2 = (AddedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    // The re-inserted file is a fresh insert, so the deletes from snap2 were already
    // accounted for in the DeletedRowsScanTask above
    assertThat(task2.deletes()).as("Re-insert should not carry previous deletes").isEmpty();
  }

  @TestTemplate
  public void testInsertAndDeleteInSameCommit() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: baseline
    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add FILE_A and delete it in the same commit (using row delta)
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should emit an AddedRowsScanTask with deletes attached
    // The net result depends on whether all rows are deleted
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    AddedRowsScanTask task = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    // The delete should be attached to the added file task
    assertThat(task.deletes())
        .as("Must have deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testOverlappingEqualityAndPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add position deletes for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add equality deletes that also affect FILE_A
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have 2 DeletedRowsScanTask for FILE_A (one per snapshot with deletes)
    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task1.addedDeletes())
        .as("Must have 1 added delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
    assertThat(task1.existingDeletes()).as("Must have no existing deletes").isEmpty();

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task2.addedDeletes())
        .as("Must have 1 added delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    // The position delete from snap2 should be an existing delete for snap3
    assertThat(task2.existingDeletes())
        .as("Must be 1 position delete from previous snapshot")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testEqualityDeleteOverlapsEqualityDelete() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A2 (has equality delete support)
    table.newFastAppend().appendFile(FILE_A2).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add equality delete on field 'id'
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add equality delete on different field 'data' that matches the same logical rows
    // This tests behavior when two equality deletes on different columns target overlapping
    // rows
    DeleteFile eqDeleteOnData =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-data.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(eqDeleteOnData).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have 2 DeletedRowsScanTask for FILE_A2 (one per snapshot with deletes)
    // This documents current behavior - whether duplicate DELETE rows are emitted or deduplicated
    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task1.addedDeletes())
        .as("Must have first equality delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    assertThat(task1.existingDeletes()).as("Must have no existing deletes").isEmpty();

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task2.addedDeletes())
        .as("Must have second equality delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(eqDeleteOnData.location());
    // First equality delete should be an existing delete for the second task
    assertThat(task2.existingDeletes())
        .as("Must have first equality delete as existing")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
  }

  @TestTemplate
  public void testDeletedFileWithBothDeleteTypes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add position deletes for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add equality deletes for FILE_A (potentially overlapping)
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap3 = table.currentSnapshot();

    // Snapshot 4: Delete FILE_A entirely
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap4 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap4.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have:
    // 1. DeletedRowsScanTask for FILE_A with position deletes (snap2)
    // 2. DeletedRowsScanTask for FILE_A with equality deletes (snap3)
    // 3. DeletedDataFileScanTask for FILE_A deletion (snap4) with both types of existing deletes
    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task1.addedDeletes())
        .as("Must have position deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
    assertThat(task1.existingDeletes()).as("Must have no existing deletes").isEmpty();

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task2.addedDeletes())
        .as("Must have equality deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    // Position delete from snap2 should be an existing delete for snap3
    assertThat(task2.existingDeletes())
        .as("Must have position delete as existing")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());

    DeletedDataFileScanTask task3 = (DeletedDataFileScanTask) tasks.get(2);
    assertThat(task3.changeOrdinal()).as("Ordinal must match").isEqualTo(2);
    assertThat(task3.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    // When file is deleted, all existing deletes should be included to omit previously deleted rows
    assertThat(task3.existingDeletes())
        .as("Must have both position and equality deletes as existing")
        .hasSize(2)
        .extracting(DeleteFile::location)
        .containsExactlyInAnyOrder(FILE_A_DELETES.location(), FILE_A2_DELETES.location());
  }

  @TestTemplate
  public void testMultipleEqualityDeletesSameFile() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A2
    table.newFastAppend().appendFile(FILE_A2).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add equality delete #1 on field 'id'
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add equality delete #2 on field 'id' (different values, no overlap)
    DeleteFile eqDelete2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(eqDelete2).commit();
    Snapshot snap3 = table.currentSnapshot();

    // Snapshot 4: Add equality delete #3 on field 'data' (potentially overlaps with delete #1 or
    // #2)
    DeleteFile eqDelete3 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-3.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(eqDelete3).commit();
    Snapshot snap4 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap4.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have 3 DeletedRowsScanTask for FILE_A2 (one per snapshot with deletes)
    // This documents cumulative equality delete tracking across multiple snapshots
    assertThat(tasks).as("Must have 3 tasks").hasSize(3);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task1.addedDeletes())
        .as("Must have first equality delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    assertThat(task1.existingDeletes()).as("Must have no existing deletes").isEmpty();

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task2.addedDeletes())
        .as("Must have second equality delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(eqDelete2.location());
    // First equality delete should be an existing delete for the second task
    assertThat(task2.existingDeletes())
        .as("Must have first equality delete as existing")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());

    DeletedRowsScanTask task3 = (DeletedRowsScanTask) tasks.get(2);
    assertThat(task3.changeOrdinal()).as("Ordinal must match").isEqualTo(2);
    assertThat(task3.file().location()).as("Data file must match").isEqualTo(FILE_A2.location());
    assertThat(task3.addedDeletes())
        .as("Must have third equality delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(eqDelete3.location());
    // Both previous equality deletes should be existing deletes for the third task
    assertThat(task3.existingDeletes())
        .as("Must have both previous equality deletes as existing")
        .hasSize(2)
        .extracting(DeleteFile::location)
        .containsExactlyInAnyOrder(FILE_A2_DELETES.location(), eqDelete2.location());
  }

  @TestTemplate
  public void testExistingAndNewDeletesBothApplied() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A with position deletes
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add FILE_B
    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add equality deletes affecting FILE_A
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have:
    // 1. AddedRowsScanTask for FILE_B (snap2)
    // 2. DeletedRowsScanTask for FILE_A with new equality delete (snap3)
    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask task1 = (AddedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.file().location()).as("Data file must match").isEqualTo(FILE_B.location());

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task2.addedDeletes())
        .as("Must have newly added delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());
    assertThat(task2.existingDeletes())
        .as("Must have existing delete")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testOverwriteSnapshotWithExistingDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A and FILE_B with deletes on FILE_A
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Overwrite - replace FILE_A with FILE_A2, keep FILE_B
    table.newOverwrite().addFile(FILE_A2).deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    // Should not throw NPE and should handle the overwrite correctly
    List<ChangelogScanTask> tasks = plan(scan);

    // The overwrite creates ADDED and DELETED tasks
    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    AddedRowsScanTask addedTask = (AddedRowsScanTask) tasks.get(0);
    assertThat(addedTask.file().location())
        .as("Added file must match")
        .isEqualTo(FILE_A2.location());

    DeletedDataFileScanTask deletedTask = (DeletedDataFileScanTask) tasks.get(1);
    assertThat(deletedTask.file().location())
        .as("Deleted file must match")
        .isEqualTo(FILE_A.location());
    // FILE_A had existing deletes which should be included
    assertThat(deletedTask.existingDeletes())
        .as("Must have existing deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testDeletedFileWithPreScanRangeDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();

    // Snapshot 2: Add deletes for FILE_A (before scan range)
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Delete FILE_A entirely (within scan range)
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    // Scan from snap2 (exclusive) to snap3
    // This means the delete of FILE_A happens within the range,
    // but the delete file (FILE_A_DELETES) was added before the range
    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have one DeletedDataFileScanTask for FILE_A
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedDataFileScanTask task = (DeletedDataFileScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());

    // The key assertion: existingDeletes should include FILE_A_DELETES
    // so consumers know to omit those previously deleted rows
    assertThat(task.existingDeletes())
        .as("Must include pre-existing deletes to omit previously deleted rows")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testLargeDeleteSetWithPruning() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A in partition 0
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add large equality delete set, but only affecting partition 0
    // Create multiple equality delete files for different partitions
    DeleteFile eqDelete1 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-1.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(100)
            .build();

    DeleteFile eqDelete2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(100)
            .build();

    DeleteFile eqDelete3 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes(1)
            .withPath("/path/to/eq-delete-3.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2")
            .withRecordCount(100)
            .build();

    table.newRowDelta().addDeletes(eqDelete1).addDeletes(eqDelete2).addDeletes(eqDelete3).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have 1 DeletedRowsScanTask for FILE_A
    // Only eqDelete1 should be included (partition 0), not eqDelete2 or eqDelete3
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    // Should only have 1 delete file (the one in partition 0)
    // The DeleteFileIndex should prune out the other partition's delete files
    assertThat(task.addedDeletes())
        .as("Must have only 1 delete (partition pruning should eliminate others)")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(eqDelete1.location());
  }

  // plans tasks and reorders them to have deterministic order
  private List<ChangelogScanTask> plan(IncrementalChangelogScan scan) {
    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      List<ChangelogScanTask> tasksAsList = Lists.newArrayList(tasks);
      tasksAsList.sort(taskComparator());
      return tasksAsList;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @TestTemplate
  public void testDeleteFilePartitionPruning() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A (partition 0)
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add delete files for FILE_A (partition 0) and FILE_C (partition 2)
    // FILE_C is not present as a data file, only its delete file
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_C2_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Scan without filter
    // Partition pruning will automatically exclude FILE_C2_DELETES because:
    // 1. There's no data file in partition 2 (FILE_C doesn't exist)
    // 2. The delete file's partition doesn't overlap with any existing data files
    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Should have a DeletedRowsScanTask for FILE_A only
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());

    // Verify that only FILE_A_DELETES is included (partition 0)
    // FILE_C2_DELETES should have been pruned because:
    // - Its partition (data_bucket=2) doesn't contain any data files
    // - The partition pruning optimization skips it during accumulation
    assertThat(task.addedDeletes())
        .as("Must have added deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  private Comparator<? super ChangelogScanTask> taskComparator() {
    return (t1, t2) ->
        ComparisonChain.start()
            .compare(t1.changeOrdinal(), t2.changeOrdinal())
            .compare(t1.getClass().getName(), t2.getClass().getName())
            .compare(path(t1), path(t2))
            .result();
  }

  private String path(ChangelogScanTask task) {
    return ((ContentScanTask<?>) task).file().location().toString();
  }

  @TestTemplate
  public void testLazyExistingDeleteIndexAppendOnly() {
    assumeThat(formatVersion).isEqualTo(2);

    // Scenario 1: Append-only with position deletes (no equality deletes, no DELETED files)
    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add position deletes for FILE_A
    // This creates a DeletedRowsScanTask for EXISTING file FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan1 =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks1 = plan(scan1);

    // Verify no errors and correct results
    assertThat(tasks1).isNotEmpty();

    // Verify existingDeleteIndex was NOT built (position deletes don't require it)
    BaseIncrementalChangelogScan baseScan1 = (BaseIncrementalChangelogScan) scan1;
    assertThat(baseScan1.getExistingDeleteIndexBuildCallCount())
        .as(
            "Should not call buildExistingDeleteIndex for position deletes without equality deletes/DELETED files")
        .isEqualTo(0);
    assertThat(baseScan1.wasExistingDeleteIndexBuilt())
        .as(
            "Should not build existingDeleteIndex for position deletes without equality deletes/DELETED files")
        .isFalse();

    // Scenario 2: Pure append-only (no deletes at all, no DELETED files)
    // Snapshot 3: Add FILE_B (pure append, no deletes)
    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan2 =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks2 = plan(scan2);

    // Verify correct results
    assertThat(tasks2).hasSize(1);
    AddedRowsScanTask task = (AddedRowsScanTask) tasks2.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_B.location());

    // Verify existingDeleteIndex was NOT built (pure append, no deletes, no DELETED files)
    BaseIncrementalChangelogScan baseScan2 = (BaseIncrementalChangelogScan) scan2;
    assertThat(baseScan2.getExistingDeleteIndexBuildCallCount())
        .as("Should not call buildExistingDeleteIndex for pure append-only workload")
        .isEqualTo(0);
    assertThat(baseScan2.wasExistingDeleteIndexBuilt())
        .as("Should not build existingDeleteIndex for pure append-only workload")
        .isFalse();
  }

  @TestTemplate
  public void testLazyExistingDeleteIndexWithEqualityDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add equality deletes for FILE_A (triggers early building)
    DeleteFile eqDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes()
            .withPath("/path/to/eq-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartition(FILE_A.partition())
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(eqDeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Verify correct results
    assertThat(tasks).isNotEmpty();
    DeletedRowsScanTask task = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());

    // Verify existingDeleteIndex was built EARLY (for equality deletes, not lazily)
    BaseIncrementalChangelogScan baseScan = (BaseIncrementalChangelogScan) scan;
    assertThat(baseScan.getExistingDeleteIndexBuildCallCount())
        .as("Should call buildExistingDeleteIndex exactly once for equality deletes")
        .isEqualTo(1);
    assertThat(baseScan.wasExistingDeleteIndexBuilt())
        .as("Should build existingDeleteIndex early when equality deletes exist")
        .isTrue();
  }

  @TestTemplate
  public void testLazyExistingDeleteIndexWithDeletedFiles() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();

    // Snapshot 2: Add deletes for FILE_A (before scan range)
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Delete FILE_A entirely (within scan range, no equality deletes)
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Verify correct results
    assertThat(tasks).hasSize(1);
    DeletedDataFileScanTask task = (DeletedDataFileScanTask) tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.existingDeletes())
        .as("Must include pre-existing deletes")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());

    // Verify existingDeleteIndex was built LAZILY (on-demand for DELETED file)
    BaseIncrementalChangelogScan baseScan = (BaseIncrementalChangelogScan) scan;
    assertThat(baseScan.getExistingDeleteIndexBuildCallCount())
        .as("Should call buildExistingDeleteIndex exactly once (lazily) for DELETED file")
        .isEqualTo(1);
    assertThat(baseScan.wasExistingDeleteIndexBuilt())
        .as("Should build existingDeleteIndex lazily when DELETED file is encountered")
        .isTrue();
  }

  @TestTemplate
  public void testLazyExistingDeleteIndexWithBothEqualityDeletesAndDeletedFiles() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A and FILE_B
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add equality deletes for FILE_A (triggers early building)
    DeleteFile eqDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofEqualityDeletes()
            .withPath("/path/to/eq-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartition(FILE_A.partition())
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(eqDeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Delete FILE_B (different file) - this will trigger Supplier.get()
    table.newDelete().deleteFile(FILE_B).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Verify correct results
    assertThat(tasks).isNotEmpty();

    // This proves that the cached index was reused when DELETED file was encountered
    BaseIncrementalChangelogScan baseScan = (BaseIncrementalChangelogScan) scan;
    assertThat(baseScan.getExistingDeleteIndexBuildCallCount())
        .as(
            "Should call buildExistingDeleteIndex exactly once (early), then reuse cached index for DELETED file")
        .isEqualTo(1);
    assertThat(baseScan.wasExistingDeleteIndexBuilt())
        .as(
            "Should build existingDeleteIndex early when equality deletes exist, even with DELETED files")
        .isTrue();

    // ensure DELETED file task has correct deletes
    DeletedDataFileScanTask deletedTask =
        tasks.stream()
            .filter(t -> t instanceof DeletedDataFileScanTask)
            .map(t -> (DeletedDataFileScanTask) t)
            .findFirst()
            .orElse(null);
    if (deletedTask != null) {
      assertThat(deletedTask.file().location()).isEqualTo(FILE_B.location());
    }
  }

  @TestTemplate
  public void testLazyExistingDeleteIndexNoExistingDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A (before scan range)
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Delete FILE_A (within scan range, no existing deletes, no equality deletes)
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Verify correct results
    assertThat(tasks).hasSize(1);
    DeletedDataFileScanTask task = (DeletedDataFileScanTask) tasks.get(0);
    assertThat(task.file().location()).isEqualTo(FILE_A.location());
    assertThat(task.existingDeletes()).isEmpty();

    // Verify existingDeleteIndex was built (lazily for DELETED file, even though no existing
    // deletes)
    // Note: It will be built but will be empty
    BaseIncrementalChangelogScan baseScan = (BaseIncrementalChangelogScan) scan;
    assertThat(baseScan.getExistingDeleteIndexBuildCallCount())
        .as("Should call buildExistingDeleteIndex exactly once (lazily) even if result is empty")
        .isEqualTo(1);
    assertThat(baseScan.wasExistingDeleteIndexBuilt())
        .as(
            "Should build existingDeleteIndex when DELETED file is encountered, even with no existing deletes")
        .isTrue();
  }
}
