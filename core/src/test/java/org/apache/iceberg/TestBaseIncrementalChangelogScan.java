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
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ComparisonChain;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
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

  @BeforeEach
  public void enableChangelogDeleteFiles() {
    table
        .updateProperties()
        .set(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
        .commit();
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
  public void testPositionDeletesOnFileWithPreExistingPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: Add FILE_A and FILE_B with position deletes on FILE_A (before the scan range)
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add more position deletes for FILE_A within the scan range
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have the newly added position delete")
        .extracting(DeleteFile::location)
        .containsExactly(newFileADeletes.location());

    // Pre-range position deletes must be attached as existing deletes even when there are no
    // equality deletes in the range, so previously deleted rows are not emitted again
    assertThat(task.existingDeletes())
        .as("Must include position deletes from before the scan range")
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testExistingDeletesOutsideAffectedPartitions() {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: FILE_A (bucket 0) and FILE_B (bucket 1), position deletes on both
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: a new position delete for FILE_A only
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-3.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // FILE_B is untouched in the range: its pre-range deletes are outside the affected scope
    // and must not surface, while FILE_A's pre-range deletes must still be attached
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have the new delete")
        .extracting(DeleteFile::location)
        .containsExactly(newFileADeletes.location());
    assertThat(task.existingDeletes())
        .as("Must have only FILE_A's pre-range delete")
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
  }

  @TestTemplate
  public void testExistingDVKeptForEqualityTriggeredTask() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A with a DV (before the scan range)
    table.newFastAppend().appendFile(FILE_A).commit();
    DeleteFile dv = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 2: an equality delete in FILE_A's partition (no file-scoped deletes added)
    table.newRowDelta().addDeletes(FILE_A2_DELETES).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have the equality delete")
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A2_DELETES.location());

    // The pre-range DV references a file that is not in the affected location set, but it lies
    // in a partition where a partition-scoped delete was added, so it must be kept to suppress
    // positions already deleted before the range
    assertThat(task.existingDeletes())
        .as("Must include the pre-range DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
  }

  @TestTemplate
  public void testDVOnExistingFile() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A and FILE_B
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add a DV for FILE_A
    DeleteFile dv = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have the added DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
    assertThat(task.existingDeletes()).as("Must have no existing deletes").isEmpty();
  }

  @TestTemplate
  public void testDVReplacementOnExistingFile() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();

    // Snapshot 2: Add a DV for FILE_A (before the scan range)
    DeleteFile dv1 = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Replace the DV — a new DV carries all previously deleted positions, so the
    // old DV must be removed in the same commit
    DeleteFile dv2 = newDV(FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .as("Must have the replacement DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());

    // The replaced DV must be attached as an existing delete: the new DV is cumulative, so
    // positions already deleted by the old DV must not be emitted as DELETE rows again
    assertThat(task.existingDeletes())
        .as("Must include the replaced DV to suppress previously deleted positions")
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());
  }

  @TestTemplate
  public void testDVReplacementWithinScanRange() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add a DV for FILE_A within the scan range
    DeleteFile dv1 = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Replace the DV within the scan range
    DeleteFile dv2 = newDV(FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.changeOrdinal()).as("Ordinal must match").isEqualTo(0);
    assertThat(task1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task1.addedDeletes())
        .as("Must have the first DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());
    assertThat(task1.existingDeletes()).as("Must have no existing deletes").isEmpty();

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.changeOrdinal()).as("Ordinal must match").isEqualTo(1);
    assertThat(task2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task2.addedDeletes())
        .as("Must have the replacement DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());
    assertThat(task2.existingDeletes())
        .as("Must include the DV replaced in this snapshot")
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());
  }

  @TestTemplate
  public void testDeletedFileWithDV() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();

    // Snapshot 2: Add a DV for FILE_A (before the scan range)
    DeleteFile dv = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Delete FILE_A entirely within the scan range
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    DeletedDataFileScanTask task = (DeletedDataFileScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.existingDeletes())
        .as("Must include the DV so previously deleted positions are not emitted")
        .extracting(DeleteFile::location)
        .containsExactly(dv.location());
  }

  @TestTemplate
  public void testAddedFileWithDVInSameSnapshot() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add FILE_B together with a DV on it (same-commit upsert pattern)
    DeleteFile dvB = newDV(FILE_B);
    table.newRowDelta().addRows(FILE_B).addDeletes(dvB).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);

    AddedRowsScanTask task = (AddedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap2.snapshotId());
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_B.location());
    assertThat(task.deletes())
        .as("Must have the DV added in the same snapshot")
        .extracting(DeleteFile::location)
        .containsExactly(dvB.location());
  }

  @TestTemplate
  public void testDVRewrittenByMidRangeReplaceSnapshot() {
    assumeThat(formatVersion).isEqualTo(3);

    // Snapshot 1: Add FILE_A with a DV (before the scan range)
    table.newFastAppend().appendFile(FILE_A).commit();
    DeleteFile dv1 = newDV(FILE_A);
    table.newRowDelta().addDeletes(dv1).commit();
    Snapshot rangeStart = table.currentSnapshot();

    // Snapshot 2: REPLACE snapshot rewrites the DV (e.g. delete compaction); changelog scans
    // skip REPLACE snapshots, so this removal/addition is never observed by planning
    DeleteFile dv1b = newDV(FILE_A);
    table.newRewrite().deleteFile(dv1).addFile(dv1b, rangeStart.sequenceNumber()).commit();
    Snapshot replaceSnap = table.currentSnapshot();

    // Snapshot 3: Replace the rewritten DV within the scan range
    DeleteFile dv2 = newDV(FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv1b)
        .addDeletes(dv2)
        .validateFromSnapshot(replaceSnap.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    // Snapshot 4: Replace the DV again
    DeleteFile dv3 = newDV(FILE_A);
    table
        .newRowDelta()
        .removeDeletes(dv2)
        .addDeletes(dv3)
        .validateFromSnapshot(snap3.snapshotId())
        .commit();
    Snapshot snap4 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(rangeStart.snapshotId()).toSnapshot(snap4.snapshotId());

    // Planning must not fail on multiple DV versions for the same file: the pre-range dv1 is
    // never observed as removed (its removal happened in the skipped REPLACE snapshot), so it
    // would collide with dv2 unless the newest DV per file wins
    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 2 tasks").hasSize(2);

    DeletedRowsScanTask task1 = (DeletedRowsScanTask) tasks.get(0);
    assertThat(task1.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap3.snapshotId());
    assertThat(task1.addedDeletes())
        .as("Must have the DV added in snapshot 3")
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());
    assertThat(task1.existingDeletes())
        .as("Must carry the content-equivalent pre-range DV")
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());

    DeletedRowsScanTask task2 = (DeletedRowsScanTask) tasks.get(1);
    assertThat(task2.commitSnapshotId()).as("Snapshot must match").isEqualTo(snap4.snapshotId());
    assertThat(task2.addedDeletes())
        .as("Must have the DV added in snapshot 4")
        .extracting(DeleteFile::location)
        .containsExactly(dv3.location());
    assertThat(task2.existingDeletes())
        .as("Must keep only the newest DV for the file")
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());
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
  public void testExistingDeletesWithStatsAreKeptWhenResidualsAreIgnored() {
    assumeThat(formatVersion).isEqualTo(2);

    // a pre-range equality delete on "id" whose bounds (id = 100) cannot match a filter of id = 1.
    // "id" is not a partition column, so only the delete file's own stats can prune this file
    int idFieldId = table.schema().findField("id").fieldId();
    ByteBuffer bound = Conversions.toByteBuffer(Types.IntegerType.get(), 100);
    DeleteFile existingEqDeletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofEqualityDeletes(idFieldId)
            .withPath("/path/to/existing-id-100-eq-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withMetrics(
                new Metrics(
                    1L, null, null, null, null, Map.of(idFieldId, bound), Map.of(idFieldId, bound)))
            .build();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(existingEqDeletes).commit();
    Snapshot snap1 = table.currentSnapshot();

    // an in-range delete on FILE_A, which turns FILE_A into a DeletedRowsScanTask
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    Expression filter = Expressions.equal("id", 1);

    // with residuals, the task filter still suppresses rows the pruned delete would have removed
    DeletedRowsScanTask withResiduals =
        (DeletedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .filter(filter)
                        .fromSnapshotExclusive(snap1.snapshotId())
                        .toSnapshot(snap2.snapshotId())));
    assertThat(withResiduals.residual())
        .as("Residual must re-apply the filter to emitted rows")
        .isEqualTo(filter);

    // without residuals, nothing re-applies the filter, so the existing delete must be retained
    DeletedRowsScanTask ignoringResiduals =
        (DeletedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .filter(filter)
                        .ignoreResiduals()
                        .fromSnapshotExclusive(snap1.snapshotId())
                        .toSnapshot(snap2.snapshotId())));
    assertThat(ignoringResiduals.residual())
        .as("Residual must be dropped")
        .isEqualTo(Expressions.alwaysTrue());
    assertThat(ignoringResiduals.addedDeletes())
        .as("Must have the newly added delete")
        .extracting(DeleteFile::location)
        .containsExactly(FILE_A_DELETES.location());
    assertThat(ignoringResiduals.existingDeletes())
        .as("Existing delete must not be pruned by its stats when residuals are ignored")
        .extracting(DeleteFile::location)
        .containsExactly(existingEqDeletes.location());
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

    // Position deletes on an EXISTING file produce a DeletedRowsScanTask, which must attach
    // deletes from before the scan range; verify the index was built lazily exactly once
    BaseIncrementalChangelogScan baseScan1 = (BaseIncrementalChangelogScan) scan1;
    assertThat(baseScan1.existingDeleteIndexBuildCount())
        .as("Should build existingDeleteIndex lazily exactly once for DeletedRowsScanTask")
        .isEqualTo(1);

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
    assertThat(baseScan2.existingDeleteIndexBuildCount())
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
    assertThat(baseScan.existingDeleteIndexBuildCount())
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
    assertThat(baseScan.existingDeleteIndexBuildCount())
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
    assertThat(baseScan.existingDeleteIndexBuildCount())
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
    assertThat(baseScan.existingDeleteIndexBuildCount())
        .as("Should call buildExistingDeleteIndex exactly once (lazily) even if result is empty")
        .isEqualTo(1);
    assertThat(baseScan.wasExistingDeleteIndexBuilt())
        .as(
            "Should build existingDeleteIndex when DELETED file is encountered, even with no existing deletes")
        .isTrue();
  }

  @TestTemplate
  public void testMixedDeleteManifestEntries() {
    assumeThat(formatVersion).isEqualTo(2);

    table
        .updateProperties()
        .set(MANIFEST_MIN_MERGE_COUNT, "1")
        .set(MANIFEST_MERGE_ENABLED, "true")
        .commit();

    // Snapshot 1: Add FILE_A
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Add Delete 1 for FILE_A
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: Add Delete 2 for FILE_A
    DeleteFile fileADeletes2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-2.parquet")
            .withFileSizeInBytes(10)
            .withPartition(FILE_A.partition())
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileADeletes2).commit();
    Snapshot snap3 = table.currentSnapshot();

    // We scan from snap2 to snap3 (meaning we only care about snap3's changes).
    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).isNotEmpty();

    // Find the task for snap3
    DeletedRowsScanTask snap3Task =
        tasks.stream()
            .filter(
                t -> t instanceof DeletedRowsScanTask && t.commitSnapshotId() == snap3.snapshotId())
            .map(t -> (DeletedRowsScanTask) t)
            .findFirst()
            .orElse(null);

    assertThat(snap3Task).isNotNull();
    assertThat(snap3Task.addedDeletes())
        .as("Must only contain the new delete from snap3")
        .hasSize(1)
        .extracting(DeleteFile::location)
        .containsExactly(fileADeletes2.location());
  }

  @TestTemplate
  public void testUnpartitionedEqualityDeletePruning() throws Exception {
    assumeThat(formatVersion).isEqualTo(2);

    // Create a new table specifically for this test, starting unpartitioned (Spec ID 0)
    String tableName =
        "test_unpartitioned_pruning_" + java.util.UUID.randomUUID().toString().replace("-", "");
    TestTables.TestTable localTable =
        TestTables.create(
            tableDir, tableName, SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    localTable
        .updateProperties()
        .set(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
        .commit();

    // Snapshot 1: Add fileA (unpartitioned, Spec ID 0)
    DataFile fileA =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withPath(localTable.location() + "/data-unpartitioned-a.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    localTable.newFastAppend().appendFile(fileA).commit();
    Snapshot snap1 = localTable.currentSnapshot();

    // Evolve spec to bucketed (Spec ID 1)
    localTable
        .updateSpec()
        .addField(org.apache.iceberg.expressions.Expressions.bucket("data", 16))
        .commit();
    PartitionSpec bucketedSpec = localTable.spec();

    // Snapshot 2: Add fileB (bucketed, Spec ID 1)
    String bucketFieldName = bucketedSpec.fields().get(0).name();
    DataFile fileB =
        DataFiles.builder(bucketedSpec)
            .withPath(localTable.location() + "/data-bucketed-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath(bucketFieldName + "=1")
            .withRecordCount(1)
            .build();
    localTable.newFastAppend().appendFile(fileB).commit();
    Snapshot snap2 = localTable.currentSnapshot();

    // Snapshot 3: Create an unpartitioned equality delete (Spec ID 0)
    int idFieldId = localTable.schema().findField("id").fieldId();
    DeleteFile globalDelete =
        FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
            .ofEqualityDeletes(idFieldId)
            .withPath(localTable.location() + "/global-delete.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    localTable.newRowDelta().addDeletes(globalDelete).commit();
    Snapshot snap3 = localTable.currentSnapshot();

    // Scan from snap1 exclusive to snap3 using localTable
    IncrementalChangelogScan scan =
        localTable
            .newIncrementalChangelogScan()
            .fromSnapshotExclusive(snap1.snapshotId())
            .toSnapshot(snap3.snapshotId());

    List<ChangelogScanTask> tasks = plan(scan);

    // Expecting 3 tasks:
    // 1 AddedRowsScanTask for fileB (Snap 2)
    // 2 DeletedRowsScanTask for fileA and fileB (Snap 3, since global delete disables pruning)
    assertThat(tasks).hasSize(3);

    long addedCount = tasks.stream().filter(t -> t instanceof AddedRowsScanTask).count();
    long deletedCount = tasks.stream().filter(t -> t instanceof DeletedRowsScanTask).count();

    assertThat(addedCount).isEqualTo(1);
    assertThat(deletedCount).isEqualTo(2);
  }

  @TestTemplate
  public void testPlanningIsProgressive() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    // Snapshot 1: FILE_A with position deletes (before the scan range)
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: a new position delete for FILE_A within the scan range
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-progressive.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());
    BaseIncrementalChangelogScan baseScan = (BaseIncrementalChangelogScan) scan;

    try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
      // Planning must be progressive: work that is only needed for emitted tasks (here, the
      // existing delete index behind the DeletedRowsScanTask) must not run until consumption
      assertThat(baseScan.wasExistingDeleteIndexBuilt())
          .as("Existing delete index must not be built before tasks are consumed")
          .isFalse();

      List<ChangelogScanTask> materialized = Lists.newArrayList(tasks);
      assertThat(materialized).as("Must have 1 task").hasSize(1);
      assertThat(baseScan.wasExistingDeleteIndexBuilt())
          .as("Existing delete index must be built once tasks are consumed")
          .isTrue();
    }
  }

  @TestTemplate
  public void testNoDataManifestReadsBeforeConsumption() {
    assumeThat(formatVersion).isEqualTo(2);

    // Pre-range: FILE_A in its own data manifest
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    String preRangeDataManifest = Iterables.getOnlyElement(snap1.dataManifests(table.io())).path();

    // In range: a position delete for FILE_A, whose DeletedRowsScanTask requires scanning the
    // live data manifests (including the pre-range one)
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-io-defer.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    // Planning must not read live data manifests; only consuming the tasks may
    withUnavailableLocations(
        ImmutableList.of(preRangeDataManifest),
        () -> assertThatCode(scan::planFiles).doesNotThrowAnyException());

    // With the manifest available again, consuming produces the expected task
    List<ChangelogScanTask> tasks = plan(scan);
    assertThat(tasks).as("Must have 1 task").hasSize(1);
    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.file().location()).as("Data file must match").isEqualTo(FILE_A.location());
    assertThat(task.addedDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(newFileADeletes.location());
  }

  @TestTemplate
  public void testUnaffectedExistingDeleteManifestsAreNotRead() {
    assumeThat(formatVersion).isEqualTo(2);

    // Two partitions with pre-range deletes committed separately, so each delete manifest
    // covers a single partition
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
    Snapshot rangeStart = table.currentSnapshot();
    String fileBDeleteManifest =
        rangeStart.deleteManifests(table.io()).stream()
            .filter(m -> m.snapshotId().equals(rangeStart.snapshotId()))
            .map(ManifestFile::path)
            .findFirst()
            .orElseThrow();

    // The range only touches FILE_A's partition
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-io-guard.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();
    Snapshot snap = table.currentSnapshot();

    // FILE_B's delete manifest is outside the affected scope: planning must succeed without
    // ever opening it
    withUnavailableLocations(
        ImmutableList.of(fileBDeleteManifest),
        () -> {
          IncrementalChangelogScan scan =
              newScan()
                  .fromSnapshotExclusive(rangeStart.snapshotId())
                  .toSnapshot(snap.snapshotId());

          List<ChangelogScanTask> tasks = plan(scan);

          assertThat(tasks).as("Must have 1 task").hasSize(1);
          DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
          assertThat(task.existingDeletes())
              .as("Must have only FILE_A's pre-range delete")
              .extracting(DeleteFile::location)
              .containsExactly(FILE_A_DELETES.location());
        });
  }

  @TestTemplate
  public void testAffectedPartitionPruningAfterSpecEvolution() {
    assumeThat(formatVersion).isEqualTo(2);

    // Two partitions with pre-range deletes committed separately, so each delete manifest
    // covers a single partition
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
    Snapshot rangeStart = table.currentSnapshot();
    String fileBDeleteManifest =
        rangeStart.deleteManifests(table.io()).stream()
            .filter(m -> m.snapshotId() != null && m.snapshotId() == rangeStart.snapshotId())
            .map(ManifestFile::path)
            .findFirst()
            .orElseThrow();

    // Evolve the spec (a metadata-only change, no new snapshot) so the range below can produce
    // affected partitions spanning both the old and the new spec
    table.updateSpec().addField("id").commit();

    // In range: a new delete on FILE_A (old spec, bucket 0) - affected partitions now contain
    // the old-spec bucket-0 tuple; nothing touches bucket 1
    DeleteFile newFileADeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-evolved.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newFileADeletes).commit();

    // In a separate in-range commit: a delete under the newly evolved spec. No data file lives
    // at this tuple, so it contributes no task, but it does add a new-spec tuple to the range's
    // affected partitions, so affected partitions now span two specs
    DeleteFile newSpecDeletes =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/data-evolved-spec-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0/id=1")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(newSpecDeletes).commit();
    Snapshot snap = table.currentSnapshot();

    // With per-spec pruning, the bucket-1 delete manifest must never be read even though the
    // range's affected partitions span two specs (the old-spec bucket-0 tuple and the new-spec
    // tuple)
    withUnavailableLocations(
        ImmutableList.of(fileBDeleteManifest),
        () -> {
          IncrementalChangelogScan scan =
              newScan()
                  .fromSnapshotExclusive(rangeStart.snapshotId())
                  .toSnapshot(snap.snapshotId());

          List<ChangelogScanTask> tasks = plan(scan);
          assertThat(tasks).as("Must have 1 task").hasSize(1);
          DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
          assertThat(task.existingDeletes())
              .as("Must have only FILE_A's pre-range delete")
              .extracting(DeleteFile::location)
              .containsExactly(FILE_A_DELETES.location());
        });
  }

  @TestTemplate
  public void testDeleteManifestsReadOnceDuringPlanning() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    CountingLocalFileIO countingIO = new CountingLocalFileIO();
    File location = java.nio.file.Files.createTempDirectory(temp, "counting-table").toFile();
    TestTables.TestTable countingTable =
        TestTables.create(
            location,
            "counting_changelog",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            new TestTables.TestTableOperations("counting_changelog", location, countingIO));
    countingTable
        .updateProperties()
        .set(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
        .commit();

    // Pre-range: FILE_A with position deletes
    countingTable.newFastAppend().appendFile(FILE_A).commit();
    countingTable.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot rangeStart = countingTable.currentSnapshot();
    String preRangeDeleteManifest =
        Iterables.getOnlyElement(rangeStart.deleteManifests(countingTable.io())).path();

    // In range: a new position delete for FILE_A
    DeleteFile inRangeDeletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-io-count.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    countingTable.newRowDelta().addDeletes(inRangeDeletes).commit();
    Snapshot snap = countingTable.currentSnapshot();
    String inRangeDeleteManifest =
        snap.deleteManifests(countingTable.io()).stream()
            .filter(m -> m.snapshotId().equals(snap.snapshotId()))
            .map(ManifestFile::path)
            .findFirst()
            .orElseThrow();

    // Count only planning-time reads, not commit-time reads
    countingIO.reset();

    IncrementalChangelogScan scan =
        countingTable
            .newIncrementalChangelogScan()
            .fromSnapshotExclusive(rangeStart.snapshotId())
            .toSnapshot(snap.snapshotId());
    List<ChangelogScanTask> tasks = plan(scan);
    assertThat(tasks).as("Must have 1 task").hasSize(1);

    assertThat(countingIO.opens(inRangeDeleteManifest))
        .as("Each in-range delete manifest must be read exactly once during planning")
        .isEqualTo(1);
    assertThat(countingIO.opens(preRangeDeleteManifest))
        .as("Each existing delete manifest must be read exactly once during planning")
        .isEqualTo(1);
  }

  @TestTemplate
  public void testDeleteManifestsReadOnceDuringPlanningWithDVs() throws IOException {
    assumeThat(formatVersion).isEqualTo(3);

    CountingLocalFileIO countingIO = new CountingLocalFileIO();
    File location = java.nio.file.Files.createTempDirectory(temp, "counting-table-dv").toFile();
    TestTables.TestTable countingTable =
        TestTables.create(
            location,
            "counting_changelog_dv",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            new TestTables.TestTableOperations("counting_changelog_dv", location, countingIO));
    countingTable
        .updateProperties()
        .set(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
        .commit();

    // Pre-range: FILE_A with a DV
    countingTable.newFastAppend().appendFile(FILE_A).commit();
    DeleteFile dv1 = FileGenerationUtil.generateDV(countingTable, FILE_A);
    countingTable.newRowDelta().addDeletes(dv1).commit();
    Snapshot rangeStart = countingTable.currentSnapshot();
    String preRangeDeleteManifest =
        Iterables.getOnlyElement(rangeStart.deleteManifests(countingTable.io())).path();

    // In range: replace the DV — the commit writes the added DV and the removed DV entry into
    // this snapshot's delete manifests, which planning must split in a single read
    DeleteFile dv2 = FileGenerationUtil.generateDV(countingTable, FILE_A);
    countingTable
        .newRowDelta()
        .removeDeletes(dv1)
        .addDeletes(dv2)
        .validateFromSnapshot(rangeStart.snapshotId())
        .commit();
    Snapshot snap = countingTable.currentSnapshot();
    List<String> inRangeDeleteManifests =
        snap.deleteManifests(countingTable.io()).stream()
            .filter(m -> m.snapshotId() != null && m.snapshotId() == snap.snapshotId())
            .map(ManifestFile::path)
            .toList();
    assertThat(inRangeDeleteManifests).isNotEmpty();

    // Count only planning-time reads, not commit-time reads
    countingIO.reset();

    IncrementalChangelogScan scan =
        countingTable
            .newIncrementalChangelogScan()
            .fromSnapshotExclusive(rangeStart.snapshotId())
            .toSnapshot(snap.snapshotId());
    List<ChangelogScanTask> tasks = plan(scan);

    assertThat(tasks).as("Must have 1 task").hasSize(1);
    DeletedRowsScanTask task = (DeletedRowsScanTask) Iterables.getOnlyElement(tasks);
    assertThat(task.addedDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(dv2.location());
    assertThat(task.existingDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(dv1.location());

    for (String manifest : inRangeDeleteManifests) {
      assertThat(countingIO.opens(manifest))
          .as("Each in-range delete manifest must be read exactly once during planning")
          .isEqualTo(1);
    }

    assertThat(countingIO.opens(preRangeDeleteManifest))
        .as("Each existing delete manifest must be read exactly once during planning")
        .isEqualTo(1);
  }

  @TestTemplate
  public void testTasksEmittedInChangeOrdinalOrderWithoutSorting() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: new data file + position delete on the existing FILE_A in one commit
    DeleteFile fileADeletes2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-order-1.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addRows(FILE_B).addDeletes(fileADeletes2).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: same shape again
    DeleteFile fileADeletes3 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-order-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addRows(FILE_C).addDeletes(fileADeletes3).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    // Deliberately NOT using the sorting plan(...) helper: the raw iteration order is the contract
    List<ChangelogScanTask> tasks;
    try (CloseableIterable<ChangelogScanTask> iterable = scan.planFiles()) {
      tasks = Lists.newArrayList(iterable);
    }

    assertThat(tasks).hasSize(4);
    assertThat(tasks.get(0)).isInstanceOf(AddedRowsScanTask.class);
    assertThat(tasks.get(0).changeOrdinal()).isEqualTo(0);
    assertThat(tasks.get(1)).isInstanceOf(DeletedRowsScanTask.class);
    assertThat(tasks.get(1).changeOrdinal()).isEqualTo(0);
    assertThat(tasks.get(2)).isInstanceOf(AddedRowsScanTask.class);
    assertThat(tasks.get(2).changeOrdinal()).isEqualTo(1);
    assertThat(tasks.get(3)).isInstanceOf(DeletedRowsScanTask.class);
    assertThat(tasks.get(3).changeOrdinal()).isEqualTo(1);
    assertThat(tasks.get(0).commitSnapshotId()).isEqualTo(snap2.snapshotId());
    assertThat(tasks.get(2).commitSnapshotId()).isEqualTo(snap3.snapshotId());
  }

  @TestTemplate
  public void testTasksEmittedInChangeOrdinalOrderWithoutSortingWithDVs() throws IOException {
    assumeThat(formatVersion).isEqualTo(3);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: new data file + DV on the existing FILE_A in one commit
    DeleteFile dvA1 = newDV(FILE_A);
    table.newRowDelta().addRows(FILE_B).addDeletes(dvA1).commit();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: same shape again — the new DV is cumulative, so the old one is replaced
    DeleteFile dvA2 = newDV(FILE_A);
    table
        .newRowDelta()
        .addRows(FILE_C)
        .removeDeletes(dvA1)
        .addDeletes(dvA2)
        .validateFromSnapshot(snap2.snapshotId())
        .commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    // Deliberately NOT using the sorting plan(...) helper: the raw iteration order is the contract
    List<ChangelogScanTask> tasks;
    try (CloseableIterable<ChangelogScanTask> iterable = scan.planFiles()) {
      tasks = Lists.newArrayList(iterable);
    }

    assertThat(tasks).hasSize(4);
    assertThat(tasks.get(0)).isInstanceOf(AddedRowsScanTask.class);
    assertThat(tasks.get(0).changeOrdinal()).isEqualTo(0);
    assertThat(tasks.get(1)).isInstanceOf(DeletedRowsScanTask.class);
    assertThat(tasks.get(1).changeOrdinal()).isEqualTo(0);
    assertThat(tasks.get(2)).isInstanceOf(AddedRowsScanTask.class);
    assertThat(tasks.get(2).changeOrdinal()).isEqualTo(1);
    assertThat(tasks.get(3)).isInstanceOf(DeletedRowsScanTask.class);
    assertThat(tasks.get(3).changeOrdinal()).isEqualTo(1);
    assertThat(tasks.get(0).commitSnapshotId()).isEqualTo(snap2.snapshotId());
    assertThat(tasks.get(2).commitSnapshotId()).isEqualTo(snap3.snapshotId());

    // DV replacement semantics hold in raw order too: the snap3 task attaches the replaced DV
    // as existing so only newly deleted positions are emitted
    DeletedRowsScanTask replacement = (DeletedRowsScanTask) tasks.get(3);
    assertThat(replacement.addedDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(dvA2.location());
    assertThat(replacement.existingDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(dvA1.location());
  }

  @TestTemplate
  public void testConsumingEarlySnapshotsDoesNotReadLaterSnapshotWork() {
    assumeThat(formatVersion).isEqualTo(2);

    // Pre-range: FILE_A in its own manifest
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();
    String preRangeDataManifest = Iterables.getOnlyElement(snap1.dataManifests(table.io())).path();

    // Snapshot 2: pure append (its task needs no pre-range manifests)
    table.newFastAppend().appendFile(FILE_B).commit();

    // Snapshot 3: position delete on FILE_A — planning ITS tasks must read the pre-range manifest
    DeleteFile fileADeletes2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-progressive-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileADeletes2).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap3.snapshotId());

    // With the pre-range manifest unavailable, planning and consuming ONLY snapshot 2's task
    // must succeed: snapshot 3's work is not touched until the walk reaches it
    withUnavailableLocations(
        ImmutableList.of(preRangeDataManifest),
        () -> {
          try (CloseableIterable<ChangelogScanTask> tasks = scan.planFiles()) {
            Iterator<ChangelogScanTask> iter = tasks.iterator();
            ChangelogScanTask first = iter.next();
            assertThat(first).isInstanceOf(AddedRowsScanTask.class);
            assertThat(((AddedRowsScanTask) first).file().location()).isEqualTo(FILE_B.location());
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });

    // Fully consumable once the manifest is back
    List<ChangelogScanTask> all = plan(scan);
    assertThat(all).hasSize(2);
  }

  @TestTemplate
  public void testReplanningUnpinnedScanRebuildsExistingDeleteIndex() {
    assumeThat(formatVersion).isEqualTo(2);

    // Pre-range: FILE_A (bucket 0) and FILE_B (bucket 1), each with its own position delete
    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    table.newRowDelta().addDeletes(FILE_B_DELETES).commit();
    Snapshot rangeStart = table.currentSnapshot();

    // First range touches only bucket 0, so the existing delete index is scoped to bucket 0
    DeleteFile fileADeletes2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-replan.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileADeletes2).commit();

    // the scan is not pinned to an end snapshot, so each planFiles() resolves a wider range
    IncrementalChangelogScan scan = newScan().fromSnapshotExclusive(rangeStart.snapshotId());

    List<ChangelogScanTask> firstPlan = plan(scan);
    assertThat(firstPlan).hasSize(1);
    assertThat(((DeletedRowsScanTask) firstPlan.get(0)).file().location())
        .isEqualTo(FILE_A.location());

    // Second range additionally touches bucket 1, whose existing delete was out of the first scope
    DeleteFile fileBDeletes2 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes-replan.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(fileBDeletes2).commit();

    List<ChangelogScanTask> secondPlan = plan(scan);
    assertThat(secondPlan).hasSize(2);

    DeletedRowsScanTask fileBTask =
        (DeletedRowsScanTask)
            secondPlan.stream()
                .filter(task -> path(task).equals(FILE_B.location()))
                .findFirst()
                .orElseThrow();

    assertThat(fileBTask.existingDeletes())
        .as("Replanning must rebuild the existing delete index for the wider range")
        .extracting(DeleteFile::location)
        .containsExactly(FILE_B_DELETES.location());
  }

  @TestTemplate
  public void testReAddedDeleteFileSurvivesLaterUnrelatedRemoval() {
    assumeThat(formatVersion).isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot rangeStart = table.currentSnapshot();

    String reusedPath = "/path/to/data-a-deletes-readded.parquet";
    DeleteFile delete1 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath(reusedPath)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    DeleteFile unrelatedDelete =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes-unrelated.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=1")
            .withRecordCount(1)
            .build();

    // s1: add a delete on FILE_A and an unrelated delete on FILE_B
    table.newRowDelta().addDeletes(delete1).addDeletes(unrelatedDelete).commit();

    // s2: remove the delete on FILE_A
    table.newRowDelta().removeDeletes(delete1).commit();

    // s3: re-add a delete file at the same path
    DeleteFile reAddedDelete1 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath(reusedPath)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(2)
            .build();
    table.newRowDelta().addDeletes(reAddedDelete1).commit();

    // s4: remove an unrelated delete file; the re-added one must not be evicted with it
    table.newRowDelta().removeDeletes(unrelatedDelete).commit();

    // s5: add another delete on FILE_A
    DeleteFile delete3 =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes-third.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(delete3).commit();
    Snapshot snap5 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(rangeStart.snapshotId()).toSnapshot(snap5.snapshotId());

    DeletedRowsScanTask lastTask =
        (DeletedRowsScanTask)
            plan(scan).stream()
                .filter(task -> task instanceof DeletedRowsScanTask)
                .filter(task -> task.commitSnapshotId() == snap5.snapshotId())
                .findFirst()
                .orElseThrow();

    assertThat(lastTask.addedDeletes())
        .extracting(DeleteFile::location)
        .containsExactly(delete3.location());
    assertThat(lastTask.existingDeletes())
        .as("A re-added delete file must survive a later removal of an unrelated delete file")
        .extracting(DeleteFile::location)
        .containsExactly(reusedPath);
  }

  @TestTemplate
  public void testDeletedRowsScanTaskColumnStats() {
    assumeThat(formatVersion).isEqualTo(2);

    DataFile fileWithStats = FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of(0));
    table.newFastAppend().appendFile(fileWithStats).commit();
    Snapshot rangeStart = table.currentSnapshot();

    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-stats-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(deletes).commit();
    Snapshot snap = table.currentSnapshot();

    DeletedRowsScanTask withStats =
        (DeletedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .includeColumnStats()
                        .fromSnapshotExclusive(rangeStart.snapshotId())
                        .toSnapshot(snap.snapshotId())));

    assertThat(withStats.file().lowerBounds())
        .as("Column stats must be returned when requested")
        .isNotNull()
        .isNotEmpty();
    assertThat(withStats.file().upperBounds()).isNotNull().isNotEmpty();

    DeletedRowsScanTask withoutStats =
        (DeletedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .fromSnapshotExclusive(rangeStart.snapshotId())
                        .toSnapshot(snap.snapshotId())));

    assertThat(withoutStats.file().lowerBounds())
        .as("Column stats must be dropped when not requested")
        .isNull();
    assertThat(withoutStats.file().upperBounds()).isNull();
  }

  @TestTemplate
  public void testDeletedRowsScanTaskColumnStatsForSubsetOfColumns() {
    assumeThat(formatVersion).isEqualTo(2);

    DataFile fileWithStats = FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of(0));
    table.newFastAppend().appendFile(fileWithStats).commit();
    Snapshot rangeStart = table.currentSnapshot();

    DeleteFile deletes =
        FileMetadata.deleteFileBuilder(SPEC)
            .ofPositionDeletes()
            .withPath("/path/to/data-stats-subset-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withRecordCount(1)
            .build();
    table.newRowDelta().addDeletes(deletes).commit();
    Snapshot snap = table.currentSnapshot();

    int idFieldId = table.schema().findField("id").fieldId();

    DeletedRowsScanTask task =
        (DeletedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .includeColumnStats(ImmutableList.of("id"))
                        .fromSnapshotExclusive(rangeStart.snapshotId())
                        .toSnapshot(snap.snapshotId())));

    assertThat(task.file().lowerBounds())
        .as("Only stats for the requested columns must be returned")
        .containsOnlyKeys(idFieldId);
    assertThat(task.file().upperBounds()).containsOnlyKeys(idFieldId);
  }

  @TestTemplate
  public void testAddedRowsScanTaskColumnStatsForSubsetOfColumns() {
    assumeThat(formatVersion).isEqualTo(2);

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot rangeStart = table.currentSnapshot();

    DataFile fileWithStats = FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of(0));
    table.newFastAppend().appendFile(fileWithStats).commit();
    Snapshot snap2 = table.currentSnapshot();

    int idFieldId = table.schema().findField("id").fieldId();

    AddedRowsScanTask task =
        (AddedRowsScanTask)
            Iterables.getOnlyElement(
                plan(
                    newScan()
                        .includeColumnStats(ImmutableList.of("id"))
                        .fromSnapshotExclusive(rangeStart.snapshotId())
                        .toSnapshot(snap2.snapshotId())));

    assertThat(task.file().lowerBounds())
        .as("Only stats for the requested columns must be returned")
        .containsOnlyKeys(idFieldId);
    assertThat(task.file().upperBounds()).containsOnlyKeys(idFieldId);
  }

  @TestTemplate
  public void testAppendOnlyRangeReadsDataManifestsOnce() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);

    CountingLocalFileIO countingIO = new CountingLocalFileIO();
    File location = java.nio.file.Files.createTempDirectory(temp, "append-only-table").toFile();
    TestTables.TestTable countingTable =
        TestTables.create(
            location,
            "append_only_changelog",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            new TestTables.TestTableOperations("append_only_changelog", location, countingIO));
    countingTable
        .updateProperties()
        .set(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
        .commit();

    countingTable.newFastAppend().appendFile(FILE_A).commit();
    Snapshot rangeStart = countingTable.currentSnapshot();

    countingTable.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = countingTable.currentSnapshot();
    String snap2DataManifest = newDataManifestPath(countingTable, snap2);

    countingTable.newFastAppend().appendFile(FILE_C).commit();
    Snapshot snap3 = countingTable.currentSnapshot();
    String snap3DataManifest = newDataManifestPath(countingTable, snap3);

    // Count only planning-time reads, not commit-time reads
    countingIO.reset();

    List<ChangelogScanTask> tasks =
        plan(
            countingTable
                .newIncrementalChangelogScan()
                .fromSnapshotExclusive(rangeStart.snapshotId())
                .toSnapshot(snap3.snapshotId()));

    assertThat(tasks).hasSize(2);
    assertThat(tasks).allMatch(AddedRowsScanTask.class::isInstance);
    assertThat(tasks).extracting(this::path).containsExactly(FILE_B.location(), FILE_C.location());

    assertThat(countingIO.opens(snap2DataManifest))
        .as("An append-only range must read each changed data manifest exactly once")
        .isEqualTo(1);
    assertThat(countingIO.opens(snap3DataManifest))
        .as("An append-only range must read each changed data manifest exactly once")
        .isEqualTo(1);
  }

  @TestTemplate
  public void testDeleteFilesRequireOptIn() {
    assumeThat(formatVersion).isEqualTo(2);

    // this test verifies default-off behavior: remove the opt-in the setup hook added
    table.updateProperties().remove(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES).commit();

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    assertThatThrownBy(scan::planFiles)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES);
  }

  @TestTemplate
  public void testPreRangeDeleteFilesRequireOptIn() {
    assumeThat(formatVersion).isEqualTo(2);

    table.updateProperties().remove(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES).commit();

    // deletes exist only BEFORE the range; DeletedDataFileScanTask would attach them
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot snap3 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap2.snapshotId()).toSnapshot(snap3.snapshotId());

    assertThatThrownBy(scan::planFiles)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES);
  }

  @TestTemplate
  public void testAppendOnlyRangeDoesNotRequireOptIn() {
    // runs on every format version: append-only changelogs must keep working with default settings
    table.updateProperties().remove(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES).commit();

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newFastAppend().appendFile(FILE_B).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan().fromSnapshotExclusive(snap1.snapshotId()).toSnapshot(snap2.snapshotId());

    assertThat(plan(scan)).hasSize(1);
  }

  @TestTemplate
  public void testScanOptionOverridesDeleteFileOptIn() {
    assumeThat(formatVersion).isEqualTo(2);

    table.updateProperties().remove(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES).commit();

    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan()
            .option(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "true")
            .fromSnapshotExclusive(snap1.snapshotId())
            .toSnapshot(snap2.snapshotId());

    assertThat(plan(scan)).hasSize(1);
  }

  @TestTemplate
  public void testScanOptionDisablesDeleteFileOptIn() {
    assumeThat(formatVersion).isEqualTo(2);

    // table property is ON via the setup hook; an explicit option=false must win
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snap1 = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot snap2 = table.currentSnapshot();

    IncrementalChangelogScan scan =
        newScan()
            .option(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES, "false")
            .fromSnapshotExclusive(snap1.snapshotId())
            .toSnapshot(snap2.snapshotId());

    assertThatThrownBy(scan::planFiles)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(TableProperties.CHANGELOG_SCAN_INCLUDE_DELETE_FILES);
  }

  private String newDataManifestPath(Table tbl, Snapshot snapshot) {
    return snapshot.dataManifests(tbl.io()).stream()
        .filter(manifest -> manifest.snapshotId().equals(snapshot.snapshotId()))
        .map(ManifestFile::path)
        .findFirst()
        .orElseThrow();
  }

  /** Counts how often each path is opened for reading, on top of local file IO. */
  private static class CountingLocalFileIO extends TestTables.LocalFileIO {
    private final Map<String, Integer> inputOpens = Maps.newConcurrentMap();

    @Override
    public org.apache.iceberg.io.InputFile newInputFile(String path) {
      inputOpens.merge(path, 1, Integer::sum);
      return super.newInputFile(path);
    }

    void reset() {
      inputOpens.clear();
    }

    int opens(String path) {
      return inputOpens.getOrDefault(path, 0);
    }
  }
}
