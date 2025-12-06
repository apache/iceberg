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
import static org.assertj.core.api.Assertions.fail;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRowLineageAssignment {
  public static final Schema SCHEMA =
      new Schema(
          NestedField.required(3, "id", Types.IntegerType.get()),
          NestedField.required(4, "data", Types.StringType.get()));

  static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(125)
          .build();

  static final DeleteFile FILE_A_DV =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.puffin")
          .withFileSizeInBytes(10)
          .withRecordCount(15)
          .withReferencedDataFile(FILE_A.location())
          .withContentOffset(4)
          .withContentSizeInBytes(35)
          .build();

  static final DeleteFile FILE_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(15)
          .withReferencedDataFile(FILE_A.location())
          .build();

  static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(100)
          .build();

  static final DataFile FILE_C =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-c.parquet")
          .withFileSizeInBytes(100)
          .withRecordCount(90)
          .build();

  @TempDir private File location;

  private BaseTable table;

  @BeforeEach
  public void createTable() {
    // create a table that uses random snapshot IDs so that conflicts can be tested. otherwise,
    // conflict cases use the same snapshot ID that is suppressed by the TableMetadata builder.
    this.table =
        TestTables.create(
            location,
            "test",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            3,
            Map.of("random-snapshot-ids", "true"));
  }

  @AfterEach
  public void cleanup() {
    TestTables.clearTables();
  }

  @Test
  public void testSingleFileAppend() {
    assertThat(table.operations().current().nextRowId()).isEqualTo(0L);

    table.newAppend().appendFile(FILE_A).commit();
    Snapshot current = table.currentSnapshot();
    assertThat(current.firstRowId()).isEqualTo(0L);
    assertThat(table.operations().current().nextRowId()).isEqualTo(FILE_A.recordCount());
    checkManifestListAssignment(table.io().newInputFile(current.manifestListLocation()), 0L);

    ManifestFile manifest = Iterables.getOnlyElement(current.dataManifests(table.io()));
    checkDataFileAssignment(table, manifest, 0L);
  }

  @Test
  public void testOverrideFirstRowId() {
    assertThat(table.operations().current().nextRowId()).isEqualTo(0L);

    // commit a file with first_row_id set
    DataFile withFirstRowId =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .copy(FILE_A)
            .withFirstRowId(1_000L)
            .build();

    table.newAppend().appendFile(withFirstRowId).commit();
    Snapshot current = table.currentSnapshot();
    assertThat(current.firstRowId()).isEqualTo(0L);
    assertThat(table.operations().current().nextRowId()).isEqualTo(withFirstRowId.recordCount());
    checkManifestListAssignment(table.io().newInputFile(current.manifestListLocation()), 0L);

    // first_row_id should be overridden by metadata assignment
    ManifestFile manifest = Iterables.getOnlyElement(current.dataManifests(table.io()));
    checkDataFileAssignment(table, manifest, 0L);
  }

  @Test
  public void testBranchAssignment() {
    // start with a single file in the table
    testSingleFileAppend();

    long startingCurrentSnapshot = table.currentSnapshot().snapshotId();
    long startingNextRowId = table.operations().current().nextRowId();

    // commit to a branch
    table.newAppend().appendFile(FILE_B).toBranch("branch").commit();
    // branch data manifests: [added(FILE_B)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount());
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(startingCurrentSnapshot);

    long branchSnapshot = table.snapshot("branch").snapshotId();

    // commit to main
    table.newAppend().appendFile(FILE_C).commit();
    // main data manifests: [added(FILE_C)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount() + FILE_C.recordCount());
    assertThat(table.snapshot("branch").snapshotId()).isEqualTo(branchSnapshot);

    // validate the commit to the branch
    checkManifestListAssignment(
        table.io().newInputFile(table.snapshot("branch").manifestListLocation()),
        startingNextRowId,
        0L);

    List<ManifestFile> branchManifests = table.snapshot("branch").dataManifests(table.io());
    checkDataFileAssignment(table, branchManifests.get(0), startingNextRowId);
    checkDataFileAssignment(table, branchManifests.get(1), 0L);

    // validate the commit to main
    checkManifestListAssignment(
        table.io().newInputFile(table.currentSnapshot().manifestListLocation()),
        startingNextRowId + FILE_B.recordCount(),
        0L);

    List<ManifestFile> mainManifests = table.currentSnapshot().dataManifests(table.io());
    checkDataFileAssignment(table, mainManifests.get(0), startingNextRowId + FILE_B.recordCount());
    checkDataFileAssignment(table, mainManifests.get(1), 0L);
  }

  @Test
  public void testCherryPickReassignsRowIds() {
    // start with a commit in a branch that diverges from main
    testBranchAssignment();

    long startingNextRowId = table.operations().current().nextRowId();
    // first row ID for C is the sum of rows in FILE_A and FILE_B because it was committed last
    long firstRowIdFileC = FILE_A.recordCount() + FILE_B.recordCount();

    // cherry-pick the commit to the main branch
    table.manageSnapshots().cherrypick(table.snapshot("branch").snapshotId()).commit();
    // main data manifests: [added(FILE_B)], [added(FILE_C)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount());
    checkManifestListAssignment(
        table.io().newInputFile(table.currentSnapshot().manifestListLocation()),
        startingNextRowId,
        firstRowIdFileC,
        0L);

    List<ManifestFile> mainManifests = table.currentSnapshot().dataManifests(table.io());
    checkDataFileAssignment(table, mainManifests.get(0), startingNextRowId);
    checkDataFileAssignment(table, mainManifests.get(1), firstRowIdFileC);
    checkDataFileAssignment(table, mainManifests.get(2), 0L);
  }

  @Test
  public void testFastForwardPreservesRowIds() {
    // start with a single file in the table
    testSingleFileAppend();

    long startingCurrentSnapshot = table.currentSnapshot().snapshotId();
    long startingNextRowId = table.operations().current().nextRowId();

    // commit to a branch
    table.newAppend().appendFile(FILE_B).toBranch("branch").commit();
    // branch data manifests: [added(FILE_B)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount());
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(startingCurrentSnapshot);

    // add a second commit to the branch
    table.newAppend().appendFile(FILE_C).toBranch("branch").commit();
    // branch data manifests: [added(FILE_C)], [added(FILE_B)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount() + FILE_C.recordCount());
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(startingCurrentSnapshot);

    long branchSnapshot = table.snapshot("branch").snapshotId();

    // fast-forward main to the branch
    table.manageSnapshots().fastForwardBranch("main", "branch").commit();
    // branch data manifests: [added(FILE_C)], [added(FILE_B)], [added(FILE_A)]

    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_B.recordCount() + FILE_C.recordCount());
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(branchSnapshot);

    // validate that the branches have the same first_row_id assignments
    for (String branch : List.of("main", "branch")) {
      checkManifestListAssignment(
          table.io().newInputFile(table.snapshot(branch).manifestListLocation()),
          startingNextRowId + FILE_B.recordCount(),
          startingNextRowId,
          0L);

      List<ManifestFile> branchManifests = table.snapshot("branch").dataManifests(table.io());
      checkDataFileAssignment(
          table, branchManifests.get(0), startingNextRowId + FILE_B.recordCount());
      checkDataFileAssignment(table, branchManifests.get(1), startingNextRowId);
      checkDataFileAssignment(table, branchManifests.get(2), 0L);
    }

    // validate that the branches have the same manifests
    assertThat(table.currentSnapshot().dataManifests(table.io()))
        .isEqualTo(table.snapshot("branch").dataManifests(table.io()));
  }

  @Test
  public void testMultiFileAppend() {
    assertThat(table.operations().current().nextRowId()).isEqualTo(0L);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    Snapshot current = table.currentSnapshot();
    assertThat(current.firstRowId()).isEqualTo(0L);
    assertThat(table.operations().current().nextRowId())
        .isEqualTo(FILE_A.recordCount() + FILE_B.recordCount());
    checkManifestListAssignment(table.io().newInputFile(current.manifestListLocation()), 0L);

    ManifestFile manifest = Iterables.getOnlyElement(current.dataManifests(table.io()));
    checkDataFileAssignment(table, manifest, 0L, FILE_A.recordCount());
  }

  @Test
  public void testMultipleFileAppends() {
    // write and validate a multi-file commit
    testMultiFileAppend();

    long startingNextRowId = table.operations().current().nextRowId();

    // add another append commit
    table.newAppend().appendFile(FILE_C).commit();
    Snapshot current = table.currentSnapshot();
    assertThat(current.firstRowId()).isEqualTo(startingNextRowId);
    assertThat(table.operations().current().nextRowId())
        .isEqualTo(startingNextRowId + FILE_C.recordCount());
    checkManifestListAssignment(
        table.io().newInputFile(current.manifestListLocation()), startingNextRowId, 0L);

    List<ManifestFile> manifests = current.dataManifests(table.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(table, manifests.get(0), startingNextRowId);
  }

  @Test
  public void testCommitConflict() {
    // start with a non-empty table
    testSingleFileAppend();

    String startingManifest =
        Iterables.getOnlyElement(table.currentSnapshot().allManifests(table.io())).path();
    long startingNextRowId = table.operations().current().nextRowId();

    // stage a new snapshot that is not committed
    AppendFiles stagedAppend = table.newAppend().appendFile(FILE_B);
    Snapshot staged = stagedAppend.apply();
    assertThat(table.operations().current().nextRowId()).isEqualTo(startingNextRowId);
    assertThat(staged.firstRowId()).isEqualTo(startingNextRowId);
    checkManifestListAssignment(
        table.io().newInputFile(staged.manifestListLocation()), startingNextRowId, 0L);

    List<ManifestFile> stagedManifests = staged.dataManifests(table.io());
    assertThat(stagedManifests).hasSize(2);

    ManifestFile stagedManifest = stagedManifests.get(0);
    checkDataFileAssignment(table, stagedManifest, startingNextRowId);

    // commit a concurrent operation with a second table reference
    BaseTable sameTable = TestTables.load(location, table.name());
    sameTable.newAppend().appendFile(FILE_C).commit();

    long secondNextRowId = sameTable.operations().current().nextRowId();
    assertThat(secondNextRowId).isEqualTo(startingNextRowId + FILE_C.recordCount());

    // committed snapshot should have the same first row ID values as the staged snapshot
    Snapshot committedFirst = sameTable.currentSnapshot();
    assertThat(committedFirst.firstRowId()).isEqualTo(startingNextRowId);

    checkManifestListAssignment(
        table.io().newInputFile(committedFirst.manifestListLocation()), startingNextRowId, 0L);

    List<ManifestFile> committedManifests = committedFirst.dataManifests(table.io());
    assertThat(committedManifests).hasSize(2);

    ManifestFile committedManifest = committedManifests.get(0);
    checkDataFileAssignment(table, committedManifest, startingNextRowId);
    assertThat(committedManifests.get(1).path()).isEqualTo(startingManifest);

    // committing the staged snapshot should reassign all first row ID values
    stagedAppend.commit();
    assertThat(table.operations().refresh().nextRowId())
        .isEqualTo(secondNextRowId + FILE_B.recordCount());

    sameTable.refresh();
    assertThat(table.currentSnapshot().snapshotId())
        .as("Both references should have the same current snapshot")
        .isEqualTo(sameTable.currentSnapshot().snapshotId());

    Snapshot committedSecond = table.currentSnapshot();
    assertThat(committedSecond.firstRowId()).isEqualTo(secondNextRowId);

    InputFile newManifestList = table.io().newInputFile(committedSecond.manifestListLocation());
    checkManifestListAssignment(newManifestList, secondNextRowId, startingNextRowId, 0L);

    List<ManifestFile> newManifests = committedSecond.dataManifests(table.io());
    assertThat(newManifests).hasSize(3);

    ManifestFile newManifest = newManifests.get(0);
    checkDataFileAssignment(table, newManifest, secondNextRowId);
    assertThat(newManifests.get(1)).isEqualTo(committedManifest);
    assertThat(newManifests.get(2).path()).isEqualTo(startingManifest);
  }

  @Test
  public void testOverwrite() {
    // start with a non-empty table
    testSingleFileAppend();

    long startingNextRowId = table.operations().current().nextRowId();
    long nextRowId = startingNextRowId + FILE_B.recordCount();

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).commit();
    assertThat(table.operations().current().nextRowId()).isEqualTo(nextRowId);

    Snapshot current = table.currentSnapshot();
    InputFile manifestList = table.io().newInputFile(current.manifestListLocation());
    // manifest removing FILE_A is written with first_row_id=startingNextRowId + FILE_B.recordCount
    // and the table's nextRowId is the same as the deleted manifest's firstRowId because the
    // manifest has 0 added or existing records
    checkManifestListAssignment(manifestList, startingNextRowId, nextRowId);

    List<ManifestFile> manifests = current.dataManifests(table.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(table, manifests.get(0), startingNextRowId);
    checkDataFileAssignment(table, manifests.get(1), 0L);
  }

  @Test
  public void testOverwriteWithFilteredManifest() {
    // start with multiple data files
    testMultiFileAppend();

    long startingNextRowId = table.operations().current().nextRowId();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_C).commit();
    // the table's nextRowId is incremented by FILE_B.recordCount() because it is in a new manifest
    long nextRowId = startingNextRowId + FILE_B.recordCount() + FILE_C.recordCount();
    assertThat(table.operations().current().nextRowId()).isEqualTo(nextRowId);

    Snapshot current = table.currentSnapshot();
    InputFile manifestList = table.io().newInputFile(current.manifestListLocation());
    // manifest removing FILE_A is written with first_row_id=startingNextRowId + FILE_C.recordCount
    checkManifestListAssignment(
        manifestList, startingNextRowId, startingNextRowId + FILE_C.recordCount());

    List<ManifestFile> manifests = current.dataManifests(table.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(table, manifests.get(0), startingNextRowId);
    // the starting row ID for FILE_B does not change
    checkDataFileAssignment(table, manifests.get(1), FILE_A.recordCount());
  }

  @Test
  public void testRowDelta() {
    // start with a non-empty table
    testSingleFileAppend();

    long startingNextRowId = table.operations().current().nextRowId();
    long nextRowId = startingNextRowId + FILE_B.recordCount();

    table.newRowDelta().addDeletes(FILE_A_DV).addRows(FILE_B).commit();
    assertThat(table.operations().current().nextRowId()).isEqualTo(nextRowId);

    Snapshot current = table.currentSnapshot();
    InputFile manifestList = table.io().newInputFile(current.manifestListLocation());
    // only one new data manifest is written
    checkManifestListAssignment(manifestList, startingNextRowId, 0L);

    List<ManifestFile> manifests = current.dataManifests(table.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(table, manifests.get(0), startingNextRowId);
    checkDataFileAssignment(table, manifests.get(1), 0L);
  }

  @Test
  public void testAssignmentWithManifestCompaction() {
    // start with a non-empty table
    // data manifests: [added(FILE_A)]
    testSingleFileAppend();

    long startingFirstRowId = table.operations().current().nextRowId();

    // add FILE_B and set the min so metadata is merged on the next commit
    table.newAppend().appendFile(FILE_B).commit();
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
    // data manifests: [added(FILE_B)], [added(FILE_A)]

    long preMergeNextRowId = startingFirstRowId + FILE_B.recordCount();
    assertThat(table.operations().current().nextRowId()).isEqualTo(preMergeNextRowId);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
    InputFile preMergeManifestList =
        table.io().newInputFile(table.currentSnapshot().manifestListLocation());
    checkManifestListAssignment(preMergeManifestList, startingFirstRowId, 0L);
    List<ManifestFile> preMergeManifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(preMergeManifests).hasSize(2);
    checkDataFileAssignment(table, preMergeManifests.get(0), startingFirstRowId);
    checkDataFileAssignment(table, preMergeManifests.get(1), 0L);

    table.newAppend().appendFile(FILE_C).commit();
    // data manifests: [add(FILE_C), exist(FILE_B), exist(FILE_A)]

    long mergedNextRowId =
        preMergeNextRowId + FILE_C.recordCount() + FILE_B.recordCount() + FILE_A.recordCount();

    assertThat(table.operations().current().nextRowId()).isEqualTo(mergedNextRowId);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    InputFile mergedManifestList =
        table.io().newInputFile(table.currentSnapshot().manifestListLocation());
    checkManifestListAssignment(mergedManifestList, preMergeNextRowId);
    List<ManifestFile> mergedManifests = table.currentSnapshot().dataManifests(table.io());
    checkDataFileAssignment(
        table, mergedManifests.get(0), preMergeNextRowId, startingFirstRowId, 0L);
  }

  @Test
  public void testTableUpgrade(@TempDir File altLocation) {
    BaseTable upgradeTable =
        TestTables.create(altLocation, "test_upgrade", SCHEMA, PartitionSpec.unpartitioned(), 2);

    // create data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    // and delete manifests: [added(FILE_A_DELETES)]
    upgradeTable.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    upgradeTable.newRowDelta().addDeletes(FILE_A_DELETES).commit(); // does not affect assignment
    upgradeTable.newOverwrite().deleteFile(FILE_B).addFile(FILE_C).commit();

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("v2 tables should always have next-row-id=0")
        .isEqualTo(0L);

    TestTables.upgrade(altLocation, "test_upgrade", 3);
    upgradeTable.refresh();

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should start at 0")
        .isEqualTo(0L);

    for (Snapshot snapshot : upgradeTable.snapshots()) {
      assertThat(snapshot.firstRowId())
          .as("Existing snapshots should not have first-row-id")
          .isNull();
    }

    Snapshot current = upgradeTable.currentSnapshot();
    InputFile manifestList = upgradeTable.io().newInputFile(current.manifestListLocation());
    // existing manifests should not have first_row_id assigned
    checkManifestListAssignment(manifestList, null, null);

    List<ManifestFile> manifests = current.dataManifests(upgradeTable.io());
    assertThat(manifests).hasSize(2);
    // manifests without first_row_id will not assign first_row_id
    checkDataFileAssignment(upgradeTable, manifests.get(0), (Long) null);
    checkDataFileAssignment(upgradeTable, manifests.get(1), (Long) null);
  }

  @Test
  public void testAssignmentAfterUpgrade(@TempDir File altLocation) {
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    testTableUpgrade(altLocation);

    BaseTable upgradeTable = TestTables.load(altLocation, "test_upgrade");
    long startingFirstRowId = upgradeTable.operations().current().nextRowId();

    List<ManifestFile> existingManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(existingManifests).hasSize(2);

    // any commit (even empty) should assign first_row_id to the entire metadata tree
    upgradeTable.newFastAppend().commit();
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should be updated to include the assigned data")
        .isEqualTo(startingFirstRowId + FILE_C.recordCount() + FILE_A.recordCount());

    Snapshot assigned = upgradeTable.currentSnapshot();

    assertThat(assigned.firstRowId()).isEqualTo(startingFirstRowId);
    InputFile manifestList = table.io().newInputFile(assigned.manifestListLocation());
    // the first manifest has added FILE_C, the second has deleted FILE_A and existing FILE_B
    checkManifestListAssignment(manifestList, 0L, FILE_C.recordCount());

    List<ManifestFile> manifests = assigned.dataManifests(upgradeTable.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, manifests.get(0), 0L);
    checkDataFileAssignment(upgradeTable, manifests.get(1), FILE_C.recordCount());
    // the existing manifests were reused without modification
    assertThat(manifests.get(0).path()).isEqualTo(existingManifests.get(0).path());
    assertThat(manifests.get(1).path()).isEqualTo(existingManifests.get(1).path());
  }

  @Test
  public void testDeleteAssignmentAfterUpgrade(@TempDir File altLocation) {
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    testTableUpgrade(altLocation);

    BaseTable upgradeTable = TestTables.load(altLocation, "test_upgrade");
    long startingFirstRowId = upgradeTable.operations().current().nextRowId();

    List<ManifestFile> existingManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(existingManifests).hasSize(2);

    // any commit (even empty) should assign first_row_id to the entire metadata tree
    upgradeTable.newDelete().deleteFile(FILE_C).commit();
    // data manifests: [deleted(FILE_C)], [existing(FILE_A), deleted(FILE_B)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should be updated to include the assigned data")
        .isEqualTo(startingFirstRowId + FILE_A.recordCount());

    Snapshot assigned = upgradeTable.currentSnapshot();

    assertThat(assigned.firstRowId()).isEqualTo(startingFirstRowId);
    InputFile manifestList = table.io().newInputFile(assigned.manifestListLocation());
    // the first manifest has added FILE_C, the second has deleted FILE_A and existing FILE_B
    checkManifestListAssignment(manifestList, 0L, 0L);

    List<ManifestFile> manifests = assigned.dataManifests(upgradeTable.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, manifests.get(0), 0L);
    checkDataFileAssignment(upgradeTable, manifests.get(1), 0L);
    // the existing manifests were reused without modification
    assertThat(manifests.get(1).path()).isEqualTo(existingManifests.get(1).path());
  }

  @Test
  public void testBranchAssignmentAfterUpgrade(@TempDir File altLocation) {
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    testTableUpgrade(altLocation);

    BaseTable upgradeTable = TestTables.load(altLocation, "test_upgrade");
    long startingFirstRowId = upgradeTable.operations().current().nextRowId();

    List<ManifestFile> existingManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(existingManifests).hasSize(2);

    // any commit (even empty) should assign first_row_id to the branch's tree
    upgradeTable.manageSnapshots().createBranch("branch").commit();
    upgradeTable.newAppend().toBranch("branch").commit();
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should be updated to include the assigned data in branch")
        .isEqualTo(startingFirstRowId + FILE_C.recordCount() + FILE_A.recordCount());

    // the main branch is unmodified and has no row IDs
    Snapshot current = upgradeTable.currentSnapshot();
    InputFile mainManifestList = upgradeTable.io().newInputFile(current.manifestListLocation());
    checkManifestListAssignment(mainManifestList, null, null);

    List<ManifestFile> mainManifests = current.dataManifests(upgradeTable.io());
    assertThat(mainManifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, mainManifests.get(0), (Long) null);
    checkDataFileAssignment(upgradeTable, mainManifests.get(1), (Long) null);
    assertThat(mainManifests.get(0).path()).isEqualTo(existingManifests.get(0).path());
    assertThat(mainManifests.get(1).path()).isEqualTo(existingManifests.get(1).path());

    // the branch should have row IDs assigned
    Snapshot assigned = upgradeTable.snapshot("branch");

    assertThat(assigned.firstRowId()).isEqualTo(startingFirstRowId);
    InputFile branchManifestList = table.io().newInputFile(assigned.manifestListLocation());
    // the first manifest has added FILE_C, the second has deleted FILE_A and existing FILE_B
    checkManifestListAssignment(branchManifestList, 0L, FILE_C.recordCount());

    List<ManifestFile> branchManifests = assigned.dataManifests(upgradeTable.io());
    assertThat(branchManifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, branchManifests.get(0), 0L);
    checkDataFileAssignment(upgradeTable, branchManifests.get(1), FILE_C.recordCount());
    // the existing manifests were reused without modification
    assertThat(branchManifests.get(0).path()).isEqualTo(existingManifests.get(0).path());
    assertThat(branchManifests.get(1).path()).isEqualTo(existingManifests.get(1).path());
  }

  @Test
  public void testOverwriteAssignmentAfterUpgrade(@TempDir File altLocation) {
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    testTableUpgrade(altLocation);

    BaseTable upgradeTable = TestTables.load(altLocation, "test_upgrade");
    long startingFirstRowId = upgradeTable.operations().current().nextRowId();

    List<ManifestFile> existingManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(existingManifests).hasSize(2);

    // any commit should assign first_row_id to the entire metadata tree
    upgradeTable.newOverwrite().deleteFile(FILE_C).addFile(FILE_B).commit();
    // data manifests: [added(FILE_B)], [deleted(FILE_C)], [existing(FILE_A), deleted(FILE_B)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should be updated to account for existing data and new changes")
        .isEqualTo(startingFirstRowId + FILE_B.recordCount() + FILE_A.recordCount());

    Snapshot assigned = upgradeTable.currentSnapshot();

    assertThat(assigned.firstRowId()).isEqualTo(startingFirstRowId);
    InputFile manifestList = table.io().newInputFile(assigned.manifestListLocation());
    // the second manifest only has deleted files and does not use ID space
    checkManifestListAssignment(manifestList, 0L, FILE_B.recordCount(), FILE_B.recordCount());

    List<ManifestFile> manifests = assigned.dataManifests(upgradeTable.io());
    assertThat(manifests).hasSize(3);
    checkDataFileAssignment(upgradeTable, manifests.get(0), 0L);
    checkDataFileAssignment(upgradeTable, manifests.get(1)); // no live files
    checkDataFileAssignment(upgradeTable, manifests.get(2), FILE_B.recordCount());
    // the last manifest is reused without modification
    assertThat(manifests.get(2).path()).isEqualTo(existingManifests.get(1).path());
  }

  @Test
  public void testRowDeltaAssignmentAfterUpgrade(@TempDir File altLocation) {
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]
    testTableUpgrade(altLocation);

    BaseTable upgradeTable = TestTables.load(altLocation, "test_upgrade");
    long startingFirstRowId = upgradeTable.operations().current().nextRowId();

    List<ManifestFile> existingManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(existingManifests).hasSize(2);

    // any commit (even empty) should assign first_row_id to the entire metadata tree
    upgradeTable.newRowDelta().addDeletes(FILE_A_DV).commit();
    // data manifests: [added(FILE_C)], [existing(FILE_A), deleted(FILE_B)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should be updated to include the assigned data")
        .isEqualTo(startingFirstRowId + FILE_C.recordCount() + FILE_A.recordCount());

    Snapshot assigned = upgradeTable.currentSnapshot();

    assertThat(assigned.firstRowId()).isEqualTo(startingFirstRowId);
    InputFile manifestList = table.io().newInputFile(assigned.manifestListLocation());
    // the first manifest has added FILE_C, the second has deleted FILE_A and existing FILE_B
    checkManifestListAssignment(manifestList, 0L, FILE_C.recordCount());

    List<ManifestFile> manifests = assigned.dataManifests(upgradeTable.io());
    assertThat(manifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, manifests.get(0), 0L);
    checkDataFileAssignment(upgradeTable, manifests.get(1), FILE_C.recordCount());
    // the existing manifests were reused without modification
    assertThat(manifests.get(0).path()).isEqualTo(existingManifests.get(0).path());
    assertThat(manifests.get(1).path()).isEqualTo(existingManifests.get(1).path());
  }

  @Test
  public void testUpgradeAssignmentWithManifestCompaction(@TempDir File altLocation) {
    // create a non-empty upgrade table with FILE_A
    BaseTable upgradeTable =
        TestTables.create(altLocation, "test_upgrade", SCHEMA, PartitionSpec.unpartitioned(), 2);

    upgradeTable.newAppend().appendFile(FILE_A).commit();
    upgradeTable.newAppend().appendFile(FILE_B).commit();
    upgradeTable.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
    // data manifests: [added(FILE_B)], [added(FILE_A)]

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("v2 tables should always have next-row-id=0")
        .isEqualTo(0L);

    TestTables.upgrade(altLocation, "test_upgrade", 3);
    upgradeTable.refresh();

    assertThat(upgradeTable.operations().current().nextRowId())
        .as("next-row-id should start at 0")
        .isEqualTo(0L);

    for (Snapshot snapshot : upgradeTable.snapshots()) {
      assertThat(snapshot.firstRowId())
          .as("Existing snapshots should not have first-row-id")
          .isNull();
    }

    assertThat(upgradeTable.currentSnapshot().allManifests(upgradeTable.io())).hasSize(2);
    InputFile preMergeManifestList =
        upgradeTable.io().newInputFile(upgradeTable.currentSnapshot().manifestListLocation());
    checkManifestListAssignment(preMergeManifestList, null, null);
    List<ManifestFile> preMergeManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    assertThat(preMergeManifests).hasSize(2);
    checkDataFileAssignment(upgradeTable, preMergeManifests.get(0), (Long) null);
    checkDataFileAssignment(upgradeTable, preMergeManifests.get(1), (Long) null);

    // add FILE_C and trigger metadata compaction
    upgradeTable.newAppend().appendFile(FILE_C).commit();
    // data manifests: [add(FILE_C), exist(FILE_B), exist(FILE_A)]

    long mergedNextRowId = FILE_C.recordCount() + FILE_B.recordCount() + FILE_A.recordCount();

    assertThat(upgradeTable.operations().current().nextRowId()).isEqualTo(mergedNextRowId);
    assertThat(upgradeTable.currentSnapshot().allManifests(upgradeTable.io())).hasSize(1);
    InputFile mergedManifestList =
        upgradeTable.io().newInputFile(upgradeTable.currentSnapshot().manifestListLocation());
    checkManifestListAssignment(mergedManifestList, 0L);
    List<ManifestFile> mergedManifests =
        upgradeTable.currentSnapshot().dataManifests(upgradeTable.io());
    checkDataFileAssignment(
        upgradeTable,
        mergedManifests.get(0),
        0L,
        FILE_C.recordCount(),
        FILE_C.recordCount() + FILE_B.recordCount());
  }

  private static ManifestContent content(int ordinal) {
    return ManifestContent.values()[ordinal];
  }

  private static void checkManifestListAssignment(InputFile in, Long... firstRowIds) {
    try (CloseableIterable<Record> reader =
        InternalData.read(FileFormat.AVRO, in)
            .project(ManifestFile.schema().select("first_row_id", "content"))
            .build()) {

      // all row IDs must be assigned at write time
      int index = 0;
      for (Record manifest : reader) {
        if (content((Integer) manifest.getField("content")) != ManifestContent.DATA) {
          assertThat(manifest.getField("first_row_id"))
              .as("Row ID for delete manifest (%s) should be null", index)
              .isNull();
        } else if (index < firstRowIds.length) {
          assertThat(manifest.getField("first_row_id"))
              .as("Row ID for data manifest (%s) should match", index)
              .isEqualTo(firstRowIds[index]);
        } else {
          fail("No expected first row ID for manifest: %s=%s", index, manifest);
        }

        index += 1;
      }

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    // also check that the values are read correctly
    int index = 0;
    for (ManifestFile manifest : ManifestLists.read(in)) {
      if (manifest.content() != ManifestContent.DATA) {
        assertThat(manifest.firstRowId()).isNull();
      } else if (index < firstRowIds.length) {
        assertThat(manifest.firstRowId()).isEqualTo(firstRowIds[index]);
      } else {
        fail("No expected first row ID for manifest: " + manifest);
      }

      index += 1;
    }
  }

  private static void checkDataFileAssignment(
      Table table, ManifestFile manifest, Long... firstRowIds) {
    // all row IDs must be assigned at write time
    int index = 0;
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(manifest, table.io(), table.specs())) {

      // test that the first_row_id column is always scanned, even if not requested
      reader.select(BaseScan.SCAN_COLUMNS);

      for (DataFile file : reader) {
        assertThat(file.content()).isEqualTo(FileContent.DATA);
        if (index < firstRowIds.length) {
          assertThat(file.firstRowId())
              .as("Row ID for data file (%s) should match", index)
              .isEqualTo(firstRowIds[index]);
        } else {
          fail("No expected first row ID for file: " + manifest);
        }

        index += 1;
      }

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
