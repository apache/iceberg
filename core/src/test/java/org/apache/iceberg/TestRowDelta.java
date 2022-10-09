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

import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_PREFIX;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_POS_DELETES_PROP;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDelta extends V2TableTestBase {
  @Test
  public void testAddDeleteFile() {
    table
        .newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 1", 1, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    Assert.assertEquals(
        "Delta commit should use operation 'overwrite'",
        DataOperations.OVERWRITE,
        snap.operation());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests(table.io()).size());
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistDefaults() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2).commit();

    // delete FILE_B
    table.newDelete().deleteFile(FILE_B).commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateFromSnapshot(validateFromSnapshotId)
                .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());

    table
        .newRowDelta()
        .addDeletes(FILE_B_DELETES)
        .validateDataFilesExist(ImmutableList.of(FILE_B.path()))
        .validateFromSnapshot(validateFromSnapshotId)
        .commit();

    Assert.assertEquals(
        "Table should have one new delete manifest",
        1,
        table.currentSnapshot().deleteManifests(table.io()).size());
    ManifestFile deletes = table.currentSnapshot().deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        seqs(4),
        ids(table.currentSnapshot().snapshotId()),
        files(FILE_B_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistOverwrite() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A
    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2).commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateFromSnapshot(validateFromSnapshotId)
                .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistReplacePartitions() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A's partition
    table.newReplacePartitions().addFile(FILE_A2).commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateFromSnapshot(validateFromSnapshotId)
                .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistFromSnapshot() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    // overwrite FILE_A's partition
    table.newReplacePartitions().addFile(FILE_A2).commit();

    // test changes to the table back to the snapshot where FILE_A was overwritten
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();
    long replaceSnapshotId = table.currentSnapshot().snapshotId();

    // even though FILE_A was deleted, it happened before the "from" snapshot, so the validation
    // allows it
    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(validateFromSnapshotId)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests(table.io()).size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        seqs(2),
        ids(replaceSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A deleted
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        seqs(1, 1),
        ids(replaceSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals(
        "Should have 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(3),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistRewrite() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // rewrite FILE_A
    table.newRewrite().rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_A2)).commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateFromSnapshot(validateFromSnapshotId)
                .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistValidateDeletes() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newDelete().deleteFile(FILE_A).commit();

    long deleteSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateDeletedFiles()
                .validateFromSnapshot(validateFromSnapshotId)
                .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateNoConflicts() {
    table.newAppend().appendFile(FILE_A).commit();

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newAppend().appendFile(FILE_A2).commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows(
        "Should fail to add FILE_A_DELETES because FILE_A2 was added",
        ValidationException.class,
        "Found conflicting files",
        () ->
            table
                .newRowDelta()
                .addDeletes(FILE_A_DELETES)
                .validateFromSnapshot(validateFromSnapshotId)
                .conflictDetectionFilter(Expressions.equal("data", "u")) // bucket16("u") -> 0
                .validateNoConflictingDataFiles()
                .commit());

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        appendSnapshotId,
        table.currentSnapshot().snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        table.currentSnapshot().deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateNoConflictsFromSnapshot() {
    table.newAppend().appendFile(FILE_A).commit();

    long appendSnapshotId = table.currentSnapshot().snapshotId();

    // delete FILE_A
    table.newAppend().appendFile(FILE_A2).commit();

    // even though FILE_A2 was added, it happened before the "from" snapshot, so the validation
    // allows it
    long validateFromSnapshotId = table.currentSnapshot().snapshotId();

    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateDeletedFiles()
        .validateFromSnapshot(validateFromSnapshotId)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .conflictDetectionFilter(Expressions.equal("data", "u")) // bucket16("u") -> 0
        .validateNoConflictingDataFiles()
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests(table.io()).size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        seqs(2),
        ids(validateFromSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A added
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        seqs(1),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should have 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(3),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testOverwriteWithDeleteFile() {
    table
        .newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting by a filter will also remove delete files that match because all matching data
    // files are removed.
    table
        .newOverwrite()
        .overwriteByRowFilter(Expressions.equal(Expressions.bucket("data", 16), 0))
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests(table.io()).size());
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplacePartitionsWithDeleteFile() {
    table
        .newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting the partition will also remove delete files that match because all matching data
    // files are removed.
    table.newReplacePartitions().addFile(FILE_A2).commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 2 data manifests", 2, snap.dataManifests(table.io()).size());
    int deleteManifestPos = snap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        snap.dataManifests(table.io()).get(deleteManifestPos),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        snap.dataManifests(table.io()).get(appendManifestPos),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testDeleteByExpressionWithDeleteFile() {
    table
        .newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting with a filter will also remove delete files that match because all matching data
    // files are removed.
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests(table.io()).size());
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.DELETED));
  }

  @Test
  public void testDeleteDataFileWithDeleteFile() {
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 1 data manifest", 1, deleteSnap.dataManifests(table.io()).size());
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        seqs(1),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, deleteSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next commit, causing the min sequence number
    // of all data files
    // to be 2, the largest known sequence number. this will cause FILE_A_DELETES to be removed
    // because it is too old
    // to apply to any data files.
    table.newDelete().deleteFile("no-such-file").commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should have 0 data manifests", 0, nextSnap.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, nextSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        seqs(1),
        ids(nextSnap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.DELETED));
  }

  @Test
  public void testFastAppendDoesNotRemoveStaleDeleteFiles() {
    table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 1 data manifest", 1, deleteSnap.dataManifests(table.io()).size());
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        seqs(1),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, deleteSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next merging commit, but FastAppend will not
    // remove it
    table.newFastAppend().appendFile(FILE_B).commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should have 2 data manifests", 2, nextSnap.dataManifests(table.io()).size());
    int deleteManifestPos =
        nextSnap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(deleteManifestPos),
        seqs(1),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(appendManifestPos),
        seqs(3),
        ids(nextSnap.snapshotId()),
        files(FILE_B),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, nextSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistWithConflictDetectionFilter() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile1).commit();

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile2).commit();

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = table.currentSnapshot();

    // add a delete file for partition A
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    Expression conflictDetectionFilter = Expressions.equal("data", "a");
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile)
            .validateDataFilesExist(ImmutableList.of(dataFile1.path()))
            .validateDeletedFiles()
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles();

    // concurrently delete the file for partition B
    table.newDelete().deleteFile(dataFile2).commit();

    // commit the delta for partition A
    rowDelta.commit();

    Assert.assertEquals(
        "Table should have one new delete manifest",
        1,
        table.currentSnapshot().deleteManifests(table.io()).size());
    ManifestFile deletes = table.currentSnapshot().deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        seqs(4),
        ids(table.currentSnapshot().snapshotId()),
        files(deleteFile),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesDoNotExistWithConflictDetectionFilter() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile1).commit();

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = table.currentSnapshot();

    // add a delete file for partition A
    DeleteFile deleteFile =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    Expression conflictDetectionFilter = Expressions.equal("data", "a");
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile)
            .validateDataFilesExist(ImmutableList.of(dataFile1.path()))
            .validateDeletedFiles()
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles();

    // concurrently delete the file for partition A
    table.newDelete().deleteFile(dataFile1).commit();

    AssertHelpers.assertThrows(
        "Should fail to add deletes because data file is missing",
        ValidationException.class,
        "Cannot commit, missing data files",
        rowDelta::commit);
  }

  @Test
  public void testAddDeleteFilesMultipleSpecs() {
    // enable partition summaries
    table.updateProperties().set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "10").commit();

    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    table.newAppend().appendFile(firstSnapshotDataFile).commit();

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    table.newAppend().appendFile(secondSnapshotDataFile).commit();

    // evolve the spec and add a new partition field
    table.updateSpec().addField("data").commit();

    // append a data file with the new spec
    DataFile thirdSnapshotDataFile = newDataFile("data=abc");
    table.newAppend().appendFile(thirdSnapshotDataFile).commit();

    Assert.assertEquals("Should have 3 specs", 3, table.specs().size());

    // commit a row delta with 1 data file and 3 delete files where delete files have different
    // specs
    DataFile dataFile = newDataFile("data=xyz");
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");
    DeleteFile thirdDeleteFile = newDeleteFile(thirdSnapshotDataFile.specId(), "data=abc");

    table
        .newRowDelta()
        .addRows(dataFile)
        .addDeletes(firstDeleteFile)
        .addDeletes(secondDeleteFile)
        .addDeletes(thirdDeleteFile)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 4", 4, snapshot.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 4", 4, table.ops().current().lastSequenceNumber());
    Assert.assertEquals(
        "Delta commit should be 'overwrite'", DataOperations.OVERWRITE, snapshot.operation());

    Map<String, String> summary = snapshot.summary();

    Assert.assertEquals(
        "Should change 4 partitions", "4", summary.get(CHANGED_PARTITION_COUNT_PROP));
    Assert.assertEquals("Should add 1 data file", "1", summary.get(ADDED_FILES_PROP));
    Assert.assertEquals("Should have 4 data files", "4", summary.get(TOTAL_DATA_FILES_PROP));
    Assert.assertEquals("Should add 3 delete files", "3", summary.get(ADDED_DELETE_FILES_PROP));
    Assert.assertEquals("Should have 3 delete files", "3", summary.get(TOTAL_DELETE_FILES_PROP));
    Assert.assertEquals("Should add 3 position deletes", "3", summary.get(ADDED_POS_DELETES_PROP));
    Assert.assertEquals("Should have 3 position deletes", "3", summary.get(TOTAL_POS_DELETES_PROP));

    Assert.assertTrue(
        "Partition metrics must be correct",
        summary.get(CHANGED_PARTITION_PREFIX).contains(ADDED_DELETE_FILES_PROP + "=1"));
    Assert.assertTrue(
        "Partition metrics must be correct",
        summary
            .get(CHANGED_PARTITION_PREFIX + "data_bucket=0")
            .contains(ADDED_DELETE_FILES_PROP + "=1"));
    Assert.assertTrue(
        "Partition metrics must be correct",
        summary
            .get(CHANGED_PARTITION_PREFIX + "data=abc")
            .contains(ADDED_DELETE_FILES_PROP + "=1"));
    Assert.assertTrue(
        "Partition metrics must be correct",
        summary.get(CHANGED_PARTITION_PREFIX + "data=xyz").contains(ADDED_FILES_PROP + "=1"));

    // 3 appends + 1 row delta
    Assert.assertEquals(
        "Should have 4 data manifest", 4, snapshot.dataManifests(table.io()).size());
    validateManifest(
        snapshot.dataManifests(table.io()).get(0),
        seqs(4),
        ids(snapshot.snapshotId()),
        files(dataFile),
        statuses(Status.ADDED));

    // each delete file goes into a separate manifest as the specs are different
    Assert.assertEquals(
        "Should produce 3 delete manifest", 3, snapshot.deleteManifests(table.io()).size());

    ManifestFile firstDeleteManifest = snapshot.deleteManifests(table.io()).get(2);
    Assert.assertEquals(
        "Spec must match", firstSnapshotDataFile.specId(), firstDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        firstDeleteManifest,
        seqs(4),
        ids(snapshot.snapshotId()),
        files(firstDeleteFile),
        statuses(Status.ADDED));

    ManifestFile secondDeleteManifest = snapshot.deleteManifests(table.io()).get(1);
    Assert.assertEquals(
        "Spec must match", secondSnapshotDataFile.specId(), secondDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        secondDeleteManifest,
        seqs(4),
        ids(snapshot.snapshotId()),
        files(secondDeleteFile),
        statuses(Status.ADDED));

    ManifestFile thirdDeleteManifest = snapshot.deleteManifests(table.io()).get(0);
    Assert.assertEquals(
        "Spec must match", thirdSnapshotDataFile.specId(), thirdDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        thirdDeleteManifest,
        seqs(4),
        ids(snapshot.snapshotId()),
        files(thirdDeleteFile),
        statuses(Status.ADDED));
  }

  @Test
  public void testManifestMergingMultipleSpecs() {
    // make sure we enable manifest merging
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        .commit();

    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    table.newAppend().appendFile(firstSnapshotDataFile).commit();

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    table.newAppend().appendFile(secondSnapshotDataFile).commit();

    // commit two delete files to two specs in a single operation
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    table.newRowDelta().addDeletes(firstDeleteFile).addDeletes(secondDeleteFile).commit();

    Snapshot thirdSnapshot = table.currentSnapshot();

    // 2 appends and 1 row delta where delete files belong to different specs
    Assert.assertEquals(
        "Should have 2 data manifest", 2, thirdSnapshot.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should have 2 delete manifest", 2, thirdSnapshot.deleteManifests(table.io()).size());

    // commit two more delete files to the same specs to trigger merging
    DeleteFile thirdDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile fourthDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    table.newRowDelta().addDeletes(thirdDeleteFile).addDeletes(fourthDeleteFile).commit();

    Snapshot fourthSnapshot = table.currentSnapshot();

    // make sure merging respects spec boundaries
    Assert.assertEquals(
        "Should have 2 data manifest", 2, fourthSnapshot.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should have 2 delete manifest", 2, fourthSnapshot.deleteManifests(table.io()).size());

    ManifestFile firstDeleteManifest = fourthSnapshot.deleteManifests(table.io()).get(1);
    Assert.assertEquals(
        "Spec must match", firstSnapshotDataFile.specId(), firstDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        firstDeleteManifest,
        seqs(4, 3),
        ids(fourthSnapshot.snapshotId(), thirdSnapshot.snapshotId()),
        files(thirdDeleteFile, firstDeleteFile),
        statuses(Status.ADDED, Status.EXISTING));

    ManifestFile secondDeleteManifest = fourthSnapshot.deleteManifests(table.io()).get(0);
    Assert.assertEquals(
        "Spec must match", secondSnapshotDataFile.specId(), secondDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        secondDeleteManifest,
        seqs(4, 3),
        ids(fourthSnapshot.snapshotId(), thirdSnapshot.snapshotId()),
        files(fourthDeleteFile, secondDeleteFile),
        statuses(Status.ADDED, Status.EXISTING));
  }

  @Test
  public void testAbortMultipleSpecs() {
    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    table.newAppend().appendFile(firstSnapshotDataFile).commit();

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    table.newAppend().appendFile(secondSnapshotDataFile).commit();

    // prepare two delete files that belong to different specs
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    // capture all deletes
    Set<String> deletedFiles = Sets.newHashSet();

    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(firstDeleteFile)
            .addDeletes(secondDeleteFile)
            .deleteWith(deletedFiles::add)
            .validateDeletedFiles()
            .validateDataFilesExist(ImmutableList.of(firstSnapshotDataFile.path()));

    rowDelta.apply();

    // perform a conflicting concurrent operation
    table.newDelete().deleteFile(firstSnapshotDataFile).commit();

    AssertHelpers.assertThrows(
        "Should fail to commit row delta",
        ValidationException.class,
        "Cannot commit, missing data files",
        rowDelta::commit);

    // we should clean up 1 manifest list and 2 delete manifests
    Assert.assertEquals("Should delete 3 files", 3, deletedFiles.size());
  }

  @Test
  public void testConcurrentConflictingRowDelta() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();

    Expression conflictDetectionFilter = Expressions.alwaysTrue();

    // mock a MERGE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addRows(FILE_B)
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    AssertHelpers.assertThrows(
        "Should reject commit",
        ValidationException.class,
        "Found new conflicting delete files",
        rowDelta::commit);
  }

  @Test
  public void testConcurrentConflictingRowDeltaWithoutAppendValidation() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();

    Expression conflictDetectionFilter = Expressions.alwaysTrue();

    // mock a MERGE operation with snapshot isolation (i.e. no append validation)
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDeleteFiles();

    table
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    AssertHelpers.assertThrows(
        "Should reject commit",
        ValidationException.class,
        "Found new conflicting delete files",
        rowDelta::commit);
  }

  @Test
  public void testConcurrentNonConflictingRowDelta() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile1).commit();

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table.newAppend().appendFile(dataFile2).commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    Expression conflictDetectionFilter = Expressions.equal("data", "a");

    // add a delete file for partition A
    DeleteFile deleteFile1 =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/data-a-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // add a delete file for partition B
    DeleteFile deleteFile2 =
        FileMetadata.deleteFileBuilder(table.spec())
            .ofPositionDeletes()
            .withPath("/path/to/data-b-deletes.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    table
        .newRowDelta()
        .addDeletes(deleteFile2)
        .validateFromSnapshot(baseSnapshot.snapshotId())
        .commit();

    rowDelta.commit();

    validateTableDeleteFiles(table, deleteFile1, deleteFile2);
  }

  @Test
  public void testConcurrentNonConflictingRowDeltaAndRewriteFilesWithSequenceNumber() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    table.newAppend().appendFile(dataFile1).commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    // add an equality delete file
    DeleteFile deleteFile1 =
        newEqualityDeleteFile(
            table.spec().specId(), "data=a", table.schema().asStruct().fields().get(0).fieldId());

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    rowDelta.commit();
    rewriteFiles.commit();

    validateTableDeleteFiles(table, deleteFile1);
    validateTableFiles(table, dataFile2);
  }

  @Test
  public void testRowDeltaAndRewriteFilesMergeManifestsWithSequenceNumber() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    table.newAppend().appendFile(dataFile1).commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    // add an equality delete file
    DeleteFile deleteFile1 =
        newEqualityDeleteFile(
            table.spec().specId(), "data=a", table.schema().asStruct().fields().get(0).fieldId());

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    rowDelta.commit();
    rewriteFiles.commit();

    table.refresh();
    List<ManifestFile> dataManifests = table.currentSnapshot().dataManifests(table.io());
    Assert.assertEquals("should have 1 data manifest", 1, dataManifests.size());
    ManifestFile mergedDataManifest = dataManifests.get(0);

    long currentSnapshotId = table.currentSnapshot().snapshotId();

    validateManifest(
        mergedDataManifest,
        seqs(1, 1),
        ids(currentSnapshotId, currentSnapshotId),
        files(dataFile2, dataFile1),
        statuses(Status.ADDED, Status.DELETED));
  }

  @Test
  public void testConcurrentConflictingRowDeltaAndRewriteFilesWithSequenceNumber() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    table.newAppend().appendFile(dataFile1).commit();

    Snapshot baseSnapshot = table.currentSnapshot();

    // add an position delete file
    DeleteFile deleteFile1 = newDeleteFile(table.spec().specId(), "data=a");

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    rowDelta.commit();

    AssertHelpers.assertThrows(
        "Should not allow any new position delete associated with the data file",
        ValidationException.class,
        "Cannot commit, found new position delete for replaced data file",
        rewriteFiles::commit);
  }

  @Test
  public void testRowDeltaCaseSensitivity() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_A2).commit();

    Snapshot firstSnapshot = table.currentSnapshot();

    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();

    Expression conflictDetectionFilter = Expressions.equal(Expressions.bucket("dAtA", 16), 0);

    AssertHelpers.assertThrows(
        "Should use case sensitive binding by default",
        ValidationException.class,
        "Cannot find field 'dAtA'",
        () ->
            table
                .newRowDelta()
                .addRows(FILE_B)
                .addDeletes(FILE_A2_DELETES)
                .validateFromSnapshot(firstSnapshot.snapshotId())
                .conflictDetectionFilter(conflictDetectionFilter)
                .validateNoConflictingDataFiles()
                .validateNoConflictingDeleteFiles()
                .commit());

    AssertHelpers.assertThrows(
        "Should fail with case sensitive binding",
        ValidationException.class,
        "Cannot find field 'dAtA'",
        () ->
            table
                .newRowDelta()
                .caseSensitive(true)
                .addRows(FILE_B)
                .addDeletes(FILE_A2_DELETES)
                .validateFromSnapshot(firstSnapshot.snapshotId())
                .conflictDetectionFilter(conflictDetectionFilter)
                .validateNoConflictingDataFiles()
                .validateNoConflictingDeleteFiles()
                .commit());

    // binding should succeed and trigger the validation
    AssertHelpers.assertThrows(
        "Should reject case sensitive binding",
        ValidationException.class,
        "Found new conflicting delete files",
        () ->
            table
                .newRowDelta()
                .caseSensitive(false)
                .addRows(FILE_B)
                .addDeletes(FILE_A2_DELETES)
                .validateFromSnapshot(firstSnapshot.snapshotId())
                .conflictDetectionFilter(conflictDetectionFilter)
                .validateNoConflictingDataFiles()
                .validateNoConflictingDeleteFiles()
                .commit());
  }

  @Test
  public void testRowDeltaToBranchUnsupported() {
    AssertHelpers.assertThrows(
        "Should reject committing row delta to branch",
        UnsupportedOperationException.class,
        "Cannot commit to branch someBranch: org.apache.iceberg.BaseRowDelta does not support branch commits",
        () ->
            table
                .newRowDelta()
                .caseSensitive(false)
                .addRows(FILE_B)
                .addDeletes(FILE_A2_DELETES)
                .toBranch("someBranch")
                .commit());
  }
}
