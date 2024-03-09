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
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;

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
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRowDelta extends V2TableTestBase {

  private final String branch;

  @Parameterized.Parameters(name = "branch = {0}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {"main"}, new Object[] {"testBranch"},
    };
  }

  public TestRowDelta(String branch) {
    this.branch = branch;
  }

  @Test
  public void testAddDeleteFile() {
    SnapshotUpdate rowDelta =
        table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES);

    commit(table, rowDelta, branch);
    Snapshot snap = latestSnapshot(table, branch);
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
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistDefaults() {
    SnapshotUpdate rowDelta1 = table.newAppend().appendFile(FILE_A).appendFile(FILE_B);

    commit(table, rowDelta1, branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A
    SnapshotUpdate rowDelta2 = table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2);

    commit(table, rowDelta2, branch);

    // delete FILE_B
    SnapshotUpdate rowDelta3 = table.newDelete().deleteFile(FILE_B);

    commit(table, rowDelta3, branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(FILE_B_DELETES)
            .validateDataFilesExist(ImmutableList.of(FILE_B.path()))
            .validateFromSnapshot(validateFromSnapshotId),
        branch);

    Assert.assertEquals(
        "Table should have one new delete manifest",
        1,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
    ManifestFile deletes = latestSnapshot(table, branch).deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(latestSnapshot(table, branch).snapshotId()),
        files(FILE_B_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistOverwrite() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A
    commit(table, table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistReplacePartitions() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A's partition
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistFromSnapshot() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A's partition
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    // test changes to the table back to the snapshot where FILE_A was overwritten
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();
    long replaceSnapshotId = latestSnapshot(table, branch).snapshotId();

    // even though FILE_A was deleted, it happened before the "from" snapshot, so the validation
    // allows it
    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests(table.io()).size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(replaceSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A deleted
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(replaceSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals(
        "Should have 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testValidateDataFilesExistRewrite() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // rewrite FILE_A
    commit(
        table,
        table.newRewrite().rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_A2)),
        branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateDataFilesExistValidateDeletes() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateDeletedFiles()
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.path())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        deleteSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateNoConflicts() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newAppend().appendFile(FILE_A2), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(FILE_A_DELETES)
                        .validateFromSnapshot(validateFromSnapshotId)
                        .conflictDetectionFilter(
                            Expressions.equal("data", "u")) // bucket16("u") -> 0
                        .validateNoConflictingDataFiles(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");

    Assert.assertEquals(
        "Table state should not be modified by failed RowDelta operation",
        appendSnapshotId,
        latestSnapshot(table, branch).snapshotId());

    Assert.assertEquals(
        "Table should not have any delete manifests",
        0,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
  }

  @Test
  public void testValidateNoConflictsFromSnapshot() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newAppend().appendFile(FILE_A2), branch);

    // even though FILE_A2 was added, it happened before the "from" snapshot, so the validation
    // allows it
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateDeletedFiles()
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
            .conflictDetectionFilter(Expressions.equal("data", "u")) // bucket16("u") -> 0
            .validateNoConflictingDataFiles(),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 3, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, snap.dataManifests(table.io()).size());
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(validateFromSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A added
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should have 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(snap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @Test
  public void testOverwriteWithDeleteFile() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1",
        1,
        latestSnapshot(table, branch).sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting by a filter will also remove delete files that match because all matching data
    // files are removed.
    commit(
        table,
        table
            .newOverwrite()
            .overwriteByRowFilter(Expressions.equal(Expressions.bucket("data", 16), 0)),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests(table.io()).size());
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplacePartitionsWithDeleteFile() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1",
        1,
        latestSnapshot(table, branch).sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting the partition will also remove delete files that match because all matching data
    // files are removed.
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    Snapshot snap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 2 data manifests", 2, snap.dataManifests(table.io()).size());
    int deleteManifestPos = snap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        snap.dataManifests(table.io()).get(deleteManifestPos),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        snap.dataManifests(table.io()).get(appendManifestPos),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testDeleteByExpressionWithDeleteFile() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES).addDeletes(FILE_B_DELETES),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1",
        1,
        latestSnapshot(table, branch).sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting with a filter will also remove delete files that match because all matching data
    // files are removed.
    commit(table, table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()), branch);

    Snapshot snap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests(table.io()).size());
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, snap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.DELETED));
  }

  @Test
  public void testDeleteDataFileWithDeleteFile() {
    commit(table, table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES), branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1",
        1,
        latestSnapshot(table, branch).sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    Snapshot deleteSnap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 1 data manifest", 1, deleteSnap.dataManifests(table.io()).size());
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, deleteSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next commit, causing the min sequence number
    // of all data files
    // to be 2, the largest known sequence number. this will cause FILE_A_DELETES to be removed
    // because it is too old
    // to apply to any data files.
    commit(table, table.newDelete().deleteFile("no-such-file"), branch);

    Snapshot nextSnap = latestSnapshot(table, branch);
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should have 0 data manifests", 0, nextSnap.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, nextSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(nextSnap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.DELETED));
  }

  @Test
  public void testFastAppendDoesNotRemoveStaleDeleteFiles() {
    commit(table, table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES), branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    Assert.assertEquals(
        "Commit should produce sequence number 1",
        1,
        latestSnapshot(table, branch).sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    Snapshot deleteSnap = latestSnapshot(table, branch);
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should produce 1 data manifest", 1, deleteSnap.dataManifests(table.io()).size());
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, deleteSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next merging commit, but FastAppend will not
    // remove it
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    Snapshot nextSnap = latestSnapshot(table, branch);
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals(
        "Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals(
        "Should have 2 data manifests", 2, nextSnap.dataManifests(table.io()).size());
    int deleteManifestPos =
        nextSnap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(deleteManifestPos),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(appendManifestPos),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(nextSnap.snapshotId()),
        files(FILE_B),
        statuses(Status.ADDED));

    Assert.assertEquals(
        "Should produce 1 delete manifest", 1, nextSnap.deleteManifests(table.io()).size());
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile2), branch);

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = latestSnapshot(table, branch);

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
    commit(table, table.newDelete().deleteFile(dataFile2), branch);

    // commit the delta for partition A
    commit(table, rowDelta, branch);

    Assert.assertEquals(
        "Table should have one new delete manifest",
        1,
        latestSnapshot(table, branch).deleteManifests(table.io()).size());
    ManifestFile deletes = latestSnapshot(table, branch).deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(latestSnapshot(table, branch).snapshotId()),
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = latestSnapshot(table, branch);

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
    commit(table, table.newDelete().deleteFile(dataFile1), branch);

    Assertions.assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");
  }

  @Test
  public void testAddDeleteFilesMultipleSpecs() {
    // enable partition summaries
    table.updateProperties().set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "10").commit();

    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // evolve the spec and add a new partition field
    table.updateSpec().addField("data").commit();

    // append a data file with the new spec
    DataFile thirdSnapshotDataFile = newDataFile("data=abc");
    commit(table, table.newAppend().appendFile(thirdSnapshotDataFile), branch);

    Assert.assertEquals("Should have 3 specs", 3, table.specs().size());

    // commit a row delta with 1 data file and 3 delete files where delete files have different
    // specs
    DataFile dataFile = newDataFile("data=xyz");
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");
    DeleteFile thirdDeleteFile = newDeleteFile(thirdSnapshotDataFile.specId(), "data=abc");

    commit(
        table,
        table
            .newRowDelta()
            .addRows(dataFile)
            .addDeletes(firstDeleteFile)
            .addDeletes(secondDeleteFile)
            .addDeletes(thirdDeleteFile),
        branch);

    Snapshot snapshot = latestSnapshot(table, branch);
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
        dataSeqs(4L),
        fileSeqs(4L),
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
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(firstDeleteFile),
        statuses(Status.ADDED));

    ManifestFile secondDeleteManifest = snapshot.deleteManifests(table.io()).get(1);
    Assert.assertEquals(
        "Spec must match", secondSnapshotDataFile.specId(), secondDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        secondDeleteManifest,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(secondDeleteFile),
        statuses(Status.ADDED));

    ManifestFile thirdDeleteManifest = snapshot.deleteManifests(table.io()).get(0);
    Assert.assertEquals(
        "Spec must match", thirdSnapshotDataFile.specId(), thirdDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        thirdDeleteManifest,
        dataSeqs(4L),
        fileSeqs(4L),
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
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // commit two delete files to two specs in a single operation
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    commit(
        table,
        table.newRowDelta().addDeletes(firstDeleteFile).addDeletes(secondDeleteFile),
        branch);

    Snapshot thirdSnapshot = latestSnapshot(table, branch);

    // 2 appends and 1 row delta where delete files belong to different specs
    Assert.assertEquals(
        "Should have 2 data manifest", 2, thirdSnapshot.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should have 2 delete manifest", 2, thirdSnapshot.deleteManifests(table.io()).size());

    // commit two more delete files to the same specs to trigger merging
    DeleteFile thirdDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile fourthDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    commit(
        table,
        table.newRowDelta().addDeletes(thirdDeleteFile).addDeletes(fourthDeleteFile),
        branch);

    Snapshot fourthSnapshot = latestSnapshot(table, branch);

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
        dataSeqs(4L, 3L),
        fileSeqs(4L, 3L),
        ids(fourthSnapshot.snapshotId(), thirdSnapshot.snapshotId()),
        files(thirdDeleteFile, firstDeleteFile),
        statuses(Status.ADDED, Status.EXISTING));

    ManifestFile secondDeleteManifest = fourthSnapshot.deleteManifests(table.io()).get(0);
    Assert.assertEquals(
        "Spec must match", secondSnapshotDataFile.specId(), secondDeleteManifest.partitionSpecId());
    validateDeleteManifest(
        secondDeleteManifest,
        dataSeqs(4L, 3L),
        fileSeqs(4L, 3L),
        ids(fourthSnapshot.snapshotId(), thirdSnapshot.snapshotId()),
        files(fourthDeleteFile, secondDeleteFile),
        statuses(Status.ADDED, Status.EXISTING));
  }

  @Test
  public void testAbortMultipleSpecs() {
    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    Assert.assertTrue("Spec must be unpartitioned", table.spec().isUnpartitioned());

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // prepare two delete files that belong to different specs
    DeleteFile firstDeleteFile = newDeleteFile(firstSnapshotDataFile.specId(), "data_bucket=0");
    DeleteFile secondDeleteFile = newDeleteFile(secondSnapshotDataFile.specId(), "");

    // capture all deletes
    Set<String> deletedFiles = Sets.newHashSet();

    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addDeletes(firstDeleteFile)
            .addDeletes(secondDeleteFile)
            .deleteWith(deletedFiles::add)
            .validateDeletedFiles()
            .validateDataFilesExist(ImmutableList.of(firstSnapshotDataFile.path()));

    rowDelta.apply();

    // perform a conflicting concurrent operation
    commit(table, table.newDelete().deleteFile(firstSnapshotDataFile), branch);

    Assertions.assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    // we should clean up 1 manifest list and 2 delete manifests
    Assert.assertEquals("Should delete 3 files", 3, deletedFiles.size());
  }

  @Test
  public void testConcurrentConflictingRowDelta() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    Expression conflictDetectionFilter = Expressions.alwaysTrue();

    // mock a MERGE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addRows(FILE_B)
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    table
        .newRowDelta()
        .toBranch(branch)
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    Assertions.assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
  }

  @Test
  public void testConcurrentConflictingRowDeltaWithoutAppendValidation() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

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
        .toBranch(branch)
        .addDeletes(FILE_A_DELETES)
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    Assertions.assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

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
            .toBranch(branch)
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
        .toBranch(branch)
        .addDeletes(deleteFile2)
        .validateFromSnapshot(baseSnapshot.snapshotId())
        .commit();

    commit(table, rowDelta, branch);

    validateBranchDeleteFiles(table, branch, deleteFile1, deleteFile2);
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add an equality delete file
    DeleteFile deleteFile1 =
        newEqualityDeleteFile(
            table.spec().specId(), "data=a", table.schema().asStruct().fields().get(0).fieldId());

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
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

    commit(table, rowDelta, branch);
    commit(table, rewriteFiles, branch);

    validateBranchDeleteFiles(table, branch, deleteFile1);
    validateBranchFiles(table, branch, dataFile2);
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

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

    commit(table, rowDelta, branch);
    commit(table, rewriteFiles, branch);

    table.refresh();
    List<ManifestFile> dataManifests = latestSnapshot(table, branch).dataManifests(table.io());
    Assert.assertEquals("should have 1 data manifest", 1, dataManifests.size());
    ManifestFile mergedDataManifest = dataManifests.get(0);
    Assert.assertEquals("Manifest seq number must match", 3L, mergedDataManifest.sequenceNumber());

    long currentSnapshotId = latestSnapshot(table, branch).snapshotId();

    validateManifest(
        mergedDataManifest,
        dataSeqs(1L, 1L),
        fileSeqs(3L, 1L),
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

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

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

    commit(table, rowDelta, branch);

    Assertions.assertThatThrownBy(() -> commit(table, rewriteFiles, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, found new position delete for replaced data file");
  }

  @Test
  public void testRowDeltaCaseSensitivity() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_A2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    commit(table, table.newRowDelta().addDeletes(FILE_A_DELETES), branch);

    Expression conflictDetectionFilter = Expressions.equal(Expressions.bucket("dAtA", 16), 0);

    Assertions.assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtA'");

    Assertions.assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .caseSensitive(true)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtA'");

    // binding should succeed and trigger the validation
    Assertions.assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .caseSensitive(false)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
  }
}
