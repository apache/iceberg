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

import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDelta extends V2TableTestBase {
  @Test
  public void testAddDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 1", 1, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    Assert.assertEquals("Delta commit should use operation 'overwrite'", DataOperations.OVERWRITE, snap.operation());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(1),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(1, 1),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testOverwriteWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting by a filter will also remove delete files that match because all matching data files are removed.
    table.newOverwrite()
        .overwriteByRowFilter(Expressions.equal(Expressions.bucket("data", 16), 0))
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplacePartitionsWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // overwriting the partition will also remove delete files that match because all matching data files are removed.
    table.newReplacePartitions()
        .addFile(FILE_A2)
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 2 data manifests", 2, snap.dataManifests().size());
    int deleteManifestPos = snap.dataManifests().get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        snap.dataManifests().get(deleteManifestPos),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        snap.dataManifests().get(appendManifestPos),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 1),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testDeleteByExpressionWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting with a filter will also remove delete files that match because all matching data files are removed.
    table.newDelete()
        .deleteFromRowFilter(Expressions.alwaysTrue())
        .commit();

    Snapshot snap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, snap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, snap.dataManifests().size());
    validateManifest(
        snap.dataManifests().get(0),
        seqs(2),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, snap.deleteManifests().size());
    validateDeleteManifest(
        snap.deleteManifests().get(0),
        seqs(2, 2),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.DELETED));
  }

  @Test
  public void testDeleteDataFileWithDeleteFile() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, deleteSnap.dataManifests().size());
    validateManifest(
        deleteSnap.dataManifests().get(0),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, deleteSnap.deleteManifests().size());
    validateDeleteManifest(
        deleteSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next commit, causing the min sequence number of all data files
    // to be 2, the largest known sequence number. this will cause FILE_A_DELETES to be removed because it is too old
    // to apply to any data files.
    table.newDelete()
        .deleteFile("no-such-file")
        .commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 0 data manifests", 0, nextSnap.dataManifests().size());
    Assert.assertEquals("Should produce 1 delete manifest", 1, nextSnap.deleteManifests().size());
    validateDeleteManifest(
        nextSnap.deleteManifests().get(0),
        seqs(3),
        ids(nextSnap.snapshotId()),
        files(FILE_A_DELETES),
        statuses(Status.DELETED));
  }

  @Test
  public void testFastAppendDoesNotRemoveStaleDeleteFiles() {
    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .commit();

    long deltaSnapshotId = table.currentSnapshot().snapshotId();
    Assert.assertEquals("Commit should produce sequence number 1", 1, table.currentSnapshot().sequenceNumber());
    Assert.assertEquals("Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    // deleting a specific data file will not affect a delete file
    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnap = table.currentSnapshot();
    Assert.assertEquals("Commit should produce sequence number 2", 2, deleteSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should produce 1 data manifest", 1, deleteSnap.dataManifests().size());
    validateManifest(
        deleteSnap.dataManifests().get(0),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, deleteSnap.deleteManifests().size());
    validateDeleteManifest(
        deleteSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));

    // the manifest that removed FILE_A will be dropped next merging commit, but FastAppend will not remove it
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Snapshot nextSnap = table.currentSnapshot();
    Assert.assertEquals("Append should produce sequence number 3", 3, nextSnap.sequenceNumber());
    Assert.assertEquals("Last sequence number should be 3", 3, table.ops().current().lastSequenceNumber());

    Assert.assertEquals("Should have 2 data manifests", 2, nextSnap.dataManifests().size());
    int deleteManifestPos = nextSnap.dataManifests().get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        nextSnap.dataManifests().get(deleteManifestPos),
        seqs(2),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        nextSnap.dataManifests().get(appendManifestPos),
        seqs(3),
        ids(nextSnap.snapshotId()),
        files(FILE_B),
        statuses(Status.ADDED));

    Assert.assertEquals("Should produce 1 delete manifest", 1, nextSnap.deleteManifests().size());
    validateDeleteManifest(
        nextSnap.deleteManifests().get(0),
        seqs(1),
        ids(deltaSnapshotId),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }
}
