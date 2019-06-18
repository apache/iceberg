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

import com.google.common.collect.Sets;
import java.io.File;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.Assert;
import org.junit.Test;

public class TestTransaction extends TableTestBase {
  @Test
  public void testEmptyTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();
    txn.commitTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0", 0, (int) version());
  }

  @Test
  public void testSingleOperationTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertSame("Base metadata should not change when an append is committed",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after append", 0, (int) version());

    txn.commitTransaction();

    validateSnapshot(base.currentSnapshot(), readMetadata().currentSnapshot(), FILE_A, FILE_B);
    Assert.assertEquals("Table should be on version 1 after commit", 1, (int) version());
  }

  @Test
  public void testMultipleOperationTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    Assert.assertSame("Base metadata should not change when an append is committed",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after append", 0, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 1 after commit", 1, (int) version());
    Assert.assertEquals("Table should have one manifest after commit",
        1, readMetadata().currentSnapshot().manifests().size());
    Assert.assertEquals("Table snapshot should be the delete snapshot",
        deleteSnapshot, readMetadata().currentSnapshot());
    validateManifestEntries(readMetadata().currentSnapshot().manifests().get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B), statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals("Table should have a snapshot for each operation",
        2, readMetadata().snapshots().size());
    validateManifestEntries(readMetadata().snapshots().get(0).manifests().get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B), statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testMultipleOperationTransactionFromTable() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.table().newDelete()
        .deleteFile(FILE_A)
        .commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    Assert.assertSame("Base metadata should not change when an append is committed",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after append", 0, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 1 after commit", 1, (int) version());
    Assert.assertEquals("Table should have one manifest after commit",
        1, readMetadata().currentSnapshot().manifests().size());
    Assert.assertEquals("Table snapshot should be the delete snapshot",
        deleteSnapshot, readMetadata().currentSnapshot());
    validateManifestEntries(readMetadata().currentSnapshot().manifests().get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B), statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals("Table should have a snapshot for each operation",
        2, readMetadata().snapshots().size());
    validateManifestEntries(readMetadata().snapshots().get(0).manifests().get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B), statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testDetectsUncommittedChange() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class,
        "Cannot create new DeleteFiles: last operation has not committed",
        txn::newDelete);
  }

  @Test
  public void testDetectsUncommittedChangeOnCommit() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    AssertHelpers.assertThrows("Should reject commit when last operation has not committed",
        IllegalStateException.class,
        "Cannot commit transaction: last operation has not committed",
        txn::commitTransaction);
  }

  @Test
  public void testTransactionConflict() {
    // set retries to 0 to catch the failure
    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    AssertHelpers.assertThrows("Transaction commit should fail",
        CommitFailedException.class, "Injected failure", txn::commitTransaction);
  }

  @Test
  public void testTransactionRetry() {
    // use only one retry
    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Set<ManifestFile> appendManifests = Sets.newHashSet(txn.table().currentSnapshot().manifests());

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 2 after commit", 2, (int) version());

    Assert.assertEquals("Should reuse manifests from initial append commit",
        appendManifests, Sets.newHashSet(table.currentSnapshot().manifests()));
  }

  @Test
  public void testTransactionRetryMergeAppend() {
    // use only one retry
    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Set<ManifestFile> appendManifests = Sets.newHashSet(txn.table().currentSnapshot().manifests());

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests = Sets.newHashSet(table.currentSnapshot().manifests());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> expectedManifests = Sets.newHashSet();
    expectedManifests.addAll(appendManifests);
    expectedManifests.addAll(conflictAppendManifests);

    Assert.assertEquals("Should reuse manifests from initial append commit and conflicting append",
        expectedManifests, Sets.newHashSet(table.currentSnapshot().manifests()));
  }

  @Test
  public void testMultipleUpdateTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.updateProperties()
        .set("test-property", "test-value")
        .commit();

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertEquals("Append should create one manifest",
        1, txn.table().currentSnapshot().manifests().size());
    ManifestFile appendManifest = txn.table().currentSnapshot().manifests().get(0);

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests = Sets.newHashSet(table.currentSnapshot().manifests());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    Assert.assertEquals("Should merge both commit manifests into a single manifest",
        1, table.currentSnapshot().manifests().size());
    Assert.assertFalse("Should merge both commit manifests into a new manifest",
        previousManifests.contains(table.currentSnapshot().manifests().get(0)));

    Assert.assertFalse("Append manifest should be deleted", new File(appendManifest.path()).exists());
  }

  @Test
  public void testTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Assert.assertEquals("Append should create one manifest",
        1, txn.table().currentSnapshot().manifests().size());
    ManifestFile appendManifest = txn.table().currentSnapshot().manifests().get(0);

    Assert.assertSame("Base metadata should not change when commit is created",
        base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests = Sets.newHashSet(table.currentSnapshot().manifests());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    Assert.assertEquals("Should merge both commit manifests into a single manifest",
        1, table.currentSnapshot().manifests().size());
    Assert.assertFalse("Should merge both commit manifests into a new manifest",
        previousManifests.contains(table.currentSnapshot().manifests().get(0)));

    Assert.assertFalse("Append manifest should be deleted", new File(appendManifest.path()).exists());
  }

  @Test
  public void testTransactionFastAppends() {
    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Transaction txn = table.newTransaction();

    txn.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    txn.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    txn.commitTransaction();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals("Expected 2 manifests", 2, manifests.size());
  }
}
