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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.Assumptions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTransaction extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestTransaction(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();
    txn.commitTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0", 0, (int) version());
  }

  @Test
  public void testSingleOperationTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertSame(
        "Base metadata should not change when an append is committed", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after append", 0, (int) version());

    txn.commitTransaction();

    validateSnapshot(base.currentSnapshot(), readMetadata().currentSnapshot(), FILE_A, FILE_B);
    Assert.assertEquals("Table should be on version 1 after commit", 1, (int) version());
  }

  @Test
  public void testMultipleOperationTransaction() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    table.newAppend().appendFile(FILE_C).commit();
    List<HistoryEntry> initialHistory = table.history();

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    Assert.assertSame(
        "Base metadata should not change when an append is committed", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 2 after commit", 2, (int) version());
    Assert.assertEquals(
        "Table should have two manifest after commit",
        2,
        readMetadata().currentSnapshot().allManifests(table.io()).size());
    Assert.assertEquals(
        "Table snapshot should be the delete snapshot",
        deleteSnapshot,
        readMetadata().currentSnapshot());
    validateManifestEntries(
        readMetadata().currentSnapshot().allManifests(table.io()).get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals(
        "Table should have a snapshot for each operation", 3, readMetadata().snapshots().size());
    validateManifestEntries(
        readMetadata().snapshots().get(1).allManifests(table.io()).get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    Assertions.assertThat(table.history()).containsAll(initialHistory);
  }

  @Test
  public void testMultipleOperationTransactionFromTable() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.table().newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    Assert.assertSame(
        "Base metadata should not change when an append is committed", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after append", 0, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 1 after commit", 1, (int) version());
    Assert.assertEquals(
        "Table should have one manifest after commit",
        1,
        readMetadata().currentSnapshot().allManifests(table.io()).size());
    Assert.assertEquals(
        "Table snapshot should be the delete snapshot",
        deleteSnapshot,
        readMetadata().currentSnapshot());
    validateManifestEntries(
        readMetadata().currentSnapshot().allManifests(table.io()).get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Assert.assertEquals(
        "Table should have a snapshot for each operation", 2, readMetadata().snapshots().size());
    validateManifestEntries(
        readMetadata().snapshots().get(0).allManifests(table.io()).get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @Test
  public void testDetectsUncommittedChange() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    Assertions.assertThatThrownBy(txn::newDelete)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create new DeleteFiles: last operation has not committed");
  }

  @Test
  public void testDetectsUncommittedChangeOnCommit() {
    Assert.assertEquals("Table should be on version 0", 0, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 0 after txn create", 0, (int) version());

    Assertions.assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot commit transaction: last operation has not committed");
  }

  @Test
  public void testTransactionConflict() {
    // set retries to 0 to catch the failure
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    Assertions.assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
  }

  @Test
  public void testTransactionRetry() {
    // use only one retry
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Set<ManifestFile> appendManifests =
        Sets.newHashSet(txn.table().currentSnapshot().allManifests(table.io()));

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 2 after commit", 2, (int) version());

    Assert.assertEquals(
        "Should reuse manifests from initial append commit",
        appendManifests,
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io())));
  }

  @Test
  public void testTransactionRetryMergeAppend() {
    // use only one retry
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Set<ManifestFile> appendManifests =
        Sets.newHashSet(txn.table().currentSnapshot().allManifests(table.io()));

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> expectedManifests = Sets.newHashSet();
    expectedManifests.addAll(appendManifests);
    expectedManifests.addAll(conflictAppendManifests);

    Assert.assertEquals(
        "Should reuse manifests from initial append commit and conflicting append",
        expectedManifests,
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io())));
  }

  @Test
  public void testMultipleUpdateTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.updateProperties().set("test-property", "test-value").commit();

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertEquals(
        "Append should create one manifest",
        1,
        txn.table().currentSnapshot().allManifests(table.io()).size());
    ManifestFile appendManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    Assert.assertEquals(
        "Should merge both commit manifests into a single manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());
    Assert.assertFalse(
        "Should merge both commit manifests into a new manifest",
        previousManifests.contains(table.currentSnapshot().allManifests(table.io()).get(0)));

    Assert.assertFalse(
        "Append manifest should be deleted", new File(appendManifest.path()).exists());
  }

  @Test
  public void testTransactionRetrySchemaUpdate() {
    // use only one retry
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    // start a transaction
    Transaction txn = table.newTransaction();
    // add column "new-column"
    txn.updateSchema().addColumn("new-column", Types.IntegerType.get()).commit();
    int schemaId = txn.table().schema().schemaId();

    // directly update the table for adding "another-column" (which causes in-progress txn commit
    // fail)
    table.updateSchema().addColumn("another-column", Types.IntegerType.get()).commit();
    int conflictingSchemaId = table.schema().schemaId();

    Assert.assertEquals(
        "Both schema IDs should be the same in order to cause a conflict",
        conflictingSchemaId,
        schemaId);

    // commit the transaction for adding "new-column"
    Assertions.assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Table metadata refresh is required");
  }

  @Test
  public void testTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after txn create", 1, (int) version());

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertEquals(
        "Append should create one manifest",
        1,
        txn.table().currentSnapshot().allManifests(table.io()).size());
    ManifestFile appendManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 1 after append", 1, (int) version());

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    Assert.assertEquals("Table should be on version 2 after real append", 2, (int) version());

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 3 after commit", 3, (int) version());

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    Assert.assertEquals(
        "Should merge both commit manifests into a single manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());
    Assert.assertFalse(
        "Should merge both commit manifests into a new manifest",
        previousManifests.contains(table.currentSnapshot().allManifests(table.io()).get(0)));

    Assert.assertFalse(
        "Append manifest should be deleted", new File(appendManifest.path()).exists());
  }

  @Test
  public void testTransactionRetryAndAppendManifestsWithoutSnapshotIdInheritance()
      throws Exception {
    // this test assumes append manifests are rewritten, which only happens in V1 tables
    Assumptions.assumeThat(formatVersion).isEqualTo(1);

    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertEquals("Table should be on version 2 after append", 2, (int) version());
    Assert.assertEquals(
        "Append should create one manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());
    ManifestFile v1manifest = table.currentSnapshot().allManifests(table.io()).get(0);

    TableMetadata base = readMetadata();

    // create a manifest append
    OutputFile manifestLocation =
        Files.localOutput("/tmp/" + UUID.randomUUID().toString() + ".avro");
    ManifestWriter<DataFile> writer = ManifestFiles.write(table.spec(), manifestLocation);
    try {
      writer.add(FILE_D);
    } finally {
      writer.close();
    }

    Transaction txn = table.newTransaction();

    txn.newAppend().appendManifest(writer.toManifestFile()).commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 2 after txn create", 2, (int) version());

    Assert.assertEquals(
        "Append should have one merged manifest",
        1,
        txn.table().currentSnapshot().allManifests(table.io()).size());
    ManifestFile mergedManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    // find the initial copy of the appended manifest
    String copiedAppendManifest =
        Iterables.getOnlyElement(
            Iterables.filter(
                Iterables.transform(listManifestFiles(), File::getPath),
                path ->
                    !v1manifest.path().contains(path) && !mergedManifest.path().contains(path)));

    Assert.assertTrue(
        "Transaction should hijack the delete of the original copied manifest",
        ((BaseTransaction) txn).deletedFiles().contains(copiedAppendManifest));
    Assert.assertTrue(
        "Copied append manifest should not be deleted yet",
        new File(copiedAppendManifest).exists());

    // cause the transaction commit to fail and retry
    table.newAppend().appendFile(FILE_C).commit();

    Assert.assertEquals("Table should be on version 3 after real append", 3, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 4 after commit", 4, (int) version());

    Assert.assertTrue(
        "Transaction should hijack the delete of the original copied manifest",
        ((BaseTransaction) txn).deletedFiles().contains(copiedAppendManifest));
    Assert.assertFalse(
        "Append manifest should be deleted", new File(copiedAppendManifest).exists());
    Assert.assertTrue(
        "Transaction should hijack the delete of the first merged manifest",
        ((BaseTransaction) txn).deletedFiles().contains(mergedManifest.path()));
    Assert.assertFalse(
        "Append manifest should be deleted", new File(mergedManifest.path()).exists());

    Assert.assertEquals(
        "Should merge all commit manifests into a single manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testTransactionRetryAndAppendManifestsWithSnapshotIdInheritance() throws Exception {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    Assert.assertEquals("Table should be on version 1", 1, (int) version());

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Assert.assertEquals("Table should be on version 2 after append", 2, (int) version());
    Assert.assertEquals(
        "Append should create one manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    ManifestFile appendManifest = writeManifestWithName("input.m0", FILE_D);
    txn.newAppend().appendManifest(appendManifest).commit();

    Assert.assertSame(
        "Base metadata should not change when commit is created", base, readMetadata());
    Assert.assertEquals("Table should be on version 2 after txn create", 2, (int) version());

    Assert.assertEquals(
        "Append should have one merged manifest",
        1,
        txn.table().currentSnapshot().allManifests(table.io()).size());
    ManifestFile mergedManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    // cause the transaction commit to fail and retry
    table.newAppend().appendFile(FILE_C).commit();

    Assert.assertEquals("Table should be on version 3 after real append", 3, (int) version());

    txn.commitTransaction();

    Assert.assertEquals("Table should be on version 4 after commit", 4, (int) version());

    Assert.assertTrue(
        "Transaction should hijack the delete of the original append manifest",
        ((BaseTransaction) txn).deletedFiles().contains(appendManifest.path()));
    Assert.assertFalse(
        "Append manifest should be deleted", new File(appendManifest.path()).exists());

    Assert.assertTrue(
        "Transaction should hijack the delete of the first merged manifest",
        ((BaseTransaction) txn).deletedFiles().contains(mergedManifest.path()));
    Assert.assertFalse(
        "Merged append manifest should be deleted", new File(mergedManifest.path()).exists());

    Assert.assertEquals(
        "Should merge all commit manifests into a single manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());
  }

  @Test
  public void testTransactionNoCustomDeleteFunc() {
    Assertions.assertThatThrownBy(
            () ->
                table
                    .newTransaction()
                    .newAppend()
                    .appendFile(FILE_A)
                    .appendFile(FILE_B)
                    .deleteWith(file -> {}))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set delete callback more than once");
  }

  @Test
  public void testTransactionFastAppends() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    Transaction txn = table.newTransaction();

    txn.newFastAppend().appendFile(FILE_A).commit();

    txn.newFastAppend().appendFile(FILE_B).commit();

    txn.commitTransaction();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    Assert.assertEquals("Expected 2 manifests", 2, manifests.size());
  }

  @Test
  public void testTransactionRewriteManifestsAppendedDirectly() throws IOException {
    Table table = load();

    table
        .updateProperties()
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    table.newFastAppend().appendFile(FILE_A).commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    Assert.assertEquals("Should have 2 manifests after 2 appends", 2, manifests.size());

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshotId, FILE_A),
            manifestEntry(ManifestEntry.Status.EXISTING, secondSnapshotId, FILE_B));

    Transaction txn = table.newTransaction();
    txn.rewriteManifests()
        .deleteManifest(manifests.get(0))
        .deleteManifest(manifests.get(1))
        .addManifest(newManifest)
        .commit();
    txn.newAppend().appendFile(FILE_C).commit();
    txn.commitTransaction();

    long finalSnapshotId = table.currentSnapshot().snapshotId();
    long finalSnapshotTimestamp = System.currentTimeMillis();

    Assert.assertTrue(
        "Append manifest should not be deleted", new File(newManifest.path()).exists());

    List<ManifestFile> finalManifests = table.currentSnapshot().allManifests(table.io());
    Assert.assertEquals("Should have 1 final manifest", 1, finalManifests.size());

    validateManifestEntries(
        finalManifests.get(0),
        ids(finalSnapshotId, firstSnapshotId, secondSnapshotId),
        files(FILE_C, FILE_A, FILE_B),
        statuses(
            ManifestEntry.Status.ADDED,
            ManifestEntry.Status.EXISTING,
            ManifestEntry.Status.EXISTING));

    table.expireSnapshots().expireOlderThan(finalSnapshotTimestamp + 1).retainLast(1).commit();

    Assert.assertFalse(
        "Append manifest should be deleted on expiry", new File(newManifest.path()).exists());
  }

  @Test
  public void testSimpleTransactionNotDeletingMetadataOnUnknownSate() throws IOException {
    Table table = TestTables.tableWithCommitSucceedButStateUnknown(tableDir, "test");

    Transaction transaction = table.newTransaction();
    transaction.newAppend().appendFile(FILE_A).commit();

    Assertions.assertThatThrownBy(transaction::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("datacenter on fire");

    // Make sure metadata files still exist
    Snapshot current = table.currentSnapshot();
    List<ManifestFile> manifests = current.allManifests(table.io());
    Assert.assertEquals("Should have 1 manifest file", 1, manifests.size());
    Assert.assertTrue("Manifest file should exist", new File(manifests.get(0).path()).exists());
    Assert.assertEquals("Should have 2 files in metadata", 2, countAllMetadataFiles(tableDir));
  }

  @Test
  public void testTransactionRecommit() {
    // update table settings to merge when there are 3 manifests
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "3").commit();

    // create manifests so that the next commit will trigger a merge
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    // start a transaction with appended files that will merge
    Transaction transaction = Transactions.newTransaction(table.name(), table.ops());

    AppendFiles append = transaction.newAppend().appendFile(FILE_D);
    Snapshot pending = append.apply();

    Assert.assertEquals(
        "Should produce 1 pending merged manifest", 1, pending.allManifests(table.io()).size());

    // because a merge happened, the appended manifest is deleted the by append operation
    append.commit();

    // concurrently commit FILE_A without a transaction to cause the previous append to retry
    table.newAppend().appendFile(FILE_C).commit();
    Assert.assertEquals(
        "Should produce 1 committed merged manifest",
        1,
        table.currentSnapshot().allManifests(table.io()).size());

    transaction.commitTransaction();

    Set<String> paths =
        Sets.newHashSet(
            Iterables.transform(
                table.newScan().planFiles(), task -> task.file().path().toString()));
    Set<String> expectedPaths =
        Sets.newHashSet(
            FILE_A.path().toString(),
            FILE_B.path().toString(),
            FILE_C.path().toString(),
            FILE_D.path().toString());

    Assert.assertEquals("Should contain all committed files", expectedPaths, paths);

    Assert.assertEquals(
        "Should produce 2 manifests", 2, table.currentSnapshot().allManifests(table.io()).size());
  }
}
