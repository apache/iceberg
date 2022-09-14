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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTransaction extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testEmptyTransaction() {
    assertThat(version()).isEqualTo(0);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();
    txn.commitTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);
  }

  @TestTemplate
  public void testSingleOperationTransaction() {
    assertThat(version()).isEqualTo(0);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.commitTransaction();

    validateSnapshot(base.currentSnapshot(), readMetadata().currentSnapshot(), FILE_A, FILE_B);
    assertThat(version()).isEqualTo(1);
  }

  @TestTemplate
  public void testMultipleOperationTransaction() {
    assertThat(version()).isEqualTo(0);

    table.newAppend().appendFile(FILE_C).commit();
    List<HistoryEntry> initialHistory = table.history();

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.commitTransaction();

    assertThat(version()).isEqualTo(2);
    assertThat(readMetadata().currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(readMetadata().currentSnapshot()).isEqualTo(deleteSnapshot);
    validateManifestEntries(
        readMetadata().currentSnapshot().allManifests(table.io()).get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    assertThat(readMetadata().snapshots()).hasSize(3);
    validateManifestEntries(
        readMetadata().snapshots().get(1).allManifests(table.io()).get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    assertThat(table.history()).containsAll(initialHistory);
  }

  @TestTemplate
  public void testMultipleOperationTransactionFromTable() {
    assertThat(version()).isEqualTo(0);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    Snapshot appendSnapshot = txn.table().currentSnapshot();

    txn.table().newDelete().deleteFile(FILE_A).commit();

    Snapshot deleteSnapshot = txn.table().currentSnapshot();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.commitTransaction();

    assertThat(version()).isEqualTo(1);
    assertThat(readMetadata().currentSnapshot().allManifests(table.io())).hasSize(1);
    assertThat(readMetadata().currentSnapshot()).isEqualTo(deleteSnapshot);
    validateManifestEntries(
        readMetadata().currentSnapshot().allManifests(table.io()).get(0),
        ids(deleteSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    assertThat(readMetadata().snapshots()).hasSize(2);
    validateManifestEntries(
        readMetadata().snapshots().get(0).allManifests(table.io()).get(0),
        ids(appendSnapshot.snapshotId(), appendSnapshot.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testDetectsUncommittedChange() {
    assertThat(version()).isEqualTo(0);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    assertThatThrownBy(txn::newDelete)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create new DeleteFiles: last operation has not committed");
  }

  @TestTemplate
  public void testDetectsUncommittedChangeOnCommit() {
    assertThat(version()).isEqualTo(0);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B); // not committed

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot commit transaction: last operation has not committed");
  }

  @TestTemplate
  public void testTransactionConflict() {
    // set retries to 0 to catch the failure
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();

    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
  }

  @TestTemplate
  public void testTransactionRetry() {
    // use only one retry
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Set<ManifestFile> appendManifests =
        Sets.newHashSet(txn.table().currentSnapshot().allManifests(table.io()));

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    // cause the transaction commit to fail
    table.ops().failCommits(1);

    txn.commitTransaction();

    assertThat(version()).isEqualTo(2);

    assertThat(Sets.newHashSet(table.currentSnapshot().allManifests(table.io())))
        .isEqualTo(appendManifests);
  }

  @TestTemplate
  public void testTransactionRetryMergeAppend() {
    // use only one retry
    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Set<ManifestFile> appendManifests =
        Sets.newHashSet(txn.table().currentSnapshot().allManifests(table.io()));

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    assertThat(version()).isEqualTo(2);

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    assertThat(version()).isEqualTo(3);

    Set<ManifestFile> expectedManifests = Sets.newHashSet();
    expectedManifests.addAll(appendManifests);
    expectedManifests.addAll(conflictAppendManifests);

    assertThat(Sets.newHashSet(table.currentSnapshot().allManifests(table.io())))
        .isEqualTo(expectedManifests);
  }

  @TestTemplate
  public void testMultipleUpdateTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.updateProperties().set("test-property", "test-value").commit();

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(txn.table().currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile appendManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    assertThat(version()).isEqualTo(2);

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    assertThat(version()).isEqualTo(3);

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .hasSize(1)
        .doesNotContainAnyElementsOf(previousManifests);

    assertThat(new File(appendManifest.path())).doesNotExist();
  }

  @TestTemplate
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

    assertThat(schemaId).isEqualTo(conflictingSchemaId);

    // commit the transaction for adding "new-column"
    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Table metadata refresh is required");
  }

  @TestTemplate
  public void testTransactionRetryMergeCleanup() {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    assertThat(version()).isEqualTo(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(txn.table().currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile appendManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(1);

    // cause the transaction commit to fail
    table.newAppend().appendFile(FILE_C).appendFile(FILE_D).commit();

    assertThat(version()).isEqualTo(2);

    Set<ManifestFile> conflictAppendManifests =
        Sets.newHashSet(table.currentSnapshot().allManifests(table.io()));

    txn.commitTransaction();

    assertThat(version()).isEqualTo(3);

    Set<ManifestFile> previousManifests = Sets.newHashSet();
    previousManifests.add(appendManifest);
    previousManifests.addAll(conflictAppendManifests);

    assertThat(table.currentSnapshot().allManifests(table.io()))
        .hasSize(1)
        .doesNotContainAnyElementsOf(previousManifests);
    assertThat(new File(appendManifest.path())).doesNotExist();
  }

  @TestTemplate
  public void testTransactionRetryAndAppendManifestsWithoutSnapshotIdInheritance()
      throws Exception {
    // this test assumes append manifests are rewritten, which only happens in V1 tables
    assumeThat(formatVersion).isEqualTo(1);

    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .commit();

    assertThat(version()).isEqualTo(1);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile v1manifest = table.currentSnapshot().allManifests(table.io()).get(0);

    TableMetadata base = readMetadata();

    // create a manifest append
    OutputFile manifestLocation = Files.localOutput("/tmp/" + UUID.randomUUID() + ".avro");
    ManifestWriter<DataFile> writer = ManifestFiles.write(table.spec(), manifestLocation);
    try {
      writer.add(FILE_D);
    } finally {
      writer.close();
    }

    Transaction txn = table.newTransaction();

    txn.newAppend().appendManifest(writer.toManifestFile()).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(2);

    assertThat(txn.table().currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile mergedManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    // find the initial copy of the appended manifest
    String copiedAppendManifest =
        Iterables.getOnlyElement(
            Iterables.filter(
                Iterables.transform(listManifestFiles(), File::getPath),
                path ->
                    !v1manifest.path().contains(path) && !mergedManifest.path().contains(path)));

    assertThat(((BaseTransaction) txn).deletedFiles())
        .as("Transaction should hijack the delete of the original copied manifest")
        .contains(copiedAppendManifest);
    assertThat(new File(copiedAppendManifest)).exists();

    // cause the transaction commit to fail and retry
    table.newAppend().appendFile(FILE_C).commit();

    assertThat(version()).isEqualTo(3);

    txn.commitTransaction();

    assertThat(version()).isEqualTo(4);

    assertThat(((BaseTransaction) txn).deletedFiles())
        .as("Transaction should hijack the delete of the original copied manifest")
        .contains(copiedAppendManifest);

    assertThat(new File(copiedAppendManifest)).doesNotExist();
    assertThat(((BaseTransaction) txn).deletedFiles())
        .as("Transaction should hijack the delete of the first merged manifest")
        .contains(mergedManifest.path());
    assertThat(new File(mergedManifest.path())).doesNotExist();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
  }

  @TestTemplate
  public void testTransactionRetryAndAppendManifestsWithSnapshotIdInheritance() throws Exception {
    // use only one retry and aggressively merge manifests
    table
        .updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "1")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0")
        .set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true")
        .commit();

    assertThat(version()).isEqualTo(1);

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(version()).isEqualTo(2);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

    TableMetadata base = readMetadata();

    Transaction txn = table.newTransaction();

    ManifestFile appendManifest = writeManifestWithName("input.m0", FILE_D);
    txn.newAppend().appendManifest(appendManifest).commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(2);

    assertThat(txn.table().currentSnapshot().allManifests(table.io())).hasSize(1);
    ManifestFile mergedManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);

    // cause the transaction commit to fail and retry
    table.newAppend().appendFile(FILE_C).commit();

    assertThat(version()).isEqualTo(3);

    txn.commitTransaction();

    assertThat(version()).isEqualTo(4);

    assertThat(((BaseTransaction) txn).deletedFiles())
        .as("Transaction should hijack the delete of the original append manifest")
        .contains(appendManifest.path());
    assertThat(new File(appendManifest.path())).doesNotExist();

    assertThat(((BaseTransaction) txn).deletedFiles())
        .as("Transaction should hijack the delete of the first merged manifest")
        .contains(mergedManifest.path());
    assertThat(new File(appendManifest.path())).doesNotExist();

    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);
  }

  @TestTemplate
  public void testTransactionNoCustomDeleteFunc() {
    assertThatThrownBy(
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

  @TestTemplate
  public void testTransactionFastAppends() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    Transaction txn = table.newTransaction();

    txn.newFastAppend().appendFile(FILE_A).commit();

    txn.newFastAppend().appendFile(FILE_B).commit();

    txn.commitTransaction();

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());
    assertThat(manifests).hasSize(2);
  }

  @TestTemplate
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
    assertThat(manifests).hasSize(2);

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

    assertThat(new File(newManifest.path())).exists();

    List<ManifestFile> finalManifests = table.currentSnapshot().allManifests(table.io());
    assertThat(finalManifests).hasSize(1);

    validateManifestEntries(
        finalManifests.get(0),
        ids(finalSnapshotId, firstSnapshotId, secondSnapshotId),
        files(FILE_C, FILE_A, FILE_B),
        statuses(
            ManifestEntry.Status.ADDED,
            ManifestEntry.Status.EXISTING,
            ManifestEntry.Status.EXISTING));

    table.expireSnapshots().expireOlderThan(finalSnapshotTimestamp + 1).retainLast(1).commit();

    assertThat(new File(newManifest.path())).doesNotExist();
  }

  @TestTemplate
  public void testSimpleTransactionNotDeletingMetadataOnUnknownSate() throws IOException {
    Table table = TestTables.tableWithCommitSucceedButStateUnknown(tableDir, "test");

    Transaction transaction = table.newTransaction();
    transaction.newAppend().appendFile(FILE_A).commit();

    assertThatThrownBy(transaction::commitTransaction)
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("datacenter on fire");

    // Make sure metadata files still exist
    Snapshot current = table.currentSnapshot();
    List<ManifestFile> manifests = current.allManifests(table.io());
    assertThat(manifests).hasSize(1);
    assertThat(new File(manifests.get(0).path())).exists();
    assertThat(countAllMetadataFiles(tableDir)).isEqualTo(2);
  }

  @TestTemplate
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

    assertThat(pending.allManifests(table.io())).hasSize(1);

    // because a merge happened, the appended manifest is deleted the by append operation
    append.commit();

    // concurrently commit FILE_A without a transaction to cause the previous append to retry
    table.newAppend().appendFile(FILE_C).commit();
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(1);

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

    assertThat(paths).isEqualTo(expectedPaths);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testSimpleConcurrentTransactionWithCommitRollbackOnBranch() {
    assumeThat(formatVersion == 2).isEqualTo(true);
    Table table = load();
    String branchName = "testBranch";
    // add table property
    table
        .updateProperties()
        .set(TableProperties.COMMIT_ALLOW_REPLACE_ROLLBACK_ENABLED, "true")
        .commit();

    table.newAppend().appendFile(FILE_A).commit();
    // create testBranch
    table.manageSnapshots().createBranch(branchName, table.currentSnapshot().snapshotId()).commit();

    // start a transaction
    // apply updates to metadata so that they are not conflicting in themselves on the present base.
    // on refreshing the base / current and then applying updates on top causes conflict
    // add a new update to rollback snapshot to parent and then re-apply updates
    Transaction transaction = table.newTransaction();

    // position delete of FILE_A
    transaction
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .toBranch(branchName)
        .commit();

    // don't close transaction
    // remove FILE_A and rewrite FILE_B
    table
        .newRewrite()
        .rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_B))
        .toBranch(branchName)
        .commit();

    // now commit transaction should conflict as FILE_A is deleted.
    transaction.commitTransaction();

    table.refresh();
    Snapshot currentSnapshot = table.snapshot(table.refs().get(branchName).snapshotId());
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      // no snapshot in the hierarchy for REPLACE operations
      assertThat(currentSnapshot.operation()).isNotEqualTo(DataOperations.REPLACE);
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }

    assertThat(totalSnapshots).isEqualTo(2);
  }

  @TestTemplate
  public void testSimpleConcurrentTransactionWithCommitRollback() {
    assumeThat(formatVersion == 2).isEqualTo(true);
    Table table = load();
    // add table property
    table
        .updateProperties()
        .set(TableProperties.COMMIT_ALLOW_REPLACE_ROLLBACK_ENABLED, "true")
        .commit();
    table.newAppend().appendFile(FILE_A).commit();

    // start a transaction
    // apply updates to metadata so that they are not conflicting in themselves on the present base.
    // on refreshing the base / current and then applying updates on top causes conflict
    // add a new update to rollback snapshot to parent and then re-apply updates
    Transaction transaction = table.newTransaction();

    // position delete of FILE_A
    transaction
        .newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .validateDataFilesExist(ImmutableList.of(FILE_A.path()))
        .commit();

    // don't close transaction
    // remove FILE_A and rewrite FILE_B
    table.newRewrite().rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_B)).commit();

    // now commit transaction should conflict as FILE_A is deleted.
    transaction.commitTransaction();

    table.refresh();
    Snapshot currentSnapshot = table.currentSnapshot();
    int totalSnapshots = 1;
    while (currentSnapshot.parentId() != null) {
      // no snapshot in the hierarchy for REPLACE operations
      assertThat(currentSnapshot.operation()).isNotEqualTo(DataOperations.REPLACE);
      currentSnapshot = table.snapshot(currentSnapshot.parentId());
      totalSnapshots += 1;
    }

    assertThat(totalSnapshots).isEqualTo(2);
  }
}
