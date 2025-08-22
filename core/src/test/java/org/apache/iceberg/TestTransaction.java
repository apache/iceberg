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
import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

@ExtendWith(ParameterizedTestExtension.class)
public class TestTransaction extends TestBase {

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
  public void testTransactionFailureBulkDeletionCleanup() throws IOException {
    TestTables.TestBulkLocalFileIO spyFileIO = Mockito.spy(new TestTables.TestBulkLocalFileIO());
    Mockito.doNothing().when(spyFileIO).deleteFiles(any());

    File location = java.nio.file.Files.createTempDirectory(temp, "junit").toFile();
    String tableName = "txnFailureBulkDeleteTest";
    TestTables.TestTable tableWithBulkIO =
        TestTables.create(
            location,
            tableName,
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            formatVersion,
            new TestTables.TestTableOperations(tableName, location, spyFileIO));

    // set retries to 0 to catch the failure
    tableWithBulkIO.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();

    // eagerly generate manifest and manifest-list
    Transaction txn = tableWithBulkIO.newTransaction();
    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    ManifestFile appendManifest = txn.table().currentSnapshot().allManifests(table.io()).get(0);
    String txnManifestList = txn.table().currentSnapshot().manifestListLocation();

    // cause the transaction commit to fail
    tableWithBulkIO.ops().failCommits(1);
    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    // ensure both files are deleted on transaction failure
    Mockito.verify(spyFileIO).deleteFiles(Set.of(appendManifest.path(), txnManifestList));
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
    try (writer) {
      writer.add(FILE_D);
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
  public void testSimpleTransactionNotDeletingMetadataOnUnknownSate() {
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
            Iterables.transform(table.newScan().planFiles(), task -> task.file().location()));
    Set<String> expectedPaths =
        Sets.newHashSet(FILE_A.location(), FILE_B.location(), FILE_C.location(), FILE_D.location());

    assertThat(paths).isEqualTo(expectedPaths);
    assertThat(table.currentSnapshot().allManifests(table.io())).hasSize(2);
  }

  @TestTemplate
  public void testCommitProperties() {
    table
        .updateProperties()
        .set(TableProperties.COMMIT_MAX_RETRY_WAIT_MS, "foo")
        .set(TableProperties.COMMIT_NUM_RETRIES, "bar")
        .set(TableProperties.COMMIT_TOTAL_RETRY_TIME_MS, Integer.toString(60 * 60 * 1000))
        .commit();
    table.updateProperties().remove(TableProperties.COMMIT_MAX_RETRY_WAIT_MS).commit();
    table.updateProperties().remove(TableProperties.COMMIT_NUM_RETRIES).commit();

    assertThat(table.properties())
        .doesNotContainKey(TableProperties.COMMIT_NUM_RETRIES)
        .doesNotContainKey(TableProperties.COMMIT_MAX_RETRY_WAIT_MS)
        .containsEntry(
            TableProperties.COMMIT_TOTAL_RETRY_TIME_MS, Integer.toString(60 * 60 * 1000));
  }

  @TestTemplate
  public void testRowDeltaWithConcurrentManifestRewrite() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);
    String branch = "main";
    RowDelta rowDelta = table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES);
    Snapshot first = commit(table, rowDelta, branch);

    Snapshot secondRowDelta =
        commit(table, table.newRowDelta().addRows(FILE_B).addDeletes(FILE_B_DELETES), branch);
    List<ManifestFile> secondRowDeltaDeleteManifests = secondRowDelta.deleteManifests(table.io());
    assertThat(secondRowDeltaDeleteManifests).hasSize(2);

    // Read the manifest entries before the manifest rewrite is committed so that referenced
    // manifests are populated
    List<ManifestEntry<DeleteFile>> readEntries = Lists.newArrayList();
    for (ManifestFile manifest : secondRowDeltaDeleteManifests) {
      try (ManifestReader<DeleteFile> deleteManifestReader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        deleteManifestReader.entries().forEach(readEntries::add);
      }
    }

    Transaction transaction = table.newTransaction();
    RowDelta removeDeletes =
        transaction
            .newRowDelta()
            .removeDeletes(readEntries.get(0).file())
            .removeDeletes(readEntries.get(1).file())
            .validateFromSnapshot(secondRowDelta.snapshotId());
    removeDeletes.commit();

    // cause the row delta transaction commit to fail and retry
    RewriteManifests rewriteManifests =
        table
            .rewriteManifests()
            .addManifest(
                writeManifest(
                    "new_delete_manifest.avro",
                    // Specify data sequence number so that the delete files don't get aged out
                    // first
                    manifestEntry(
                        ManifestEntry.Status.EXISTING, first.snapshotId(), 3L, 0L, FILE_A_DELETES),
                    manifestEntry(
                        ManifestEntry.Status.EXISTING,
                        secondRowDelta.snapshotId(),
                        3L,
                        0L,
                        FILE_B_DELETES)))
            .deleteManifest(secondRowDeltaDeleteManifests.get(0))
            .deleteManifest(secondRowDeltaDeleteManifests.get(1));
    commit(table, rewriteManifests, branch);

    transaction.commitTransaction();
    Snapshot removedDeletes = table.currentSnapshot();
    List<ManifestFile> deleteManifests = removedDeletes.deleteManifests(table.io());
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(3L, 3L),
        fileSeqs(0L, 0L),
        ids(removedDeletes.snapshotId(), removedDeletes.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(Status.DELETED, Status.DELETED));
  }

  @TestTemplate
  public void testOverwriteWithConcurrentManifestRewrite() throws IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(2);
    String branch = "main";
    OverwriteFiles overwrite = table.newOverwrite().addFile(FILE_A).addFile(FILE_A2);
    Snapshot first = commit(table, overwrite, branch);

    overwrite = table.newOverwrite().addFile(FILE_B);
    Snapshot second = commit(table, overwrite, branch);
    List<ManifestFile> secondOverwriteManifests = second.dataManifests(table.io());
    assertThat(secondOverwriteManifests).hasSize(2);

    // Read the manifest entries before the manifest rewrite is committed so that referenced
    // manifests are populated
    List<ManifestEntry<DataFile>> entries = Lists.newArrayList();
    for (ManifestFile manifest : secondOverwriteManifests) {
      try (ManifestReader<DataFile> manifestReader =
          ManifestFiles.read(manifest, table.io(), table.specs())) {
        manifestReader.entries().forEach(entries::add);
      }
    }

    ManifestEntry<DataFile> removedDataFileEntry =
        entries.stream()
            .filter(entry -> entry.file().location().equals(FILE_A2.location()))
            .collect(Collectors.toList())
            .get(0);

    Transaction overwriteTransaction = table.newTransaction();
    OverwriteFiles overwriteFiles =
        overwriteTransaction
            .newOverwrite()
            .deleteFile(removedDataFileEntry.file())
            .validateFromSnapshot(second.snapshotId());
    overwriteFiles.commit();

    // cause the overwrite transaction commit to fail and retry
    RewriteManifests rewriteManifests =
        table
            .rewriteManifests()
            .addManifest(
                writeManifest(
                    "new_manifest.avro",
                    manifestEntry(Status.EXISTING, first.snapshotId(), FILE_A),
                    manifestEntry(Status.EXISTING, first.snapshotId(), FILE_A2),
                    manifestEntry(Status.EXISTING, second.snapshotId(), FILE_B)))
            .deleteManifest(secondOverwriteManifests.get(0))
            .deleteManifest(secondOverwriteManifests.get(1));
    commit(table, rewriteManifests, branch);

    overwriteTransaction.commitTransaction();
    Snapshot latestOverwrite = table.currentSnapshot();
    List<ManifestFile> manifests = latestOverwrite.dataManifests(table.io());
    validateManifest(
        manifests.get(0),
        dataSeqs(0L, 0L, 0L),
        fileSeqs(0L, 0L, 0L),
        ids(first.snapshotId(), latestOverwrite.snapshotId(), second.snapshotId()),
        files(FILE_A, FILE_A2, FILE_B),
        statuses(Status.EXISTING, Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testExtendBaseTransaction() {
    assertThat(version()).isEqualTo(0);
    TableMetadata base = readMetadata();

    Transaction txn =
        new AppendToBranchTransaction(
            table.name(),
            table.ops(),
            BaseTransaction.TransactionType.SIMPLE,
            table.ops().refresh());
    AppendFiles appendFiles = txn.newAppend().appendFile(FILE_A);
    Snapshot branchSnapshot = appendFiles.apply();
    appendFiles.commit();

    assertThat(readMetadata()).isSameAs(base);
    assertThat(version()).isEqualTo(0);

    // first snapshot write to main
    table.newAppend().appendFile(FILE_B).commit();

    Snapshot mainSnapshot = readMetadata().currentSnapshot();
    assertThat(version()).isEqualTo(1);
    validateSnapshot(base.currentSnapshot(), mainSnapshot, FILE_B);

    // second snapshot write to branch
    txn.commitTransaction();
    assertThat(version()).isEqualTo(2);

    assertThat(readMetadata().refs()).hasSize(2).containsKey("main").containsKey("branch");
    assertThat(readMetadata().ref("main").snapshotId()).isEqualTo(mainSnapshot.snapshotId());
    assertThat(readMetadata().snapshot(mainSnapshot.snapshotId()).allManifests(table.io()))
        .hasSize(1);
    assertThat(readMetadata().ref("branch").snapshotId()).isEqualTo(branchSnapshot.snapshotId());
    assertThat(readMetadata().snapshot(branchSnapshot.snapshotId()).allManifests(table.io()))
        .hasSize(2);
  }

  private static class AppendToBranchTransaction extends BaseTransaction {

    AppendToBranchTransaction(
        String tableName, TableOperations ops, TransactionType type, TableMetadata start) {
      super(tableName, ops, type, start);
    }

    @Override
    public AppendFiles newAppend() {
      AppendFiles append =
          new MergeAppend(tableName(), ((HasTableOperations) table()).operations())
              .toBranch("branch");
      return appendUpdate(append);
    }
  }
}
