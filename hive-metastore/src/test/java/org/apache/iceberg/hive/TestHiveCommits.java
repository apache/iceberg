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
package org.apache.iceberg.hive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;

public class TestHiveCommits extends HiveTableBaseTest {

  @Test
  public void testSuppressUnlockExceptions() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    AtomicReference<HiveLock> lockRef = new AtomicReference<>();

    when(spyOps.lockObject(metadataV1))
        .thenAnswer(
            i -> {
              HiveLock lock = (HiveLock) i.callRealMethod();
              lockRef.set(lock);
              return lock;
            });

    try {
      spyOps.commit(metadataV2, metadataV1);
      HiveLock spyLock = spy(lockRef.get());
      doThrow(new RuntimeException()).when(spyLock).unlock();
    } finally {
      lockRef.get().unlock();
    }

    ops.refresh();

    // the commit must succeed
    assertThat(ops.current().schema().columns()).hasSize(1);
  }

  /**
   * Pretends we throw an error while persisting, and not found with check state, commit state
   * should be treated as unknown, because in reality the persisting may still succeed, just not yet
   * by the time of checking.
   */
  @Test
  public void testThriftExceptionUnknownStateIfNotInHistoryFailureOnCommit()
      throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();
    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(metadataV2)).as("Current metadata should still exist").isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as(
            "New metadata files should still exist, new location not in history but"
                + " the commit may still succeed")
        .isEqualTo(3);
  }

  /** Pretends we throw an error while persisting that actually does commit serverside */
  @Test
  public void testThriftExceptionSuccessOnCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an
    // exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    assertThat(ops.current()).as("Current metadata should have changed").isNotEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as("Commit should have been successful and new metadata file should be made")
        .isEqualTo(3);
  }

  /**
   * Pretends we throw an exception while persisting and don't know what happened, can't check to
   * find out, but in reality the commit failed
   */
  @Test
  public void testThriftExceptionUnknownFailedCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as("Client could not determine outcome so new metadata file should also exist")
        .isEqualTo(3);
  }

  /**
   * Pretends we throw an exception while persisting and don't know what happened, can't check to
   * find out, but in reality the commit succeeded
   */
  @Test
  public void testThriftExceptionsUnknownSuccessCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    commitAndThrowException(ops, spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();

    assertThat(ops.current()).as("Current metadata should have changed").isNotEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
  }

  /**
   * Pretends we threw an exception while persisting, the commit succeeded, the lock expired, and a
   * second committer placed a commit on top of ours before the first committer was able to check if
   * their commit succeeded or not
   *
   * <p>Timeline:
   *
   * <ul>
   *   <li>Client 1 commits which throws an exception but succeeded
   *   <li>Client 1's lock expires while waiting to do the recheck for commit success
   *   <li>Client 2 acquires a lock, commits successfully on top of client 1's commit and release
   *       lock
   *   <li>Client 1 check's to see if their commit was successful
   * </ul>
   *
   * <p>This tests to make sure a disconnected client 1 doesn't think their commit failed just
   * because it isn't the current one during the recheck phase.
   */
  @Test
  public void testThriftExceptionConcurrentCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    AtomicReference<HiveLock> lock = new AtomicReference<>();
    doAnswer(
            l -> {
              lock.set(ops.lockObject(metadataV1));
              return lock.get();
            })
        .when(spyOps)
        .lockObject(metadataV1);

    concurrentCommitAndThrowException(ops, spyOps, table, lock);

    /*
    This commit and our concurrent commit should succeed even though this commit throws an exception
    after the persist operation succeeds
     */
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    assertThat(ops.current()).as("Current metadata should have changed").isNotEqualTo(metadataV2);
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
    assertThat(ops.current().schema().columns())
        .as("The column addition from the concurrent commit should have been successful")
        .hasSize(2);
  }

  @Test
  public void testInvalidObjectException() {
    TableIdentifier badTi = TableIdentifier.of(DB_NAME, "`tbl`");
    assertThatThrownBy(() -> catalog.createTable(badTi, schema, PartitionSpec.unpartitioned()))
        .isInstanceOf(ValidationException.class)
        .hasMessage(String.format("Invalid Hive object for %s.%s", DB_NAME, "`tbl`"));
  }

  @Test
  public void testAlreadyExistsException() {
    assertThatThrownBy(
            () -> catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned()))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage(String.format("Table already exists: %s.%s", DB_NAME, TABLE_NAME));
  }

  /** Uses NoLock and pretends we throw an error because of a concurrent commit */
  @Test
  public void testNoLockThriftExceptionConcurrentCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    // Sets NoLock
    doReturn(new NoLock()).when(spyOps).lockObject(any());

    // Simulate a concurrent table modification error
    doThrow(
            new RuntimeException(
                "MetaException(message:The table has been modified. The parameter value for key 'metadata_location' is"))
        .when(spyOps)
        .persistTable(any(), anyBoolean(), any());

    // Should throw a CommitFailedException so the commit could be retried
    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("The table hivedb.tbl has been modified concurrently");

    ops.refresh();
    assertThat(ops.current()).as("Current metadata should not have changed").isEqualTo(metadataV2);
    assertThat(metadataFileExists(metadataV2)).as("Current metadata should still exist").isTrue();
    assertThat(metadataFileCount(ops.current()))
        .as("New metadata files should not exist")
        .isEqualTo(2);
  }

  @Test
  public void testLockExceptionUnknownSuccessCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              ops.persistTable(tbl, true, location);
              throw new LockException("Datacenter on fire");
            })
        .when(spyOps)
        .persistTable(any(), anyBoolean(), any());

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .hasMessageContaining("Failed to heartbeat for hive lock while")
        .isInstanceOf(CommitStateUnknownException.class);

    ops.refresh();

    assertThat(ops.current().location())
        .as("Current metadata should have changed to metadata V1")
        .isEqualTo(metadataV1.location());
    assertThat(metadataFileExists(ops.current()))
        .as("Current metadata file should still exist")
        .isTrue();
  }

  @Test
  public void testCommitExceptionWithoutMessage() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    assertThat(ops.current().schema().columns()).hasSize(2);

    HiveTableOperations spyOps = spy(ops);

    doThrow(new RuntimeException()).when(spyOps).persistTable(any(), anyBoolean(), any());

    assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("null");
  }

  private void commitAndThrowException(
      HiveTableOperations realOperations, HiveTableOperations spyOperations)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              realOperations.persistTable(tbl, true, location);
              throw new TException("Datacenter on fire");
            })
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void concurrentCommitAndThrowException(
      HiveTableOperations realOperations,
      HiveTableOperations spyOperations,
      Table table,
      AtomicReference<HiveLock> lock)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(
            i -> {
              org.apache.hadoop.hive.metastore.api.Table tbl =
                  i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
              String location = i.getArgument(2, String.class);
              realOperations.persistTable(tbl, true, location);
              // Simulate lock expiration or removal
              lock.get().unlock();
              table.refresh();
              table.updateSchema().addColumn("newCol", Types.IntegerType.get()).commit();
              throw new TException("Datacenter on fire");
            })
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void failCommitAndThrowException(HiveTableOperations spyOperations)
      throws TException, InterruptedException {
    doThrow(new TException("Datacenter on fire"))
        .when(spyOperations)
        .persistTable(any(), anyBoolean(), any());
  }

  private void breakFallbackCatalogCommitCheck(HiveTableOperations spyOperations) {
    when(spyOperations.refresh())
        .thenThrow(new RuntimeException("Still on fire")); // Failure on commit check
  }

  private boolean metadataFileExists(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).exists();
  }

  private int metadataFileCount(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", ""))
        .getParentFile()
        .listFiles(file -> file.getName().endsWith("metadata.json"))
        .length;
  }
}
