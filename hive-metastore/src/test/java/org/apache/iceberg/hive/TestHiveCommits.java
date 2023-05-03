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
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestHiveCommits extends HiveTableBaseTest {

  @Test
  public void testSuppressUnlockExceptions() {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

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
    Assert.assertEquals(1, ops.current().schema().columns().size());
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);

    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals(
        "New metadata files should still exist, new location not in history but"
            + " the commit may still succeed",
        3,
        metadataFileCount(ops.current()));
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an
    // exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "Commit should have been successful and new metadata file should be made",
        3,
        metadataFileCount(ops.current()));
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();

    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "Client could not determine outcome so new metadata file should also exist",
        3,
        metadataFileCount(ops.current()));
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    commitAndThrowException(ops, spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitStateUnknownException.class)
        .hasMessageStartingWith("Datacenter on fire");

    ops.refresh();

    Assert.assertFalse("Current metadata should have changed", ops.current().equals(metadataV2));
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

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
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue(
        "Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals(
        "The column addition from the concurrent commit should have been successful",
        2,
        ops.current().schema().columns().size());
  }

  @Test
  public void testInvalidObjectException() {
    TableIdentifier badTi = TableIdentifier.of(DB_NAME, "`tbl`");
    Assert.assertThrows(
        String.format("Invalid table name for %s.%s", DB_NAME, "`tbl`"),
        ValidationException.class,
        () -> catalog.createTable(badTi, schema, PartitionSpec.unpartitioned()));
  }

  @Test
  public void testAlreadyExistsException() {
    Assert.assertThrows(
        String.format("Table already exists: %s.%s", DB_NAME, TABLE_NAME),
        AlreadyExistsException.class,
        () -> catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned()));
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

    Assert.assertEquals(2, ops.current().schema().columns().size());

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
    Assertions.assertThatThrownBy(() -> spyOps.commit(metadataV2, metadataV1))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("The table hivedb.tbl has been modified concurrently");

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("New metadata files should not exist", 2, metadataFileCount(ops.current()));
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
