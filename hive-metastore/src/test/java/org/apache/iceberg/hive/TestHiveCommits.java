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

import java.io.File;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHiveCommits extends HiveTableBaseTest {

  @Test
  public void testSuppressUnlockExceptions() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    ArgumentCaptor<Long> lockId = ArgumentCaptor.forClass(Long.class);
    doThrow(new RuntimeException()).when(spyOps).doUnlock(lockId.capture());

    try {
      spyOps.commit(metadataV2, metadataV1);
    } finally {
      ops.doUnlock(lockId.getValue());
    }

    ops.refresh();

    // the commit must succeed
    Assert.assertEquals(1, ops.current().schema().columns().size());
  }

  /**
   * Pretends we throw an error while persisting that actually fails to commit serverside
   */
  @Test
  public void testThriftExceptionFailureOnCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);

    AssertHelpers.assertThrows("We should rethrow generic runtime errors if the " +
        "commit actually doesn't succeed", RuntimeException.class, "Metastore operation failed",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();
    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

  /**
   * Pretends we throw an error while persisting that actually does commit serverside
   */
  @Test
  public void testThriftExceptionSuccessOnCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    // Simulate a communication error after a successful commit
    commitAndThrowException(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Commit should have been successful and new metadata file should be made",
        3, metadataFileCount(ops.current()));
  }

  /**
   * Pretends we throw an exception while persisting and don't know what happened, can't check to find out,
   * but in reality the commit failed
   */
  @Test
  public void testThriftExceptionUnknownFailedCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    failCommitAndThrowException(spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    AssertHelpers.assertThrows("Should throw CommitStateUnknownException since the catalog check was blocked",
        CommitStateUnknownException.class, "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();

    Assert.assertEquals("Current metadata should not have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Client could not determine outcome so new metadata file should also exist",
        3, metadataFileCount(ops.current()));
  }

  /**
   * Pretends we throw an exception while persisting and don't know what happened, can't check to find out,
   * but in reality the commit succeeded
   */
  @Test
  public void testThriftExceptionsUnknownSuccessCommit() throws TException, InterruptedException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    commitAndThrowException(ops, spyOps);
    breakFallbackCatalogCommitCheck(spyOps);

    AssertHelpers.assertThrows("Should throw CommitStateUnknownException since the catalog check was blocked",
        CommitStateUnknownException.class, "Datacenter on fire",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();

    Assert.assertFalse("Current metadata should have changed", ops.current().equals(metadataV2));
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
  }

  /**
   * Pretends we threw an exception while persisting, the commit succeeded, the lock expired,
   * and a second committer placed a commit on top of ours before the first committer was able to check
   * if their commit succeeded or not
   *
   * Timeline:
   *   Client 1 commits which throws an exception but suceeded
   *   Client 1's lock expires while waiting to do the recheck for commit success
   *   Client 2 acquires a lock, commits successfully on top of client 1's commit and release lock
   *   Client 1 check's to see if their commit was successful
   *
   * This tests to make sure a disconnected client 1 doesn't think their commit failed just because it isn't the
   * current one during the recheck phase.
   */
  @Test
  public void testThriftExceptionConcurrentCommit() throws TException, InterruptedException, UnknownHostException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);
    HiveTableOperations ops = (HiveTableOperations) ((HasTableOperations) table).operations();

    TableMetadata metadataV1 = ops.current();

    table.updateSchema()
        .addColumn("n", Types.IntegerType.get())
        .commit();

    ops.refresh();

    TableMetadata metadataV2 = ops.current();

    Assert.assertEquals(2, ops.current().schema().columns().size());

    HiveTableOperations spyOps = spy(ops);

    AtomicLong lockId = new AtomicLong();
    doAnswer(i -> {
      lockId.set(ops.acquireLock());
      return lockId.get();
    }).when(spyOps).acquireLock();

    concurrentCommitAndThrowException(ops, spyOps, table, lockId);

    /*
    This commit and our concurrent commit should succeed even though this commit throws an exception
    after the persist operation succeeds
     */
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertNotEquals("Current metadata should have changed", metadataV2, ops.current());
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("The column addition from the concurrent commit should have been successful",
        2, ops.current().schema().columns().size());
  }

  private void commitAndThrowException(HiveTableOperations realOperations, HiveTableOperations spyOperations)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(i -> {
      org.apache.hadoop.hive.metastore.api.Table tbl =
          i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
      realOperations.persistTable(tbl, true);
      throw new TException("Datacenter on fire");
    }).when(spyOperations).persistTable(any(), anyBoolean());
  }

  private void concurrentCommitAndThrowException(HiveTableOperations realOperations, HiveTableOperations spyOperations,
                                                 Table table, AtomicLong lockId)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(i -> {
      org.apache.hadoop.hive.metastore.api.Table tbl =
          i.getArgument(0, org.apache.hadoop.hive.metastore.api.Table.class);
      realOperations.persistTable(tbl, true);
      // Simulate lock expiration or removal
      realOperations.doUnlock(lockId.get());
      table.refresh();
      table.updateSchema().addColumn("newCol", Types.IntegerType.get()).commit();
      throw new TException("Datacenter on fire");
    }).when(spyOperations).persistTable(any(), anyBoolean());
  }

  private void failCommitAndThrowException(HiveTableOperations spyOperations) throws TException, InterruptedException {
    doThrow(new TException("Datacenter on fire"))
        .when(spyOperations)
        .persistTable(any(), anyBoolean());
  }

  private void breakFallbackCatalogCommitCheck(HiveTableOperations spyOperations) {
    when(spyOperations.refresh())
        .thenThrow(new RuntimeException("Still on fire")); // Failure on commit check
  }

  private boolean metadataFileExists(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).exists();
  }

  private int metadataFileCount(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).getParentFile()
        .listFiles(file -> file.getName().endsWith("metadata.json")).length;
  }
}
