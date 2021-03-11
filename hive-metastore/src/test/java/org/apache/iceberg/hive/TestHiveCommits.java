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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
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

    throwExceptionDontCommit(spyOps);

    AssertHelpers.assertThrows("We should rethrow generic runtime errors as CFE if the " +
        "commit actually doesn't succeed", CommitFailedException.class,
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();
    Assert.assertTrue("Current metadata should not have changed", ops.current().equals(metadataV2));
    Assert.assertTrue("Current metadata should still exist", metadataFileExists(metadataV2));
    Assert.assertEquals("No new metadata files should exist", 2, metadataFileCount(ops.current()));
  }

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
    throwExceptionAndCommit(ops, spyOps);

    // Shouldn't throw because the commit actually succeeds even though persistTable throws an exception
    spyOps.commit(metadataV2, metadataV1);

    ops.refresh();
    Assert.assertFalse("Current metadata should have changed", ops.current().equals(metadataV2));
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Commit should have been successful and new metadata file should be made",
        3, metadataFileCount(ops.current()));
  }

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

    throwExceptionDontCommit(spyOps);
    breakCommitCheck(spyOps);

    AssertHelpers.assertThrows("Should throw original thrift exception since we couldn't determine final state",
        RuntimeException.class, "Metastore operation failed for",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();

    Assert.assertTrue("Current metadata should not have changed", ops.current().equals(metadataV2));
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
    Assert.assertEquals("Client could not determine outcome so new metadata file should also exist",
        3, metadataFileCount(ops.current()));
  }

  @Test
  public void testRuntimeExceptionsUnknownSuccessCommit() throws TException, InterruptedException {
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

    throwExceptionAndCommit(ops, spyOps);
    breakCommitCheck(spyOps);

    AssertHelpers.assertThrows("Should throw original thrift exception since we couldn't determine final state",
        RuntimeException.class, "Metastore operation failed for",
        () -> spyOps.commit(metadataV2, metadataV1));

    ops.refresh();

    Assert.assertFalse("Current metadata should have changed", ops.current().equals(metadataV2));
    Assert.assertTrue("Current metadata file should still exist", metadataFileExists(ops.current()));
  }

  private void throwExceptionAndCommit(HiveTableOperations realOperations, HiveTableOperations spyOperations)
      throws TException, InterruptedException {
    // Simulate a communication error after a successful commit
    doAnswer(i -> {
      org.apache.hadoop.hive.metastore.api.Table tbl =
          i.getArgumentAt(0, org.apache.hadoop.hive.metastore.api.Table.class);
      realOperations.persistTable(tbl, true);
      throw new TException("Datacenter on fire");
    }).when(spyOperations).persistTable(any(), anyBoolean());
  }

  private void throwExceptionDontCommit(HiveTableOperations spyOperations) throws TException, InterruptedException {
    doThrow(new TException("Datacenter on fire"))
        .when(spyOperations)
        .persistTable(any(), anyBoolean());

  }

  private void breakCommitCheck(HiveTableOperations spyOperations) throws TException, InterruptedException {
    when(spyOperations.loadHmsTable())
        .thenCallRealMethod() // Success when loading
        .thenThrow(new TException("Still on fire")); // Failure on commit check
  }


  private boolean metadataFileExists(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).exists();
  }

  private int metadataFileCount(TableMetadata metadata) {
    return new File(metadata.metadataFileLocation().replace("file:", "")).getParentFile()
        .listFiles(file -> file.getName().endsWith("metadata.json")).length;
  }
}
