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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.Assert;
import org.junit.Test;

public class TestSequenceNumber extends TableTestBase {

  @Test
  public void testReadWriteSequenceNumber() {
    table.newFastAppend().appendFile(FILE_A).commit();
    Assert.assertEquals(1, table.currentSnapshot().sequenceNumber().longValue());
    table.newFastAppend().appendFile(FILE_B).commit();
    Assert.assertEquals(2, table.currentSnapshot().sequenceNumber().longValue());
  }

  @Test
  public void testCommitConflict() {
    Transaction txn = table.newTransaction();

    txn.newFastAppend().appendFile(FILE_A).apply();
    table.newFastAppend().appendFile(FILE_B).commit();

    AssertHelpers.assertThrows("Should failed due to conflict",
        IllegalStateException.class, "last operation has not committed", txn::commitTransaction);

    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());
  }

  @Test
  public void testConcurrentCommit() throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(4);
    List<Callable<Void>> tasks = new ArrayList<>();

    Callable<Void> write1 = () -> {
      Transaction txn = table.newTransaction();
      txn.newFastAppend().appendFile(FILE_A).commit();
      txn.commitTransaction();
      return null;
    };

    Callable<Void> write2 = () -> {
      Transaction txn = table.newTransaction();
      txn.newAppend().appendFile(FILE_B).commit();
      txn.commitTransaction();
      return null;
    };

    Callable<Void> write3 = () -> {
      Transaction txn = table.newTransaction();
      txn.newDelete().deleteFile(FILE_A).commit();
      txn.commitTransaction();
      return null;
    };

    Callable<Void> write4 = () -> {
      Transaction txn = table.newTransaction();
      txn.newOverwrite().addFile(FILE_D).commit();
      txn.commitTransaction();
      return null;
    };

    tasks.add(write1);
    tasks.add(write2);
    tasks.add(write3);
    tasks.add(write4);
    threadPool.invokeAll(tasks);
    threadPool.shutdown();

    Assert.assertEquals(4, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());
  }

  @Test
  public void testRollBack() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();

    Assert.assertEquals(2, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    table.rollback().toSnapshotId(snapshotId).commit();

    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());
  }

  @Test
  public void testMultipleTxnOperations() {
    Snapshot snapshot;
    Transaction txn = table.newTransaction();
    txn.newOverwrite().addFile(FILE_A).commit();
    txn.commitTransaction();
    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    txn = table.newTransaction();
    Set<DataFile> toAddFiles = new HashSet<>();
    Set<DataFile> toDeleteFiles = new HashSet<>();
    toAddFiles.add(FILE_B);
    toDeleteFiles.add(FILE_A);
    txn.newRewrite().rewriteFiles(toDeleteFiles, toAddFiles).commit();
    txn.commitTransaction();
    Assert.assertEquals(2, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    txn = table.newTransaction();
    txn.newReplacePartitions().addFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(3, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    txn = table.newTransaction();
    txn.newDelete().deleteFile(FILE_C).commit();
    txn.commitTransaction();

    Assert.assertEquals(4, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(5, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    snapshot = table.currentSnapshot();

    txn = table.newTransaction();
    txn.newOverwrite().addFile(FILE_D).deleteFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(6, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());

    txn = table.newTransaction();
    txn.expireSnapshots().expireOlderThan(snapshot.timestampMillis()).commit();
    txn.commitTransaction();
    Assert.assertEquals(6, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber()
        .longValue());
  }
}
