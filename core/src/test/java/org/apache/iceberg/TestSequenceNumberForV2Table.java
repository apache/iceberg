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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class TestSequenceNumberForV2Table extends TableTestBase {

  public TestSequenceNumberForV2Table() {
    super(2);
  }

  @Test
  public void testSequenceNumberForFastAppend() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();
    Assert.assertEquals(1, table.currentSnapshot().sequenceNumber());
    ManifestFile manifestFile = table.currentSnapshot().manifests().get(0);
    Assert.assertEquals(1, manifestFile.sequenceNumber());

    table.newFastAppend().appendFile(FILE_B).commit();
    Assert.assertEquals(2, table.currentSnapshot().sequenceNumber());
    manifestFile = table.currentSnapshot().manifests().stream()
        .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
        .collect(Collectors.toList()).get(0);
    Assert.assertEquals(2, manifestFile.sequenceNumber());

    manifestFile = writeManifest(FILE_C, FILE_D);
    table.newFastAppend().appendManifest(manifestFile).commit();
    Assert.assertEquals(3, table.currentSnapshot().sequenceNumber());

    manifestFile = table.currentSnapshot().manifests().stream()
        .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
        .collect(Collectors.toList()).get(0);
    Assert.assertEquals(3, manifestFile.sequenceNumber());

    for (ManifestEntry entry : ManifestFiles.read(manifestFile,
        table.io(), table.ops().current().specsById()).entries()) {
      if (entry.file().path().equals(FILE_C.path()) || entry.file().path().equals(FILE_D.path())) {
        Assert.assertEquals(3, entry.sequenceNumber().longValue());
      }
    }
  }

  @Test
  public void testSequenceNumberForMergeAppend() throws IOException {
    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();
    table.newAppend().appendFile(FILE_A).commit();
    Assert.assertEquals(1, table.currentSnapshot().sequenceNumber());

    table.newAppend().appendFile(FILE_B).commit();
    Assert.assertEquals(2, table.currentSnapshot().sequenceNumber());

    ManifestFile manifestFile = writeManifest(FILE_C, FILE_D);
    table.newAppend().appendManifest(manifestFile).commit();
    Assert.assertEquals(3, table.currentSnapshot().sequenceNumber());

    manifestFile = table.currentSnapshot().manifests().get(0);

    Assert.assertEquals("the sequence number of manifest should be 3", 3, manifestFile.sequenceNumber());

    for (ManifestEntry entry : ManifestFiles.read(manifestFile,
        table.io(), table.ops().current().specsById()).entries()) {
      if (entry.file().path().equals(FILE_A.path())) {
        Assert.assertEquals("the sequence number of data file should be 1", 1, entry.sequenceNumber().longValue());
      }

      if (entry.file().path().equals(FILE_B.path())) {
        Assert.assertEquals("the sequence number of data file should be 2", 2, entry.sequenceNumber().longValue());
      }

      if (entry.file().path().equals(FILE_C.path()) || entry.file().path().equals(FILE_D.path())) {
        Assert.assertEquals("the sequence number of data file should be 3", 3, entry.sequenceNumber().longValue());
      }
    }
  }

  @Test
  public void testSequenceNumberForRewrite() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    Assert.assertEquals(2, table.currentSnapshot().sequenceNumber());

    table.rewriteManifests().clusterBy(file -> "").commit();
    Assert.assertEquals("the sequence number of snapshot should be 3",
        3, table.currentSnapshot().sequenceNumber());

    ManifestFile newManifest = table.currentSnapshot().manifests().get(0);
    Assert.assertEquals("the sequence number of manifest should be 3",
        3, newManifest.sequenceNumber());

    for (ManifestEntry entry : ManifestFiles.read(newManifest,
        table.io(), table.ops().current().specsById()).entries()) {
      if (entry.file().path().equals(FILE_A.path())) {
        Assert.assertEquals("the sequence number of data file should be 1", 1, entry.sequenceNumber().longValue());
      }

      if (entry.file().path().equals(FILE_B.path())) {
        Assert.assertEquals("the sequence number of data file should be 1", 2, entry.sequenceNumber().longValue());
      }
    }
  }

  @Test
  public void testCommitConflict() {
    Transaction txn = table.newTransaction();

    txn.newFastAppend().appendFile(FILE_A).apply();
    table.newFastAppend().appendFile(FILE_B).commit();

    AssertHelpers.assertThrows("Should failed due to conflict",
        IllegalStateException.class, "last operation has not committed", txn::commitTransaction);

    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_C);
    appendFiles.apply();
    table.newFastAppend().appendFile(FILE_D).commit();
    appendFiles.commit();

    ManifestFile manifestFile = table.currentSnapshot().manifests().stream()
        .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
        .collect(Collectors.toList()).get(0);

    for (ManifestEntry entry : ManifestFiles.read(manifestFile, table.io(),
        table.ops().current().specsById()).entries()) {
      if (entry.file().path().equals(FILE_C.path())) {
        Assert.assertEquals(table.currentSnapshot().sequenceNumber(), entry.sequenceNumber().longValue());
      }
    }
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

    Assert.assertEquals(4, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());
  }

  @Test
  public void testRollBack() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();

    Assert.assertEquals(2, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    table.manageSnapshots().rollbackTo(snapshotId).commit();

    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());
  }

  @Test
  public void testMultipleTxnOperations() {
    Snapshot snapshot;
    Transaction txn = table.newTransaction();
    txn.newOverwrite().addFile(FILE_A).commit();
    txn.commitTransaction();
    Assert.assertEquals(1, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    txn = table.newTransaction();
    Set<DataFile> toAddFiles = new HashSet<>();
    Set<DataFile> toDeleteFiles = new HashSet<>();
    toAddFiles.add(FILE_B);
    toDeleteFiles.add(FILE_A);
    txn.newRewrite().rewriteFiles(toDeleteFiles, toAddFiles).commit();
    txn.commitTransaction();
    Assert.assertEquals(2, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    txn = table.newTransaction();
    txn.newReplacePartitions().addFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(3, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    txn = table.newTransaction();
    txn.newDelete().deleteFile(FILE_C).commit();
    txn.commitTransaction();

    Assert.assertEquals(4, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(5, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    snapshot = table.currentSnapshot();
    txn = table.newTransaction();
    txn.newOverwrite().addFile(FILE_D).deleteFile(FILE_C).commit();
    txn.commitTransaction();
    Assert.assertEquals(6, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());

    txn = table.newTransaction();
    txn.expireSnapshots().expireOlderThan(snapshot.timestampMillis()).commit();
    txn.commitTransaction();
    Assert.assertEquals(6, TestTables.load(tableDir, "test").currentSnapshot().sequenceNumber());
  }

  @Test
  public void testSequenceNumberForCherryPicking() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();

    Assert.assertEquals("the snapshot sequence number should be 1", 1,
        table.currentSnapshot().sequenceNumber());

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = readMetadata().snapshots().get(1);

    Assert.assertEquals("the snapshot sequence number should be 2", 2,
        wapSnapshot.sequenceNumber());

    // table has new commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    Assert.assertEquals("the snapshot sequence number should be 3",
        3, table.currentSnapshot().sequenceNumber());

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();

    Assert.assertEquals("the snapshot sequence number should be 4",
        4, table.currentSnapshot().sequenceNumber());

  }
}
