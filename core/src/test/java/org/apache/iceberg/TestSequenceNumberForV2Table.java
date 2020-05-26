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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.junit.Assert;
import org.junit.Test;

public class TestSequenceNumberForV2Table extends TableTestBase {

  public TestSequenceNumberForV2Table() {
    super(2);
  }

  @Test
  public void testMergeAppend() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    ManifestFile manifestFile = table.currentSnapshot().manifests().get(0);
    validateManifestEntries(manifestFile, 1, files(FILE_A), seqs(1));
    table.newAppend().appendFile(FILE_B).commit();
    manifestFile = table.currentSnapshot().manifests().get(0);
    validateManifestEntries(manifestFile, 2, files(FILE_B), seqs(2));

    table.newAppend()
        .appendManifest(writeManifest("input-m0.avro",
            manifestEntry(ManifestEntry.Status.ADDED, null, FILE_C)))
        .commit();

    validateDataFiles(files(FILE_A, FILE_B, FILE_C), seqs(1, 2, 3));

    table.updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    table.newAppend()
        .appendManifest(writeManifest("input-m1.avro",
            manifestEntry(ManifestEntry.Status.ADDED, null, FILE_D)))
        .commit();

    manifestFile = table.currentSnapshot().manifests().stream()
        .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
        .collect(Collectors.toList()).get(0);
    validateManifestEntries(manifestFile, 4, files(FILE_A, FILE_B, FILE_C, FILE_D), seqs(1, 2, 3, 4));
    validateDataFiles(files(FILE_A, FILE_B, FILE_C, FILE_D), seqs(1, 2, 3, 4));
  }

  @Test
  public void testRewrite() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    Assert.assertEquals(2, table.currentSnapshot().sequenceNumber());

    table.rewriteManifests().clusterBy(file -> "").commit();
    Assert.assertEquals("Snapshot sequence number should be 3",
        3, table.currentSnapshot().sequenceNumber());

    ManifestFile newManifest = table.currentSnapshot().manifests().stream()
        .filter(manifest -> manifest.snapshotId() == table.currentSnapshot().snapshotId())
        .collect(Collectors.toList()).get(0);

    validateManifestEntries(newManifest, 3, files(FILE_A, FILE_B), seqs(1, 2));
  }

  @Test
  public void testCommitConflict() {
    AppendFiles appendA = table.newFastAppend();
    appendA.appendFile(FILE_A).apply();

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        .commit();

    table.ops().failCommits(1);

    AssertHelpers.assertThrows("Should reject commit",
        CommitFailedException.class, "Injected failure",
        () -> table.newFastAppend().appendFile(FILE_B).commit());

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "5")
        .commit();

    appendA.commit();

    Assert.assertEquals(1, table.currentSnapshot().sequenceNumber());

    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_C);
    appendFiles.apply();
    table.newFastAppend().appendFile(FILE_D).commit();
    appendFiles.commit();

    validateDataFiles(files(FILE_A, FILE_D, FILE_C), seqs(1, 2, 3));
  }

  @Test
  public void testRollBack() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).commit();
    Assert.assertEquals("Snapshot sequence number should match expected",
        2, table.currentSnapshot().sequenceNumber());

    table.manageSnapshots().rollbackTo(snapshotId).commit();
    Assert.assertEquals("Snapshot sequence number should match expected",
        1, table.currentSnapshot().sequenceNumber());

    Assert.assertEquals("Table last sequence number should be 2",
        2, table.operations().current().lastSequenceNumber());

    table.newFastAppend().appendFile(FILE_C).commit();
    Assert.assertEquals("Snapshot sequence number should match expected",
        3, table.currentSnapshot().sequenceNumber());
  }

  @Test
  public void testSingleTransaction() {
    Transaction txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_A).commit();
    txn.commitTransaction();
    Assert.assertEquals("Snapshot sequence number should match expected",
        1, table.currentSnapshot().sequenceNumber());
  }

  @Test
  public void testConcurrentTransaction() {
    Transaction txn1 = table.newTransaction();
    Transaction txn2 = table.newTransaction();
    Transaction txn3 = table.newTransaction();
    Transaction txn4 = table.newTransaction();

    txn1.newFastAppend().appendFile(FILE_A).commit();
    txn3.newOverwrite().addFile(FILE_C).commit();
    txn4.newDelete().deleteFile(FILE_A).commit();
    txn2.newAppend().appendFile(FILE_B).commit();

    txn1.commitTransaction();
    txn2.commitTransaction();
    txn3.commitTransaction();
    txn4.commitTransaction();

    Assert.assertEquals("Snapshot sequence number should match expected",
        4, table.currentSnapshot().sequenceNumber());
    validateDataFiles(files(FILE_A, FILE_B, FILE_C), seqs(4, 2, 3));
  }

  @Test
  public void testMultipleOperationsTransaction() {
    Transaction txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_A).commit();
    Set<DataFile> toAddFiles = new HashSet<>();
    Set<DataFile> toDeleteFiles = new HashSet<>();
    toAddFiles.add(FILE_B);
    toDeleteFiles.add(FILE_A);
    txn.newRewrite().rewriteFiles(toDeleteFiles, toAddFiles).commit();
    txn.commitTransaction();

    Assert.assertEquals("Snapshot sequence number should match expected",
        2, table.currentSnapshot().sequenceNumber());
    validateDataFiles(files(FILE_A, FILE_B), seqs(2, 2));
  }

  @Test
  public void testExpirationInTransaction() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();

    Transaction txn = table.newTransaction();
    txn.expireSnapshots().expireSnapshotId(snapshotId).commit();
    txn.commitTransaction();

    Assert.assertEquals("Snapshot sequence number should match expected",
        2, table.currentSnapshot().sequenceNumber());
    validateDataFiles(files(FILE_B), seqs(2));
  }

  @Test
  public void testTransactionFailure() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateProperties()
        .set(TableProperties.COMMIT_NUM_RETRIES, "0")
        .commit();

    table.ops().failCommits(1);

    Transaction txn = table.newTransaction();
    txn.newAppend().appendFile(FILE_C).commit();

    AssertHelpers.assertThrows("Transaction commit should fail",
        CommitFailedException.class, "Injected failure", txn::commitTransaction);

    Assert.assertEquals("Snapshot sequence number should match expected",
        1, table.currentSnapshot().sequenceNumber());
  }

  @Test
  public void testCherryPicking() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    Assert.assertEquals("Snapshot sequence number should be 1", 1,
        table.currentSnapshot().sequenceNumber());

    // pick the snapshot that's staged but not committed
    Snapshot stagedSnapshot = readMetadata().snapshots().get(1);

    Assert.assertEquals("Snapshot sequence number should be 2", 2,
        stagedSnapshot.sequenceNumber());

    // table has new commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    Assert.assertEquals("Snapshot sequence number should be 3",
        3, table.currentSnapshot().sequenceNumber());

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(stagedSnapshot.snapshotId()).commit();

    Assert.assertEquals("Snapshot sequence number should be 4",
        4, table.currentSnapshot().sequenceNumber());


    validateDataFiles(files(FILE_A, FILE_B, FILE_C), seqs(1, 4, 3));
  }

  @Test
  public void testCherryPickFastForward() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    Assert.assertEquals("Snapshot sequence number should be 1", 1,
        table.currentSnapshot().sequenceNumber());

    // pick the snapshot that's staged but not committed
    Snapshot stagedSnapshot = readMetadata().snapshots().get(1);

    Assert.assertEquals("Snapshot sequence number should be 2", 2,
        stagedSnapshot.sequenceNumber());

    // cherry-pick snapshot, this will fast forward
    table.manageSnapshots().cherrypick(stagedSnapshot.snapshotId()).commit();
    Assert.assertEquals("Snapshot sequence number should be 2",
        2, table.currentSnapshot().sequenceNumber());

    validateDataFiles(files(FILE_A, FILE_B), seqs(1, 2));
  }

  void validateDataFiles(Iterator<DataFile> files, Iterator<Long> expectedSeqs) {
    Map<String, Long> fileToSeqMap = new HashMap<>();
    while (files.hasNext() && expectedSeqs.hasNext()) {
      DataFile file = files.next();
      fileToSeqMap.put(file.path().toString(), expectedSeqs.next());
    }

    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (ManifestEntry entry : ManifestFiles.read(manifest, FILE_IO).entries()) {
        String path = entry.file().path().toString();
        if (fileToSeqMap.containsKey(path)) {
          Assert.assertEquals("Sequence number should match expected",
              fileToSeqMap.get(path), entry.sequenceNumber());
        }
      }
    }
  }

  void validateManifestEntries(ManifestFile manifestFile, long expectedSequenceNumber,
      Iterator<DataFile> files, Iterator<Long> expectedSeqs) {
    Map<String, Long> fileToSeqMap = new HashMap<>();
    while (files.hasNext() && expectedSeqs.hasNext()) {
      DataFile file = files.next();
      fileToSeqMap.put(file.path().toString(), expectedSeqs.next());
    }
    Assert.assertEquals("Manifest sequence number should match exptected",
        expectedSequenceNumber, manifestFile.sequenceNumber());

    for (ManifestEntry entry : ManifestFiles.read(manifestFile, FILE_IO).entries()) {
      String path = entry.file().path().toString();
      if (fileToSeqMap.containsKey(path)) {
        Assert.assertEquals("Sequence number should match expected",
            fileToSeqMap.get(path), entry.sequenceNumber());
      }
    }
  }

}
