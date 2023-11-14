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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRemoveSnapshots extends TableTestBase {
  private final boolean incrementalCleanup;

  @Parameterized.Parameters(name = "formatVersion = {0}, incrementalCleanup = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {1, true},
      new Object[] {2, true},
      new Object[] {1, false},
      new Object[] {2, false}
    };
  }

  public TestRemoveSnapshots(int formatVersion, boolean incrementalCleanup) {
    super(formatVersion);
    this.incrementalCleanup = incrementalCleanup;
  }

  private long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  @Test
  public void testExpireOlderThan() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newAppend().appendFile(FILE_B).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Expire should not change current snapshot",
        snapshotId,
        table.currentSnapshot().snapshotId());
    Assert.assertNull(
        "Expire should remove the oldest snapshot", table.snapshot(firstSnapshot.snapshotId()));
    Assert.assertEquals(
        "Should remove only the expired manifest list location",
        Sets.newHashSet(firstSnapshot.manifestListLocation()),
        deletedFiles);
  }

  @Test
  public void testExpireOlderThanWithDelete() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should create one manifest", 1, firstSnapshot.allManifests(table.io()).size());

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should create replace manifest with a rewritten manifest",
        1,
        secondSnapshot.allManifests(table.io()).size());

    table.newAppend().appendFile(FILE_B).commit();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Expire should not change current snapshot",
        snapshotId,
        table.currentSnapshot().snapshotId());
    Assert.assertNull(
        "Expire should remove the oldest snapshot", table.snapshot(firstSnapshot.snapshotId()));
    Assert.assertNull(
        "Expire should remove the second oldest snapshot",
        table.snapshot(secondSnapshot.snapshotId()));

    Assert.assertEquals(
        "Should remove expired manifest lists and deleted data file",
        Sets.newHashSet(
            firstSnapshot.manifestListLocation(), // snapshot expired
            firstSnapshot
                .allManifests(table.io())
                .get(0)
                .path(), // manifest was rewritten for delete
            secondSnapshot.manifestListLocation(), // snapshot expired
            secondSnapshot
                .allManifests(table.io())
                .get(0)
                .path(), // manifest contained only deletes, was dropped
            FILE_A.path()), // deleted
        deletedFiles);
  }

  @Test
  public void testExpireOlderThanWithDeleteInMergedManifests() {
    // merge every commit
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should create one manifest", 1, firstSnapshot.allManifests(table.io()).size());

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table
        .newDelete()
        .deleteFile(FILE_A) // FILE_B is still in the dataset
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should replace manifest with a rewritten manifest",
        1,
        secondSnapshot.allManifests(table.io()).size());

    table
        .newFastAppend() // do not merge to keep the last snapshot's manifest valid
        .appendFile(FILE_C)
        .commit();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Expire should not change current snapshot",
        snapshotId,
        table.currentSnapshot().snapshotId());
    Assert.assertNull(
        "Expire should remove the oldest snapshot", table.snapshot(firstSnapshot.snapshotId()));
    Assert.assertNull(
        "Expire should remove the second oldest snapshot",
        table.snapshot(secondSnapshot.snapshotId()));

    Assert.assertEquals(
        "Should remove expired manifest lists and deleted data file",
        Sets.newHashSet(
            firstSnapshot.manifestListLocation(), // snapshot expired
            firstSnapshot
                .allManifests(table.io())
                .get(0)
                .path(), // manifest was rewritten for delete
            secondSnapshot.manifestListLocation(), // snapshot expired
            FILE_A.path()), // deleted
        deletedFiles);
  }

  @Test
  public void testExpireOlderThanWithRollback() {
    // merge every commit
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should create one manifest", 1, firstSnapshot.allManifests(table.io()).size());

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newDelete().deleteFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Set<ManifestFile> secondSnapshotManifests =
        Sets.newHashSet(secondSnapshot.allManifests(table.io()));
    secondSnapshotManifests.removeAll(firstSnapshot.allManifests(table.io()));
    Assert.assertEquals(
        "Should add one new manifest for append", 1, secondSnapshotManifests.size());

    table.manageSnapshots().rollbackTo(firstSnapshot.snapshotId()).commit();

    long tAfterCommits = waitUntilAfter(secondSnapshot.timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Expire should not change current snapshot",
        snapshotId,
        table.currentSnapshot().snapshotId());
    Assert.assertNotNull(
        "Expire should keep the oldest snapshot, current",
        table.snapshot(firstSnapshot.snapshotId()));
    Assert.assertNull(
        "Expire should remove the orphaned snapshot", table.snapshot(secondSnapshot.snapshotId()));

    Assert.assertEquals(
        "Should remove expired manifest lists and reverted appended data file",
        Sets.newHashSet(
            secondSnapshot.manifestListLocation(), // snapshot expired
            Iterables.getOnlyElement(secondSnapshotManifests)
                .path()), // manifest is no longer referenced
        deletedFiles);
  }

  @Test
  public void testExpireOlderThanWithRollbackAndMergedManifests() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should create one manifest", 1, firstSnapshot.allManifests(table.io()).size());

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Set<ManifestFile> secondSnapshotManifests =
        Sets.newHashSet(secondSnapshot.allManifests(table.io()));
    secondSnapshotManifests.removeAll(firstSnapshot.allManifests(table.io()));
    Assert.assertEquals(
        "Should add one new manifest for append", 1, secondSnapshotManifests.size());

    table.manageSnapshots().rollbackTo(firstSnapshot.snapshotId()).commit();

    long tAfterCommits = waitUntilAfter(secondSnapshot.timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Expire should not change current snapshot",
        snapshotId,
        table.currentSnapshot().snapshotId());
    Assert.assertNotNull(
        "Expire should keep the oldest snapshot, current",
        table.snapshot(firstSnapshot.snapshotId()));
    Assert.assertNull(
        "Expire should remove the orphaned snapshot", table.snapshot(secondSnapshot.snapshotId()));

    Assert.assertEquals(
        "Should remove expired manifest lists and reverted appended data file",
        Sets.newHashSet(
            secondSnapshot.manifestListLocation(), // snapshot expired
            Iterables.getOnlyElement(secondSnapshotManifests)
                .path(), // manifest is no longer referenced
            FILE_B.path()), // added, but rolled back
        deletedFiles);
  }

  @Test
  public void testRetainLastWithExpireOlderThan() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots
    removeSnapshots(table).expireOlderThan(t3).retainLast(2).commit();

    Assert.assertEquals(
        "Should have two snapshots.", 2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(
        "First snapshot should not present.", null, table.snapshot(firstSnapshotId));
  }

  @Test
  public void testRetainLastWithExpireById() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 3 snapshots, but explicitly remove the first snapshot
    removeSnapshots(table).expireSnapshotId(firstSnapshotId).retainLast(3).commit();

    Assert.assertEquals(
        "Should have two snapshots.", 2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(
        "First snapshot should not present.", null, table.snapshot(firstSnapshotId));
  }

  @Test
  public void testRetainNAvailableSnapshotsWithTransaction() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    Assert.assertEquals(
        "Should be 3 manifest lists", 3, listManifestLists(table.location()).size());

    // Retain last 2 snapshots, which means 1 is deleted.
    Transaction tx = table.newTransaction();
    removeSnapshots(tx.table()).expireOlderThan(t3).retainLast(2).commit();
    tx.commitTransaction();

    Assert.assertEquals(
        "Should have two snapshots.", 2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(
        "First snapshot should not present.", null, table.snapshot(firstSnapshotId));
    Assert.assertEquals(
        "Should be 2 manifest lists", 2, listManifestLists(table.location()).size());
  }

  @Test
  public void testRetainLastWithTooFewSnapshots() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    // Retain last 3 snapshots
    removeSnapshots(table).expireOlderThan(t2).retainLast(3).commit();

    Assert.assertEquals(
        "Should have two snapshots", 2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals(
        "First snapshot should still present",
        firstSnapshotId,
        table.snapshot(firstSnapshotId).snapshotId());
  }

  @Test
  public void testRetainNLargerThanCurrentSnapshots() {
    // Append 3 files
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 4 snapshots
    Transaction tx = table.newTransaction();
    removeSnapshots(tx.table()).expireOlderThan(t3).retainLast(4).commit();
    tx.commitTransaction();

    Assert.assertEquals(
        "Should have three snapshots.", 3, Lists.newArrayList(table.snapshots()).size());
  }

  @Test
  public void testRetainLastKeepsExpiringSnapshot() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_D) // data_bucket=3
        .commit();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    removeSnapshots(table).expireOlderThan(secondSnapshot.timestampMillis()).retainLast(2).commit();

    Assert.assertEquals(
        "Should have three snapshots.", 3, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNotNull(
        "Second snapshot should present.", table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testExpireOlderThanMultipleCalls() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    Snapshot thirdSnapshot = table.currentSnapshot();
    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    removeSnapshots(table)
        .expireOlderThan(secondSnapshot.timestampMillis())
        .expireOlderThan(thirdSnapshot.timestampMillis())
        .commit();

    Assert.assertEquals(
        "Should have one snapshots.", 1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNull(
        "Second snapshot should not present.", table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testRetainLastMultipleCalls() {
    long t0 = System.currentTimeMillis();
    table
        .newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table
        .newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    removeSnapshots(table).expireOlderThan(t3).retainLast(2).retainLast(1).commit();

    Assert.assertEquals(
        "Should have one snapshots.", 1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNull(
        "Second snapshot should not present.", table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testRetainZeroSnapshots() {
    Assertions.assertThatThrownBy(() -> removeSnapshots(table).retainLast(0).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Number of snapshots to retain must be at least 1, cannot be: 0");
  }

  @Test
  public void testScanExpiredManifestInValidSnapshotAppend() {
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.newOverwrite().addFile(FILE_C).deleteFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_D).commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(t3).deleteWith(deletedFiles::add).commit();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
  }

  @Test
  public void testScanExpiredManifestInValidSnapshotFastAppend() {
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    table.newOverwrite().addFile(FILE_C).deleteFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_D).commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(t3).deleteWith(deletedFiles::add).commit();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
  }

  @Test
  public void dataFilesCleanup() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_B).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D)).commit();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C)).commit();
    long fourthSnapshotId = table.currentSnapshot().snapshotId();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(Status.EXISTING, thirdSnapshotId, FILE_C),
            manifestEntry(Status.EXISTING, fourthSnapshotId, FILE_D));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    manifests.forEach(rewriteManifests::deleteManifest);
    rewriteManifests.addManifest(newManifest);
    rewriteManifests.commit();

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(t4).deleteWith(deletedFiles::add).commit();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
    Assert.assertTrue("FILE_B should be deleted", deletedFiles.contains(FILE_B.path().toString()));
  }

  @Test
  public void dataFilesCleanupWithParallelTasks() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_B).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D)).commit();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C)).commit();
    long fourthSnapshotId = table.currentSnapshot().snapshotId();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());

    ManifestFile newManifest =
        writeManifest(
            "manifest-file-1.avro",
            manifestEntry(Status.EXISTING, thirdSnapshotId, FILE_C),
            manifestEntry(Status.EXISTING, fourthSnapshotId, FILE_D));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    manifests.forEach(rewriteManifests::deleteManifest);
    rewriteManifests.addManifest(newManifest);
    rewriteManifests.commit();

    Set<String> deletedFiles = Sets.newHashSet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);

    removeSnapshots(table)
        .executeDeleteWith(
            Executors.newFixedThreadPool(
                4,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("remove-snapshot-" + deleteThreadsIndex.getAndIncrement());
                  thread.setDaemon(
                      true); // daemon threads will be terminated abruptly when the JVM exits
                  return thread;
                }))
        .planWith(
            Executors.newFixedThreadPool(
                1,
                runnable -> {
                  Thread thread = new Thread(runnable);
                  thread.setName("plan-" + planThreadsIndex.getAndIncrement());
                  thread.setDaemon(
                      true); // daemon threads will be terminated abruptly when the JVM exits
                  return thread;
                }))
        .expireOlderThan(t4)
        .deleteWith(
            s -> {
              deleteThreads.add(Thread.currentThread().getName());
              deletedFiles.add(s);
            })
        .commit();

    // Verifies that the delete methods ran in the threads created by the provided ExecutorService
    // ThreadFactory
    Assert.assertEquals(
        deleteThreads,
        Sets.newHashSet(
            "remove-snapshot-0", "remove-snapshot-1", "remove-snapshot-2", "remove-snapshot-3"));

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
    Assert.assertTrue("FILE_B should be deleted", deletedFiles.contains(FILE_B.path().toString()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void noDataFileCleanup() throws IOException {
    table.newFastAppend().appendFile(FILE_A).commit();

    table.newFastAppend().appendFile(FILE_B).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D)).commit();

    table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C)).commit();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table)
        .cleanExpiredFiles(false)
        .expireOlderThan(t4)
        .deleteWith(deletedFiles::add)
        .commit();

    Assert.assertTrue("No files should have been deleted", deletedFiles.isEmpty());
  }

  /**
   * Test on table below, and expiring the staged commit `B` using `expireOlderThan` API. Table: A -
   * C ` B (staged)
   */
  @Test
  public void testWithExpiringDanglingStageCommit() {
    // `A` commit
    table.newAppend().appendFile(FILE_A).commit();

    // `B` staged commit
    table.newAppend().appendFile(FILE_B).stageOnly().commit();

    TableMetadata base = readMetadata();
    Snapshot snapshotA = base.snapshots().get(0);
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit
    table.newAppend().appendFile(FILE_C).commit();

    Set<String> deletedFiles = Sets.newHashSet();

    // Expire all commits including dangling staged snapshot.
    removeSnapshots(table)
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotB.timestampMillis() + 1)
        .commit();

    Set<String> expectedDeletes = Sets.newHashSet();
    expectedDeletes.add(snapshotA.manifestListLocation());

    // Files should be deleted of dangling staged snapshot
    snapshotB
        .addedDataFiles(table.io())
        .forEach(
            i -> {
              expectedDeletes.add(i.path().toString());
            });

    // ManifestList should be deleted too
    expectedDeletes.add(snapshotB.manifestListLocation());
    snapshotB
        .dataManifests(table.io())
        .forEach(
            file -> {
              // Only the manifest of B should be deleted.
              if (file.snapshotId() == snapshotB.snapshotId()) {
                expectedDeletes.add(file.path());
              }
            });
    Assert.assertSame(
        "Files deleted count should be expected", expectedDeletes.size(), deletedFiles.size());
    // Take the diff
    expectedDeletes.removeAll(deletedFiles);
    Assert.assertTrue("Exactly same files should be deleted", expectedDeletes.isEmpty());
  }

  /**
   * Expire cherry-pick the commit as shown below, when `B` is in table's current state Table: A - B
   * - C <--current snapshot `- D (source=B)
   */
  @Test
  public void testWithCherryPickTableSnapshot() {
    // `A` commit
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotA = table.currentSnapshot();

    // `B` commit
    Set<String> deletedAFiles = Sets.newHashSet();
    table.newOverwrite().addFile(FILE_B).deleteFile(FILE_A).deleteWith(deletedAFiles::add).commit();
    Assert.assertTrue("No files should be physically deleted", deletedAFiles.isEmpty());

    // pick the snapshot 'B`
    Snapshot snapshotB = readMetadata().currentSnapshot();

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend().appendFile(FILE_C).commit();
    Snapshot snapshotC = readMetadata().currentSnapshot();

    // Move the table back to `A`
    table.manageSnapshots().setCurrentSnapshot(snapshotA.snapshotId()).commit();

    // Generate A -> `D (B)`
    table.manageSnapshots().cherrypick(snapshotB.snapshotId()).commit();
    Snapshot snapshotD = readMetadata().currentSnapshot();

    // Move the table back to `C`
    table.manageSnapshots().setCurrentSnapshot(snapshotC.snapshotId()).commit();
    List<String> deletedFiles = Lists.newArrayList();

    // Expire `C`
    removeSnapshots(table)
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotC.timestampMillis() + 1)
        .commit();

    // Make sure no dataFiles are deleted for the B, C, D snapshot
    Lists.newArrayList(snapshotB, snapshotC, snapshotD)
        .forEach(
            i -> {
              i.addedDataFiles(table.io())
                  .forEach(
                      item -> {
                        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
                      });
            });
  }

  /**
   * Test on table below, and expiring `B` which is not in current table state. 1) Expire `B` 2) All
   * commit Table: A - C - D (B) ` B (staged)
   */
  @Test
  public void testWithExpiringStagedThenCherrypick() {
    // `A` commit
    table.newAppend().appendFile(FILE_A).commit();

    // `B` commit
    table.newAppend().appendFile(FILE_B).stageOnly().commit();

    // pick the snapshot that's staged but not committed
    TableMetadata base = readMetadata();
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend().appendFile(FILE_C).commit();

    // `D (B)` cherry-pick commit
    table.manageSnapshots().cherrypick(snapshotB.snapshotId()).commit();

    base = readMetadata();
    Snapshot snapshotD = base.snapshots().get(3);

    List<String> deletedFiles = Lists.newArrayList();

    // Expire `B` commit.
    removeSnapshots(table)
        .deleteWith(deletedFiles::add)
        .expireSnapshotId(snapshotB.snapshotId())
        .commit();

    // Make sure no dataFiles are deleted for the staged snapshot
    Lists.newArrayList(snapshotB)
        .forEach(
            i -> {
              i.addedDataFiles(table.io())
                  .forEach(
                      item -> {
                        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
                      });
            });

    // Expire all snapshots including cherry-pick
    removeSnapshots(table)
        .deleteWith(deletedFiles::add)
        .expireOlderThan(table.currentSnapshot().timestampMillis() + 1)
        .commit();

    // Make sure no dataFiles are deleted for the staged and cherry-pick
    Lists.newArrayList(snapshotB, snapshotD)
        .forEach(
            i -> {
              i.addedDataFiles(table.io())
                  .forEach(
                      item -> {
                        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
                      });
            });
  }

  @Test
  public void testExpireSnapshotsWhenGarbageCollectionDisabled() {
    table.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();

    table.newAppend().appendFile(FILE_A).commit();

    Assertions.assertThatThrownBy(() -> table.expireSnapshots())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot expire snapshots: GC is disabled");
  }

  @Test
  public void testExpireWithDefaultRetainLast() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    Assert.assertEquals("Expected 3 snapshots", 3, Iterables.size(table.snapshots()));

    table.updateProperties().set(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "3").commit();

    Set<String> deletedFiles = Sets.newHashSet();

    Snapshot snapshotBeforeExpiration = table.currentSnapshot();

    removeSnapshots(table)
        .expireOlderThan(System.currentTimeMillis())
        .deleteWith(deletedFiles::add)
        .commit();

    Assert.assertEquals(
        "Should not change current snapshot", snapshotBeforeExpiration, table.currentSnapshot());
    Assert.assertEquals("Should keep 3 snapshots", 3, Iterables.size(table.snapshots()));
    Assert.assertTrue("Should not delete data", deletedFiles.isEmpty());
  }

  @Test
  public void testExpireWithDefaultSnapshotAge() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    waitUntilAfter(firstSnapshot.timestampMillis());

    table.newAppend().appendFile(FILE_B).commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    waitUntilAfter(secondSnapshot.timestampMillis());

    table.newAppend().appendFile(FILE_C).commit();
    Snapshot thirdSnapshot = table.currentSnapshot();

    waitUntilAfter(thirdSnapshot.timestampMillis());

    Assert.assertEquals("Expected 3 snapshots", 3, Iterables.size(table.snapshots()));

    table.updateProperties().set(TableProperties.MAX_SNAPSHOT_AGE_MS, "1").commit();

    Set<String> deletedFiles = Sets.newHashSet();

    // rely solely on default configs
    removeSnapshots(table).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Should not change current snapshot", thirdSnapshot, table.currentSnapshot());
    Assert.assertEquals("Should keep 1 snapshot", 1, Iterables.size(table.snapshots()));
    Assert.assertEquals(
        "Should remove expired manifest lists",
        Sets.newHashSet(
            firstSnapshot.manifestListLocation(), secondSnapshot.manifestListLocation()),
        deletedFiles);
  }

  @Test
  public void testExpireWithDeleteFiles() {
    Assume.assumeTrue("Delete files only supported in V2 spec", formatVersion == 2);

    // Data Manifest => File_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    // Data Manifest => FILE_A
    // Delete Manifest => FILE_A_DELETES
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot secondSnapshot = table.currentSnapshot();
    Assert.assertEquals(
        "Should have 1 data manifest", 1, secondSnapshot.dataManifests(table.io()).size());
    Assert.assertEquals(
        "Should have 1 delete manifest", 1, secondSnapshot.deleteManifests(table.io()).size());

    // FILE_A and FILE_A_DELETES move into "DELETED" state
    table
        .newRewrite()
        .rewriteFiles(
            ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES), // deleted
            ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_B_DELETES)) // added
        .validateFromSnapshot(secondSnapshot.snapshotId())
        .commit();
    Snapshot thirdSnapshot = table.currentSnapshot();
    Set<ManifestFile> manifestOfDeletedFiles =
        thirdSnapshot.allManifests(table.io()).stream()
            .filter(ManifestFile::hasDeletedFiles)
            .collect(Collectors.toSet());
    Assert.assertEquals(
        "Should have two manifests of deleted files", 2, manifestOfDeletedFiles.size());

    // Need one more commit before manifests of files of DELETED state get cleared from current
    // snapshot.
    table.newAppend().appendFile(FILE_C).commit();

    Snapshot fourthSnapshot = table.currentSnapshot();
    long fourthSnapshotTs = waitUntilAfter(fourthSnapshot.timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();
    removeSnapshots(table).expireOlderThan(fourthSnapshotTs).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(
        "Should remove old delete files and delete file manifests",
        ImmutableSet.builder()
            .add(FILE_A.path())
            .add(FILE_A_DELETES.path())
            .add(firstSnapshot.manifestListLocation())
            .add(secondSnapshot.manifestListLocation())
            .add(thirdSnapshot.manifestListLocation())
            .addAll(manifestPaths(secondSnapshot, table.io()))
            .addAll(
                manifestOfDeletedFiles.stream()
                    .map(ManifestFile::path)
                    .collect(Collectors.toList()))
            .build(),
        deletedFiles);
  }

  @Test
  public void testTagExpiration() {
    table.newAppend().appendFile(FILE_A).commit();

    long now = System.currentTimeMillis();
    long maxAgeMs = 100;
    long expirationTime = now + maxAgeMs;

    table
        .manageSnapshots()
        .createTag("tag", table.currentSnapshot().snapshotId())
        .setMaxRefAgeMs("tag", maxAgeMs)
        .commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();

    waitUntilAfter(expirationTime);

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    Assert.assertNull(table.ops().current().ref("tag"));
    Assert.assertNotNull(table.ops().current().ref("branch"));
    Assert.assertNotNull(table.ops().current().ref(SnapshotRef.MAIN_BRANCH));
  }

  @Test
  public void testBranchExpiration() {
    table.newAppend().appendFile(FILE_A).commit();

    long now = System.currentTimeMillis();
    long maxAgeMs = 100;
    long expirationTime = now + maxAgeMs;

    table
        .manageSnapshots()
        .createBranch("branch", table.currentSnapshot().snapshotId())
        .setMaxRefAgeMs("branch", maxAgeMs)
        .commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.manageSnapshots().createTag("tag", table.currentSnapshot().snapshotId()).commit();

    waitUntilAfter(expirationTime);

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    Assert.assertNull(table.ops().current().ref("branch"));
    Assert.assertNotNull(table.ops().current().ref("tag"));
    Assert.assertNotNull(table.ops().current().ref(SnapshotRef.MAIN_BRANCH));
  }

  @Test
  public void testMultipleRefsAndCleanExpiredFilesFailsForIncrementalCleanup() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newDelete().deleteFile(FILE_A).commit();
    table.manageSnapshots().createTag("TagA", table.currentSnapshot().snapshotId()).commit();
    waitUntilAfter(table.currentSnapshot().timestampMillis());
    RemoveSnapshots removeSnapshots = (RemoveSnapshots) table.expireSnapshots();

    Assertions.assertThatThrownBy(
            () ->
                removeSnapshots
                    .withIncrementalCleanup(true)
                    .expireOlderThan(table.currentSnapshot().timestampMillis())
                    .cleanExpiredFiles(true)
                    .commit())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot incrementally clean files for tables with more than 1 ref");
  }

  @Test
  public void testExpireWithStatisticsFiles() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    String statsFileLocation1 = statsFileLocation(table.location());
    StatisticsFile statisticsFile1 =
        writeStatsFile(
            table.currentSnapshot().snapshotId(),
            table.currentSnapshot().sequenceNumber(),
            statsFileLocation1,
            table.io());
    commitStats(table, statisticsFile1);

    table.newAppend().appendFile(FILE_B).commit();
    String statsFileLocation2 = statsFileLocation(table.location());
    StatisticsFile statisticsFile2 =
        writeStatsFile(
            table.currentSnapshot().snapshotId(),
            table.currentSnapshot().sequenceNumber(),
            statsFileLocation2,
            table.io());
    commitStats(table, statisticsFile2);
    Assert.assertEquals("Should have 2 statistics file", 2, table.statisticsFiles().size());

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file should be retained
    Assert.assertEquals("Should keep 1 snapshot", 1, Iterables.size(table.snapshots()));
    Assertions.assertThat(table.statisticsFiles())
        .hasSize(1)
        .extracting(StatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));

    Assertions.assertThat(new File(statsFileLocation1).exists()).isFalse();
    Assertions.assertThat(new File(statsFileLocation2).exists()).isTrue();
  }

  @Test
  public void testExpireWithStatisticsFilesWithReuse() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    String statsFileLocation1 = statsFileLocation(table.location());
    StatisticsFile statisticsFile1 =
        writeStatsFile(
            table.currentSnapshot().snapshotId(),
            table.currentSnapshot().sequenceNumber(),
            statsFileLocation1,
            table.io());
    commitStats(table, statisticsFile1);

    table.newAppend().appendFile(FILE_B).commit();
    // If an expired snapshot's stats file is reused for some reason by the live snapshots,
    // that stats file should not get deleted from the file system as the live snapshots still
    // reference it.
    StatisticsFile statisticsFile2 =
        reuseStatsFile(table.currentSnapshot().snapshotId(), statisticsFile1);
    commitStats(table, statisticsFile2);

    Assert.assertEquals("Should have 2 statistics file", 2, table.statisticsFiles().size());

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file (reused from previous snapshot) should be
    // retained
    Assert.assertEquals("Should keep 1 snapshot", 1, Iterables.size(table.snapshots()));
    Assertions.assertThat(table.statisticsFiles())
        .hasSize(1)
        .extracting(StatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));
    // the reused stats file should exist.
    Assertions.assertThat(new File(statsFileLocation1).exists()).isTrue();
  }

  @Test
  public void testFailRemovingSnapshotWhenStillReferencedByBranch() {
    table.newAppend().appendFile(FILE_A).commit();

    AppendFiles append = table.newAppend().appendFile(FILE_B).stageOnly();

    long snapshotId = append.apply().snapshotId();

    append.commit();

    table.manageSnapshots().createBranch("branch", snapshotId).commit();

    Assertions.assertThatThrownBy(
            () -> removeSnapshots(table).expireSnapshotId(snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot expire 2. Still referenced by refs: [branch]");
  }

  @Test
  public void testFailRemovingSnapshotWhenStillReferencedByTag() {
    table.newAppend().appendFile(FILE_A).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createTag("tag", snapshotId).commit();

    // commit another snapshot so the first one isn't referenced by main
    table.newAppend().appendFile(FILE_B).commit();

    Assertions.assertThatThrownBy(
            () -> removeSnapshots(table).expireSnapshotId(snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot expire 1. Still referenced by refs: [tag]");
  }

  @Test
  public void testRetainUnreferencedSnapshotsWithinExpirationAge() {
    table.newAppend().appendFile(FILE_A).commit();

    long expireTimestampSnapshotA = waitUntilAfter(table.currentSnapshot().timestampMillis());
    waitUntilAfter(expireTimestampSnapshotA);

    table.newAppend().appendFile(FILE_B).stageOnly().commit();

    table.newAppend().appendFile(FILE_C).commit();

    removeSnapshots(table).expireOlderThan(expireTimestampSnapshotA).commit();

    Assert.assertEquals(2, table.ops().current().snapshots().size());
  }

  @Test
  public void testUnreferencedSnapshotParentOfTag() {
    table.newAppend().appendFile(FILE_A).commit();

    long initialSnapshotId = table.currentSnapshot().snapshotId();

    // this will be expired because it is still unreferenced with a tag on its child snapshot
    table.newAppend().appendFile(FILE_B).commit();

    long expiredSnapshotId = table.currentSnapshot().snapshotId();

    long expireTimestampSnapshotB = waitUntilAfter(table.currentSnapshot().timestampMillis());
    waitUntilAfter(expireTimestampSnapshotB);

    table.newAppend().appendFile(FILE_C).commit();

    // create a tag that references the current history and rewrite main to point to the initial
    // snapshot
    table
        .manageSnapshots()
        .createTag("tag", table.currentSnapshot().snapshotId())
        .replaceBranch("main", initialSnapshotId)
        .commit();

    removeSnapshots(table)
        .expireOlderThan(expireTimestampSnapshotB)
        .cleanExpiredFiles(false)
        .commit();

    Assert.assertNull(
        "Should remove unreferenced snapshot beneath a tag", table.snapshot(expiredSnapshotId));
    Assert.assertEquals(2, table.ops().current().snapshots().size());
  }

  @Test
  public void testSnapshotParentOfBranchNotUnreferenced() {
    // similar to testUnreferencedSnapshotParentOfTag, but checks that branch history is not
    // considered unreferenced
    table.newAppend().appendFile(FILE_A).commit();

    long initialSnapshotId = table.currentSnapshot().snapshotId();

    // this will be expired because it is still unreferenced with a tag on its child snapshot
    table.newAppend().appendFile(FILE_B).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    long expireTimestampSnapshotB = waitUntilAfter(table.currentSnapshot().timestampMillis());
    waitUntilAfter(expireTimestampSnapshotB);

    table.newAppend().appendFile(FILE_C).commit();

    // create a branch that references the current history and rewrite main to point to the initial
    // snapshot
    table
        .manageSnapshots()
        .createBranch("branch", table.currentSnapshot().snapshotId())
        .setMaxSnapshotAgeMs("branch", Long.MAX_VALUE)
        .replaceBranch("main", initialSnapshotId)
        .commit();

    removeSnapshots(table)
        .expireOlderThan(expireTimestampSnapshotB)
        .cleanExpiredFiles(false)
        .commit();

    Assert.assertNotNull("Should not remove snapshot beneath a branch", table.snapshot(snapshotId));
    Assert.assertEquals(3, table.ops().current().snapshots().size());
  }

  @Test
  public void testMinSnapshotsToKeepMultipleBranches() {
    table.newAppend().appendFile(FILE_A).commit();
    long initialSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();

    // stage a snapshot and get its id
    AppendFiles append = table.newAppend().appendFile(FILE_C).stageOnly();
    long branchSnapshotId = append.apply().snapshotId();
    append.commit();

    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    long maxSnapshotAgeMs = 1;
    long expirationTime = System.currentTimeMillis() + maxSnapshotAgeMs;

    // configure main so that the initial snapshot will expire
    table
        .manageSnapshots()
        .setMinSnapshotsToKeep(SnapshotRef.MAIN_BRANCH, 1)
        .setMaxSnapshotAgeMs(SnapshotRef.MAIN_BRANCH, 1)
        .commit();

    // retain 3 snapshots on branch (including the initial snapshot)
    table
        .manageSnapshots()
        .createBranch("branch", branchSnapshotId)
        .setMinSnapshotsToKeep("branch", 3)
        .setMaxSnapshotAgeMs("branch", maxSnapshotAgeMs)
        .commit();

    waitUntilAfter(expirationTime);
    table.expireSnapshots().cleanExpiredFiles(false).commit();

    Assert.assertEquals(
        "Should have 3 snapshots (none removed)", 3, Iterables.size(table.snapshots()));

    // stop retaining snapshots from the branch
    table.manageSnapshots().setMinSnapshotsToKeep("branch", 1).commit();

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    Assert.assertEquals(
        "Should have 2 snapshots (initial removed)", 2, Iterables.size(table.snapshots()));
    Assert.assertNull(table.ops().current().snapshot(initialSnapshotId));
  }

  @Test
  public void testMaxSnapshotAgeMultipleBranches() {
    table.newAppend().appendFile(FILE_A).commit();
    long initialSnapshotId = table.currentSnapshot().snapshotId();

    long ageMs = 10;
    long expirationTime = System.currentTimeMillis() + ageMs;

    waitUntilAfter(expirationTime);

    table.newAppend().appendFile(FILE_B).commit();

    // configure main so that the initial snapshot will expire
    table
        .manageSnapshots()
        .setMaxSnapshotAgeMs(SnapshotRef.MAIN_BRANCH, ageMs)
        .setMinSnapshotsToKeep(SnapshotRef.MAIN_BRANCH, 1)
        .commit();

    // stage a snapshot and get its id
    AppendFiles append = table.newAppend().appendFile(FILE_C).stageOnly();
    long branchSnapshotId = append.apply().snapshotId();
    append.commit();

    Assert.assertEquals("Should have 3 snapshots", 3, Iterables.size(table.snapshots()));

    // retain all snapshots on branch (including the initial snapshot)
    table
        .manageSnapshots()
        .createBranch("branch", branchSnapshotId)
        .setMinSnapshotsToKeep("branch", 1)
        .setMaxSnapshotAgeMs("branch", Long.MAX_VALUE)
        .commit();

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    Assert.assertEquals(
        "Should have 3 snapshots (none removed)", 3, Iterables.size(table.snapshots()));

    // allow the initial snapshot to age off from branch
    table.manageSnapshots().setMaxSnapshotAgeMs("branch", ageMs).commit();

    table.expireSnapshots().cleanExpiredFiles(false).commit();

    Assert.assertEquals(
        "Should have 2 snapshots (initial removed)", 2, Iterables.size(table.snapshots()));
    Assert.assertNull(table.ops().current().snapshot(initialSnapshotId));
  }

  @Test
  public void testRetainFilesOnRetainedBranches() {
    // Append a file to main and test branch
    String testBranch = "test-branch";
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot appendA = table.currentSnapshot();
    table.manageSnapshots().createBranch(testBranch, appendA.snapshotId()).commit();

    // Delete A from main
    table.newDelete().deleteFile(FILE_A).commit();
    Snapshot deletionA = table.currentSnapshot();
    // Add B to main
    table.newAppend().appendFile(FILE_B).commit();
    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();
    Set<String> expectedDeletes = Sets.newHashSet();

    // Only deletionA's manifest list and manifests should be removed
    expectedDeletes.add(deletionA.manifestListLocation());
    expectedDeletes.addAll(manifestPaths(deletionA, table.io()));
    table.expireSnapshots().expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    Assert.assertEquals(2, Iterables.size(table.snapshots()));
    Assert.assertEquals(expectedDeletes, deletedFiles);

    // Delete A on test branch
    table.newDelete().deleteFile(FILE_A).toBranch(testBranch).commit();
    Snapshot branchDelete = table.snapshot(testBranch);

    // Append C on test branch
    table.newAppend().appendFile(FILE_C).toBranch(testBranch).commit();
    Snapshot testBranchHead = table.snapshot(testBranch);

    deletedFiles = Sets.newHashSet();
    expectedDeletes = Sets.newHashSet();

    waitUntilAfter(testBranchHead.timestampMillis());
    table
        .expireSnapshots()
        .expireOlderThan(testBranchHead.timestampMillis())
        .deleteWith(deletedFiles::add)
        .commit();

    expectedDeletes.add(appendA.manifestListLocation());
    expectedDeletes.addAll(manifestPaths(appendA, table.io()));
    expectedDeletes.add(branchDelete.manifestListLocation());
    expectedDeletes.addAll(manifestPaths(branchDelete, table.io()));
    expectedDeletes.add(FILE_A.path().toString());

    Assert.assertEquals(2, Iterables.size(table.snapshots()));
    Assert.assertEquals(expectedDeletes, deletedFiles);
  }

  private Set<String> manifestPaths(Snapshot snapshot, FileIO io) {
    return snapshot.allManifests(io).stream().map(ManifestFile::path).collect(Collectors.toSet());
  }

  private RemoveSnapshots removeSnapshots(Table table) {
    RemoveSnapshots removeSnapshots = (RemoveSnapshots) table.expireSnapshots();
    return (RemoveSnapshots) removeSnapshots.withIncrementalCleanup(incrementalCleanup);
  }

  private StatisticsFile writeStatsFile(
      long snapshotId, long snapshotSequenceNumber, String statsLocation, FileIO fileIO)
      throws IOException {
    try (PuffinWriter puffinWriter = Puffin.write(fileIO.newOutputFile(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              ImmutableList.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();

      return new GenericStatisticsFile(
          snapshotId,
          statsLocation,
          puffinWriter.fileSize(),
          puffinWriter.footerSize(),
          puffinWriter.writtenBlobsMetadata().stream()
              .map(GenericBlobMetadata::from)
              .collect(ImmutableList.toImmutableList()));
    }
  }

  private StatisticsFile reuseStatsFile(long snapshotId, StatisticsFile statisticsFile) {
    return new GenericStatisticsFile(
        snapshotId,
        statisticsFile.path(),
        statisticsFile.fileSizeInBytes(),
        statisticsFile.fileFooterSizeInBytes(),
        statisticsFile.blobMetadata());
  }

  private void commitStats(Table table, StatisticsFile statisticsFile) {
    table.updateStatistics().setStatistics(statisticsFile.snapshotId(), statisticsFile).commit();
  }

  private String statsFileLocation(String tableLocation) {
    String statsFileName = "stats-file-" + UUID.randomUUID();
    return tableLocation + "/metadata/" + statsFileName;
  }
}
