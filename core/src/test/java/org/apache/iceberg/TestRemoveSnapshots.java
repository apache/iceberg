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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestRemoveSnapshots extends TableTestBase {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { 1 },
        new Object[] { 2 },
    };
  }

  public TestRemoveSnapshots(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testRetainLastWithExpireOlderThan() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots
    table.expireSnapshots()
        .expireOlderThan(t3)
        .retainLast(2)
        .commit();

    Assert.assertEquals("Should have two snapshots.",
        2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("First snapshot should not present.",
        null, table.snapshot(firstSnapshotId));
  }

  @Test
  public void testRetainLastWithExpireById() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 3 snapshots, but explicitly remove the first snapshot
    table.expireSnapshots()
        .expireSnapshotId(firstSnapshotId)
        .retainLast(3)
        .commit();

    Assert.assertEquals("Should have two snapshots.",
        2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("First snapshot should not present.",
        null, table.snapshot(firstSnapshotId));
  }

  @Test
  public void testRetainNAvailableSnapshotsWithTransaction() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots
    Transaction tx = table.newTransaction();
    tx.expireSnapshots()
        .expireOlderThan(t3)
        .retainLast(2)
        .commit();
    tx.commitTransaction();

    Assert.assertEquals("Should have two snapshots.",
        2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("First snapshot should not present.",
        null, table.snapshot(firstSnapshotId));
  }

  @Test
  public void testRetainLastWithTooFewSnapshots() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .appendFile(FILE_B) // data_bucket=1
        .commit();
    long firstSnapshotId = table.currentSnapshot().snapshotId();

    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    // Retain last 3 snapshots
    table.expireSnapshots()
        .expireOlderThan(t2)
        .retainLast(3)
        .commit();

    Assert.assertEquals("Should have two snapshots",
        2, Lists.newArrayList(table.snapshots()).size());
    Assert.assertEquals("First snapshot should still present",
        firstSnapshotId, table.snapshot(firstSnapshotId).snapshotId());
  }

  @Test
  public void testRetainLastKeepsExpiringSnapshot() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_D) // data_bucket=3
        .commit();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    table.expireSnapshots()
        .expireOlderThan(secondSnapshot.timestampMillis())
        .retainLast(2)
        .commit();

    Assert.assertEquals("Should have three snapshots.",
        3, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNotNull("Second snapshot should present.",
        table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testExpireOlderThanMultipleCalls() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    Snapshot thirdSnapshot = table.currentSnapshot();
    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    table.expireSnapshots()
        .expireOlderThan(secondSnapshot.timestampMillis())
        .expireOlderThan(thirdSnapshot.timestampMillis())
        .commit();

    Assert.assertEquals("Should have one snapshots.",
        1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNull("Second snapshot should not present.",
        table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testRetainLastMultipleCalls() {
    long t0 = System.currentTimeMillis();
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();
    long t1 = System.currentTimeMillis();
    while (t1 <= table.currentSnapshot().timestampMillis()) {
      t1 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_B) // data_bucket=1
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    long t2 = System.currentTimeMillis();
    while (t2 <= table.currentSnapshot().timestampMillis()) {
      t2 = System.currentTimeMillis();
    }

    table.newAppend()
        .appendFile(FILE_C) // data_bucket=2
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    // Retain last 2 snapshots and expire older than t3
    table.expireSnapshots()
        .expireOlderThan(t3)
        .retainLast(2)
        .retainLast(1)
        .commit();

    Assert.assertEquals("Should have one snapshots.",
        1, Lists.newArrayList(table.snapshots()).size());
    Assert.assertNull("Second snapshot should not present.",
        table.snapshot(secondSnapshot.snapshotId()));
  }

  @Test
  public void testRetainZeroSnapshots() {
    AssertHelpers.assertThrows("Should fail retain 0 snapshots " +
            "because number of snapshots to retain cannot be zero",
        IllegalArgumentException.class,
        "Number of snapshots to retain must be at least 1, cannot be: 0",
        () -> table.expireSnapshots().retainLast(0).commit());
  }

  @Test
  public void dataFilesCleanup() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_D))
        .commit();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    table.newRewrite()
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_C))
        .commit();
    long fourthSnapshotId = table.currentSnapshot().snapshotId();

    long t4 = System.currentTimeMillis();
    while (t4 <= table.currentSnapshot().timestampMillis()) {
      t4 = System.currentTimeMillis();
    }

    List<ManifestFile> manifests = table.currentSnapshot().manifests();

    ManifestFile newManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(Status.EXISTING, thirdSnapshotId, FILE_C),
        manifestEntry(Status.EXISTING, fourthSnapshotId, FILE_D));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    manifests.forEach(rewriteManifests::deleteManifest);
    rewriteManifests.addManifest(newManifest);
    rewriteManifests.commit();

    Set<String> deletedFiles = Sets.newHashSet();

    table.expireSnapshots()
        .expireOlderThan(t4)
        .deleteWith(deletedFiles::add)
        .commit();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
    Assert.assertTrue("FILE_B should be deleted", deletedFiles.contains(FILE_B.path().toString()));
  }

  /**
   * Test on table below, and expiring the staged commit `B` using `expireOlderThan` API.
   * Table: A - C
   *          ` B (staged)
   */
  @Test
  public void testWithExpiringDanglingStageCommit() {
    // `A` commit
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // `B` staged commit
    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    TableMetadata base = readMetadata();
    Snapshot snapshotA = base.snapshots().get(0);
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    Set<String> deletedFiles = new HashSet<>();

    // Expire all commits including dangling staged snapshot.
    table.expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotB.timestampMillis() + 1)
        .commit();

    Set<String> expectedDeletes = new HashSet<>();
    expectedDeletes.add(snapshotA.manifestListLocation());

    // Files should be deleted of dangling staged snapshot
    snapshotB.addedFiles().forEach(i -> {
      expectedDeletes.add(i.path().toString());
    });

    // ManifestList should be deleted too
    expectedDeletes.add(snapshotB.manifestListLocation());
    snapshotB.manifests().forEach(file -> {
      //Only the manifest of B should be deleted.
      if (file.snapshotId() == snapshotB.snapshotId()) {
        expectedDeletes.add(file.path());
      }
    });
    Assert.assertSame("Files deleted count should be expected", expectedDeletes.size(), deletedFiles.size());
    //Take the diff
    expectedDeletes.removeAll(deletedFiles);
    Assert.assertTrue("Exactly same files should be deleted", expectedDeletes.isEmpty());
  }

  /**
   * Expire cherry-pick the commit as shown below, when `B` is in table's current state
   *  Table:
   *  A - B - C <--current snapshot
   *   `- D (source=B)
   */
  @Test
  public void testWithCherryPickTableSnapshot() {
    // `A` commit
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    Snapshot snapshotA = table.currentSnapshot();

    // `B` commit
    Set<String> deletedAFiles = new HashSet<>();
    table.newOverwrite()
        .addFile(FILE_B)
        .deleteFile(FILE_A)
        .deleteWith(deletedAFiles::add)
        .commit();
    Assert.assertTrue("No files should be physically deleted", deletedAFiles.isEmpty());

    // pick the snapshot 'B`
    Snapshot snapshotB = readMetadata().currentSnapshot();

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    Snapshot snapshotC = readMetadata().currentSnapshot();

    // Move the table back to `A`
    table.manageSnapshots()
        .setCurrentSnapshot(snapshotA.snapshotId())
        .commit();

    // Generate A -> `D (B)`
    table.manageSnapshots()
        .cherrypick(snapshotB.snapshotId())
        .commit();
    Snapshot snapshotD = readMetadata().currentSnapshot();

    // Move the table back to `C`
    table.manageSnapshots()
        .setCurrentSnapshot(snapshotC.snapshotId())
        .commit();
    List<String> deletedFiles = new ArrayList<>();

    // Expire `C`
    table.expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotC.timestampMillis() + 1)
        .commit();

    // Make sure no dataFiles are deleted for the B, C, D snapshot
    Lists.newArrayList(snapshotB, snapshotC, snapshotD).forEach(i -> {
      i.addedFiles().forEach(item -> {
        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
      });
    });
  }

  /**
   * Test on table below, and expiring `B` which is not in current table state.
   *  1) Expire `B`
   *  2) All commit
   * Table: A - C - D (B)
   *          ` B (staged)
   */
  @Test
  public void testWithExpiringStagedThenCherrypick() {
    // `A` commit
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    // `B` commit
    table.newAppend()
        .appendFile(FILE_B)
        .stageOnly()
        .commit();

    // pick the snapshot that's staged but not committed
    TableMetadata base = readMetadata();
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    // `D (B)` cherry-pick commit
    table.manageSnapshots()
        .cherrypick(snapshotB.snapshotId())
        .commit();

    base = readMetadata();
    Snapshot snapshotD = base.snapshots().get(3);

    List<String> deletedFiles = new ArrayList<>();

    // Expire `B` commit.
    table.expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireSnapshotId(snapshotB.snapshotId())
        .commit();

    // Make sure no dataFiles are deleted for the staged snapshot
    Lists.newArrayList(snapshotB).forEach(i -> {
      i.addedFiles().forEach(item -> {
        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
      });
    });

    // Expire all snapshots including cherry-pick
    table.expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(table.currentSnapshot().timestampMillis() + 1)
        .commit();

    // Make sure no dataFiles are deleted for the staged and cherry-pick
    Lists.newArrayList(snapshotB, snapshotD).forEach(i -> {
      i.addedFiles().forEach(item -> {
        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
      });
    });
  }
}
