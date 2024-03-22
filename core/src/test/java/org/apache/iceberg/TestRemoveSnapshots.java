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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRemoveSnapshots extends TestBase {
  @Parameter(index = 1)
  private boolean incrementalCleanup;

  @Parameters(name = "formatVersion = {0}, incrementalCleanup = {1}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {1, true},
        new Object[] {2, true},
        new Object[] {1, false},
        new Object[] {2, false});
  }

  private long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  @TestTemplate
  public void testExpireOlderThan() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newAppend().appendFile(FILE_B).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(table.snapshot(firstSnapshot.snapshotId())).isNull();
    assertThat(deletedFiles).containsExactly(firstSnapshot.manifestListLocation());
  }

  @TestTemplate
  public void testExpireOlderThanWithDelete() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    assertThat(firstSnapshot.allManifests(table.io())).hasSize(1);

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newDelete().deleteFile(FILE_A).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    assertThat(secondSnapshot.allManifests(table.io()))
        .as("Should create replace manifest with a rewritten manifest")
        .hasSize(1);

    table.newAppend().appendFile(FILE_B).commit();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(table.snapshot(firstSnapshot.snapshotId())).isNull();
    assertThat(table.snapshot(secondSnapshot.snapshotId())).isNull();

    assertThat(deletedFiles)
        .as("Should remove expired manifest lists and deleted data file")
        .isEqualTo(
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
                FILE_A.path().toString() // deleted
                ));
  }

  @TestTemplate
  public void testExpireOlderThanWithDeleteInMergedManifests() {
    // merge every commit
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    assertThat(firstSnapshot.allManifests(table.io())).hasSize(1);

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table
        .newDelete()
        .deleteFile(FILE_A) // FILE_B is still in the dataset
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    assertThat(secondSnapshot.allManifests(table.io()))
        .as("Should replace manifest with a rewritten manifest")
        .hasSize(1);

    table
        .newFastAppend() // do not merge to keep the last snapshot's manifest valid
        .appendFile(FILE_C)
        .commit();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(table.snapshot(firstSnapshot.snapshotId())).isNull();
    assertThat(table.snapshot(secondSnapshot.snapshotId())).isNull();

    assertThat(deletedFiles)
        .as("Should remove expired manifest lists and deleted data file")
        .isEqualTo(
            Sets.newHashSet(
                firstSnapshot.manifestListLocation(), // snapshot expired
                firstSnapshot
                    .allManifests(table.io())
                    .get(0)
                    .path(), // manifest was rewritten for delete
                secondSnapshot.manifestListLocation(), // snapshot expired
                FILE_A.path()));
  }

  @TestTemplate
  public void testExpireOlderThanWithRollback() {
    // merge every commit
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "0").commit();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    assertThat(firstSnapshot.allManifests(table.io())).hasSize(1);

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newDelete().deleteFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Set<ManifestFile> secondSnapshotManifests =
        Sets.newHashSet(secondSnapshot.allManifests(table.io()));
    secondSnapshotManifests.removeAll(firstSnapshot.allManifests(table.io()));
    assertThat(secondSnapshotManifests).hasSize(1);

    table.manageSnapshots().rollbackTo(firstSnapshot.snapshotId()).commit();

    long tAfterCommits = waitUntilAfter(secondSnapshot.timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(table.snapshot(firstSnapshot.snapshotId()))
        .as("Expire should keep the oldest snapshot, current")
        .isNotNull();
    assertThat(table.snapshot(secondSnapshot.snapshotId()))
        .as("Expire should remove the orphaned snapshot")
        .isNull();

    assertThat(deletedFiles)
        .as("Should remove expired manifest lists and reverted appended data file")
        .isEqualTo(
            Sets.newHashSet(
                secondSnapshot.manifestListLocation(), // snapshot expired
                Iterables.getOnlyElement(secondSnapshotManifests)
                    .path()) // manifest is no longer referenced
            );
  }

  @TestTemplate
  public void testExpireOlderThanWithRollbackAndMergedManifests() {
    table.newAppend().appendFile(FILE_A).commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    assertThat(firstSnapshot.allManifests(table.io())).hasSize(1);

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    table.newAppend().appendFile(FILE_B).commit();

    Snapshot secondSnapshot = table.currentSnapshot();
    Set<ManifestFile> secondSnapshotManifests =
        Sets.newHashSet(secondSnapshot.allManifests(table.io()));
    secondSnapshotManifests.removeAll(firstSnapshot.allManifests(table.io()));
    assertThat(secondSnapshotManifests).hasSize(1);

    table.manageSnapshots().rollbackTo(firstSnapshot.snapshotId()).commit();

    long tAfterCommits = waitUntilAfter(secondSnapshot.timestampMillis());

    long snapshotId = table.currentSnapshot().snapshotId();

    Set<String> deletedFiles = Sets.newHashSet();

    removeSnapshots(table).expireOlderThan(tAfterCommits).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(snapshotId);
    assertThat(table.snapshot(firstSnapshot.snapshotId()))
        .as("Expire should keep the oldest snapshot, current")
        .isNotNull();
    assertThat(table.snapshot(secondSnapshot.snapshotId()))
        .as("Expire should remove the orphaned snapshot")
        .isNull();

    assertThat(deletedFiles)
        .as("Should remove expired manifest lists and reverted appended data file")
        .isEqualTo(
            Sets.newHashSet(
                secondSnapshot.manifestListLocation(), // snapshot expired
                Iterables.getOnlyElement(secondSnapshotManifests)
                    .path(), // manifest is no longer referenced
                FILE_B.path()) // added, but rolled back
            );
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.snapshot(firstSnapshotId)).isNull();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.snapshot(firstSnapshotId)).isNull();
  }

  @TestTemplate
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

    assertThat(listManifestFiles(new File(table.location()))).hasSize(3);

    // Retain last 2 snapshots, which means 1 is deleted.
    Transaction tx = table.newTransaction();
    removeSnapshots(tx.table()).expireOlderThan(t3).retainLast(2).commit();
    tx.commitTransaction();

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.snapshot(firstSnapshotId)).isNull();
    assertThat(listManifestLists(new File(table.location()))).hasSize(2);
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.snapshot(firstSnapshotId).snapshotId()).isEqualTo(firstSnapshotId);
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(3);
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(3);
    assertThat(table.snapshot(secondSnapshot.snapshotId())).isNotNull();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.snapshot(secondSnapshot.snapshotId())).isNull();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.snapshot(secondSnapshot.snapshotId())).isNull();
  }

  @TestTemplate
  public void testRetainZeroSnapshots() {
    assertThatThrownBy(() -> removeSnapshots(table).retainLast(0).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Number of snapshots to retain must be at least 1, cannot be: 0");
  }

  @TestTemplate
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

    assertThat(deletedFiles).contains(FILE_A.path().toString());
  }

  @TestTemplate
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

    assertThat(deletedFiles).contains(FILE_A.path().toString());
  }

  @TestTemplate
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

    assertThat(deletedFiles).contains(FILE_A.path().toString());
    assertThat(deletedFiles).contains(FILE_B.path().toString());
  }

  @TestTemplate
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

    Set<String> deletedFiles = ConcurrentHashMap.newKeySet();
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
    assertThat(deleteThreads)
        .containsExactly(
            "remove-snapshot-3", "remove-snapshot-2", "remove-snapshot-1", "remove-snapshot-0");

    assertThat(deletedFiles).contains(FILE_A.path().toString());
    assertThat(deletedFiles).contains(FILE_B.path().toString());
    assertThat(planThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
  }

  @TestTemplate
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

    assertThat(deletedFiles).isEmpty();
  }

  /**
   * Test on table below, and expiring the staged commit `B` using `expireOlderThan` API. Table: A -
   * C ` B (staged)
   */
  @TestTemplate
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
    assertThat(deletedFiles).isEqualTo(expectedDeletes);
    // Take the diff
    expectedDeletes.removeAll(deletedFiles);
    assertThat(expectedDeletes).isEmpty();
  }

  /**
   * Expire cherry-pick the commit as shown below, when `B` is in table's current state Table: A - B
   * - C <--current snapshot `- D (source=B)
   */
  @TestTemplate
  public void testWithCherryPickTableSnapshot() {
    // `A` commit
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotA = table.currentSnapshot();

    // `B` commit
    Set<String> deletedAFiles = Sets.newHashSet();
    table.newOverwrite().addFile(FILE_B).deleteFile(FILE_A).deleteWith(deletedAFiles::add).commit();
    assertThat(deletedAFiles).isEmpty();

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
                        assertThat(deletedFiles).doesNotContain(item.path().toString());
                      });
            });
  }

  /**
   * Test on table below, and expiring `B` which is not in current table state. 1) Expire `B` 2) All
   * commit Table: A - C - D (B) ` B (staged)
   */
  @TestTemplate
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
                        assertThat(deletedFiles).doesNotContain(item.path().toString());
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
                        assertThat(deletedFiles).doesNotContain(item.path().toString());
                      });
            });
  }

  @TestTemplate
  public void testExpireSnapshotsWhenGarbageCollectionDisabled() {
    table.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();

    table.newAppend().appendFile(FILE_A).commit();

    assertThatThrownBy(() -> table.expireSnapshots())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot expire snapshots: GC is disabled");
  }

  @TestTemplate
  public void testExpireWithDefaultRetainLast() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).commit();

    table.newAppend().appendFile(FILE_C).commit();

    assertThat(table.snapshots()).hasSize(3);

    table.updateProperties().set(TableProperties.MIN_SNAPSHOTS_TO_KEEP, "3").commit();

    Set<String> deletedFiles = Sets.newHashSet();

    Snapshot snapshotBeforeExpiration = table.currentSnapshot();

    removeSnapshots(table)
        .expireOlderThan(System.currentTimeMillis())
        .deleteWith(deletedFiles::add)
        .commit();

    assertThat(table.currentSnapshot()).isEqualTo(snapshotBeforeExpiration);
    assertThat(table.snapshots()).hasSize(3);
    assertThat(deletedFiles).isEmpty();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(3);

    table.updateProperties().set(TableProperties.MAX_SNAPSHOT_AGE_MS, "1").commit();

    Set<String> deletedFiles = Sets.newHashSet();

    // rely solely on default configs
    removeSnapshots(table).deleteWith(deletedFiles::add).commit();

    assertThat(table.currentSnapshot()).isEqualTo(thirdSnapshot);
    assertThat(table.snapshots()).hasSize(1);
    assertThat(deletedFiles)
        .isEqualTo(
            Sets.newHashSet(
                firstSnapshot.manifestListLocation(), secondSnapshot.manifestListLocation()));
  }

  @TestTemplate
  public void testExpireWithDeleteFiles() {
    assumeThat(formatVersion).as("Delete files only supported in V2 spec").isEqualTo(2);

    // Data Manifest => File_A
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot firstSnapshot = table.currentSnapshot();

    // Data Manifest => FILE_A
    // Delete Manifest => FILE_A_DELETES
    table.newRowDelta().addDeletes(FILE_A_DELETES).commit();
    Snapshot secondSnapshot = table.currentSnapshot();
    assertThat(secondSnapshot.dataManifests(table.io())).hasSize(1);
    assertThat(secondSnapshot.deleteManifests(table.io())).hasSize(1);

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
    assertThat(manifestOfDeletedFiles).hasSize(2);

    // Need one more commit before manifests of files of DELETED state get cleared from current
    // snapshot.
    table.newAppend().appendFile(FILE_C).commit();

    Snapshot fourthSnapshot = table.currentSnapshot();
    long fourthSnapshotTs = waitUntilAfter(fourthSnapshot.timestampMillis());

    Set<String> deletedFiles = Sets.newHashSet();
    removeSnapshots(table).expireOlderThan(fourthSnapshotTs).deleteWith(deletedFiles::add).commit();

    assertThat(deletedFiles)
        .as("Should remove old delete files and delete file manifests")
        .isEqualTo(
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
                .build());
  }

  @TestTemplate
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

    assertThat(table.ops().current().ref("tag")).isNull();
    assertThat(table.ops().current().ref("branch")).isNotNull();
    assertThat(table.ops().current().ref(SnapshotRef.MAIN_BRANCH)).isNotNull();
  }

  @TestTemplate
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

    assertThat(table.ops().current().ref("branch")).isNull();
    assertThat(table.ops().current().ref("tag")).isNotNull();
    assertThat(table.ops().current().ref(SnapshotRef.MAIN_BRANCH)).isNotNull();
  }

  @TestTemplate
  public void testMultipleRefsAndCleanExpiredFilesFailsForIncrementalCleanup() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newDelete().deleteFile(FILE_A).commit();
    table.manageSnapshots().createTag("TagA", table.currentSnapshot().snapshotId()).commit();
    waitUntilAfter(table.currentSnapshot().timestampMillis());
    RemoveSnapshots removeSnapshots = (RemoveSnapshots) table.expireSnapshots();

    assertThatThrownBy(
            () ->
                removeSnapshots
                    .withIncrementalCleanup(true)
                    .expireOlderThan(table.currentSnapshot().timestampMillis())
                    .cleanExpiredFiles(true)
                    .commit())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot incrementally clean files for tables with more than 1 ref");
  }

  @TestTemplate
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
    assertThat(table.statisticsFiles()).hasSize(2);

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file should be retained
    assumeThat(table.snapshots()).hasSize(1);
    assertThat(table.statisticsFiles())
        .hasSize(1)
        .extracting(StatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));

    assertThat(new File(statsFileLocation1)).doesNotExist();
    assertThat(new File(statsFileLocation2)).exists();
  }

  @TestTemplate
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

    assertThat(table.statisticsFiles()).hasSize(2);

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file (reused from previous snapshot) should be
    // retained
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.statisticsFiles())
        .hasSize(1)
        .extracting(StatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));
    // the reused stats file should exist.
    assertThat(new File(statsFileLocation1)).exists();
  }

  @TestTemplate
  public void testExpireWithPartitionStatisticsFiles() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    String statsFileLocation1 = statsFileLocation(table.location());
    PartitionStatisticsFile statisticsFile1 =
        writePartitionStatsFile(
            table.currentSnapshot().snapshotId(), statsFileLocation1, table.io());
    commitPartitionStats(table, statisticsFile1);

    table.newAppend().appendFile(FILE_B).commit();
    String statsFileLocation2 = statsFileLocation(table.location());
    PartitionStatisticsFile statisticsFile2 =
        writePartitionStatsFile(
            table.currentSnapshot().snapshotId(), statsFileLocation2, table.io());
    commitPartitionStats(table, statisticsFile2);
    assertThat(table.partitionStatisticsFiles()).hasSize(2);

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file should be retained
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.partitionStatisticsFiles())
        .hasSize(1)
        .extracting(PartitionStatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));

    assertThat(new File(statsFileLocation1)).doesNotExist();
    assertThat(new File(statsFileLocation2)).exists();
  }

  @TestTemplate
  public void testExpireWithPartitionStatisticsFilesWithReuse() throws IOException {
    table.newAppend().appendFile(FILE_A).commit();
    String statsFileLocation1 = statsFileLocation(table.location());
    PartitionStatisticsFile statisticsFile1 =
        writePartitionStatsFile(
            table.currentSnapshot().snapshotId(), statsFileLocation1, table.io());
    commitPartitionStats(table, statisticsFile1);

    table.newAppend().appendFile(FILE_B).commit();
    // If an expired snapshot's stats file is reused for some reason by the live snapshots,
    // that stats file should not get deleted from the file system as the live snapshots still
    // reference it.
    PartitionStatisticsFile statisticsFile2 =
        reusePartitionStatsFile(table.currentSnapshot().snapshotId(), statisticsFile1);
    commitPartitionStats(table, statisticsFile2);

    assumeThat(table.partitionStatisticsFiles()).hasSize(2);

    long tAfterCommits = waitUntilAfter(table.currentSnapshot().timestampMillis());
    removeSnapshots(table).expireOlderThan(tAfterCommits).commit();

    // only the current snapshot and its stats file (reused from previous snapshot) should be
    // retained
    assertThat(table.snapshots()).hasSize(1);
    assertThat(table.partitionStatisticsFiles())
        .hasSize(1)
        .extracting(PartitionStatisticsFile::snapshotId)
        .as("Should contain only the statistics file of snapshot2")
        .isEqualTo(Lists.newArrayList(statisticsFile2.snapshotId()));
    // the reused stats file should exist.
    assertThat(new File(statsFileLocation1)).exists();
  }

  @TestTemplate
  public void testFailRemovingSnapshotWhenStillReferencedByBranch() {
    table.newAppend().appendFile(FILE_A).commit();

    AppendFiles append = table.newAppend().appendFile(FILE_B).stageOnly();

    long snapshotId = append.apply().snapshotId();

    append.commit();

    table.manageSnapshots().createBranch("branch", snapshotId).commit();

    assertThatThrownBy(() -> removeSnapshots(table).expireSnapshotId(snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot expire 2. Still referenced by refs: [branch]");
  }

  @TestTemplate
  public void testFailRemovingSnapshotWhenStillReferencedByTag() {
    table.newAppend().appendFile(FILE_A).commit();

    long snapshotId = table.currentSnapshot().snapshotId();

    table.manageSnapshots().createTag("tag", snapshotId).commit();

    // commit another snapshot so the first one isn't referenced by main
    table.newAppend().appendFile(FILE_B).commit();

    assertThatThrownBy(() -> removeSnapshots(table).expireSnapshotId(snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot expire 1. Still referenced by refs: [tag]");
  }

  @TestTemplate
  public void testRetainUnreferencedSnapshotsWithinExpirationAge() {
    table.newAppend().appendFile(FILE_A).commit();

    long expireTimestampSnapshotA = waitUntilAfter(table.currentSnapshot().timestampMillis());
    waitUntilAfter(expireTimestampSnapshotA);

    table.newAppend().appendFile(FILE_B).stageOnly().commit();

    table.newAppend().appendFile(FILE_C).commit();

    removeSnapshots(table).expireOlderThan(expireTimestampSnapshotA).commit();

    assertThat(table.ops().current().snapshots()).hasSize(2);
  }

  @TestTemplate
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

    assertThat(table.snapshot(expiredSnapshotId))
        .as("Should remove unreferenced snapshot beneath a tag")
        .isNull();
    assertThat(table.ops().current().snapshots()).hasSize(2);
  }

  @TestTemplate
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

    assertThat(table.snapshot(snapshotId))
        .as("Should not remove snapshot beneath a branch")
        .isNotNull();
    assertThat(table.ops().current().snapshots()).hasSize(3);
  }

  @TestTemplate
  public void testMinSnapshotsToKeepMultipleBranches() {
    table.newAppend().appendFile(FILE_A).commit();
    long initialSnapshotId = table.currentSnapshot().snapshotId();
    table.newAppend().appendFile(FILE_B).commit();

    // stage a snapshot and get its id
    AppendFiles append = table.newAppend().appendFile(FILE_C).stageOnly();
    long branchSnapshotId = append.apply().snapshotId();
    append.commit();

    assertThat(table.snapshots()).hasSize(3);

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

    assertThat(table.snapshots()).hasSize(3);

    // stop retaining snapshots from the branch
    table.manageSnapshots().setMinSnapshotsToKeep("branch", 1).commit();

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.ops().current().snapshot(initialSnapshotId)).isNull();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(3);

    // retain all snapshots on branch (including the initial snapshot)
    table
        .manageSnapshots()
        .createBranch("branch", branchSnapshotId)
        .setMinSnapshotsToKeep("branch", 1)
        .setMaxSnapshotAgeMs("branch", Long.MAX_VALUE)
        .commit();

    removeSnapshots(table).cleanExpiredFiles(false).commit();

    assertThat(table.snapshots()).hasSize(3);

    // allow the initial snapshot to age off from branch
    table.manageSnapshots().setMaxSnapshotAgeMs("branch", ageMs).commit();

    table.expireSnapshots().cleanExpiredFiles(false).commit();

    assertThat(table.snapshots()).hasSize(2);
    assertThat(table.ops().current().snapshot(initialSnapshotId)).isNull();
  }

  @TestTemplate
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

    assertThat(table.snapshots()).hasSize(2);
    assertThat(deletedFiles).isEqualTo(expectedDeletes);

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

    assertThat(table.snapshots()).hasSize(2);
    assertThat(deletedFiles).isEqualTo(expectedDeletes);
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

  private static PartitionStatisticsFile writePartitionStatsFile(
      long snapshotId, String statsLocation, FileIO fileIO) {
    PositionOutputStream positionOutputStream;
    try {
      positionOutputStream = fileIO.newOutputFile(statsLocation).create();
      positionOutputStream.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .fileSizeInBytes(42L)
        .path(statsLocation)
        .build();
  }

  private static PartitionStatisticsFile reusePartitionStatsFile(
      long snapshotId, PartitionStatisticsFile statisticsFile) {
    return ImmutableGenericPartitionStatisticsFile.builder()
        .path(statisticsFile.path())
        .fileSizeInBytes(statisticsFile.fileSizeInBytes())
        .snapshotId(snapshotId)
        .build();
  }

  private static void commitPartitionStats(Table table, PartitionStatisticsFile statisticsFile) {
    table.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();
  }
}
