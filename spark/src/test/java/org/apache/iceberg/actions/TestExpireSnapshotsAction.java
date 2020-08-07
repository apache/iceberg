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

package org.apache.iceberg.actions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestExpireSnapshotsAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  private static final List<ThreeColumnRecord> RECORDS = Lists.newArrayList(new ThreeColumnRecord(1, "AAAA", "AAAA"));

  static final DataFile FILE_A = DataFiles.builder(SPEC)
      .withPath("/path/to/data-a.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=0") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_B = DataFiles.builder(SPEC)
      .withPath("/path/to/data-b.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=1") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_C = DataFiles.builder(SPEC)
      .withPath("/path/to/data-c.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=2") // easy way to set partition data for now
      .withRecordCount(1)
      .build();
  static final DataFile FILE_D = DataFiles.builder(SPEC)
      .withPath("/path/to/data-d.parquet")
      .withFileSizeInBytes(10)
      .withPartitionPath("c1=3") // easy way to set partition data for now
      .withRecordCount(1)
      .build();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableDir;
  private String tableLocation;
  private Table table;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);
  }

  private Dataset<Row> buildDF(List<ThreeColumnRecord> records) {
    return spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
  }

  private void writeDF(Dataset<Row> df, String mode) {
    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode(mode)
        .save(tableLocation);
  }

  private void checkExpirationResults(Long expectedDatafiles, Long expectedManifestsDeleted,
      Long expectedManifestListsDeleted, ExpireSnapshotsActionResult results) {

    Assert.assertEquals("Incorrect number of manifest files deleted",
        expectedManifestsDeleted, results.getManifestFilesDeleted());
    Assert.assertEquals("Incorrect number of datafiles deleted",
        expectedDatafiles, results.getDataFilesDeleted());
    Assert.assertEquals("Incorrect number of manifest lists deleted",
        expectedManifestListsDeleted, results.getManifestListsDeleted());
  }


  @Test
  public void testFilesCleaned() throws Exception {
    Dataset<Row> df = buildDF(RECORDS);

    writeDF(df, "append");

    List<Path> expiredDataFiles = Files
        .list(tableDir.toPath().resolve("data").resolve("c1=1"))
        .collect(Collectors.toList());

    Assert.assertEquals("There should be a data file to delete but there was none.",
        2, expiredDataFiles.size());

    writeDF(df, "overwrite");
    writeDF(df, "append");

    long end = System.currentTimeMillis();
    while (end <= table.currentSnapshot().timestampMillis()) {
      end = System.currentTimeMillis();
    }

    ExpireSnapshotsActionResult results =
        Actions.forTable(table).expireSnapshots().expireOlderThan(end).execute();

    table.refresh();

    Assert.assertEquals("Table does not have 1 snapshot after expiration", 1, Iterables.size(table.snapshots()));

    for (Path p : expiredDataFiles) {
      Assert.assertFalse(String.format("File %s still exists but should have been deleted", p),
          Files.exists(p));
    }

    checkExpirationResults(1L, 2L, 2L, results);
  }

  @Test
  public void dataFilesCleanupWithParallelTasks() throws IOException {

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

    Set<String> deletedFiles = Sets.newHashSet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);

    Actions.forTable(table).expireSnapshots()
        .executeDeleteWith(Executors.newFixedThreadPool(4, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("remove-snapshot-" + deleteThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }))
        .expireOlderThan(t4)
        .deleteWith(s -> {
          deleteThreads.add(Thread.currentThread().getName());
          deletedFiles.add(s);
        })
        .execute();

    // Verifies that the delete methods ran in the threads created by the provided ExecutorService ThreadFactory
    Assert.assertEquals(deleteThreads,
        Sets.newHashSet("remove-snapshot-0", "remove-snapshot-1", "remove-snapshot-2", "remove-snapshot-3"));

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
    Assert.assertTrue("FILE_B should be deleted", deletedFiles.contains(FILE_B.path().toString()));
  }

  @Test
  public void testNoFilesDeletedWhenNoSnapshotsExpired() throws Exception {
    Dataset<Row> df = buildDF(RECORDS);

    writeDF(df, "append");

    ExpireSnapshotsActionResult results =
        Actions.forTable(table).expireSnapshots().execute();

    checkExpirationResults(0L, 0L, 0L, results);
  }

  @Test
  public void testCleanupRepeatedOverwrites() throws Exception {
    Dataset<Row> df = buildDF(RECORDS);

    writeDF(df, "append");

    for (int i = 0; i < 10; i++) {
      writeDF(df, "overwrite");
    }

    long end = System.currentTimeMillis();
    while (end <= table.currentSnapshot().timestampMillis()) {
      end = System.currentTimeMillis();
    }

    ExpireSnapshotsActionResult results =
        Actions.forTable(table).expireSnapshots().expireOlderThan(end).execute();

    checkExpirationResults(10L, 19L, 10L, results);
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
    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(t3)
        .retainLast(2)
        .execute();

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
    Actions.forTable(table).expireSnapshots()
        .expireSnapshotId(firstSnapshotId)
        .retainLast(3)
        .execute();

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
    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(t2)
        .retainLast(3)
        .execute();

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
    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(secondSnapshot.timestampMillis())
        .retainLast(2)
        .execute();

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
    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(secondSnapshot.timestampMillis())
        .expireOlderThan(thirdSnapshot.timestampMillis())
        .execute();

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
    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(t3)
        .retainLast(2)
        .retainLast(1)
        .execute();

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
        () -> Actions.forTable(table).expireSnapshots().retainLast(0).execute());
  }

  @Test
  public void testScanExpiredManifestInValidSnapshotAppend() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.newOverwrite()
        .addFile(FILE_C)
        .deleteFile(FILE_A)
        .commit();

    table.newAppend()
        .appendFile(FILE_D)
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    Set<String> deletedFiles = Sets.newHashSet();

    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(t3)
        .deleteWith(deletedFiles::add)
        .execute();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));

  }

  @Test
  public void testScanExpiredManifestInValidSnapshotFastAppend() {
    table.updateProperties()
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1")
        .commit();

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.newOverwrite()
        .addFile(FILE_C)
        .deleteFile(FILE_A)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_D)
        .commit();

    long t3 = System.currentTimeMillis();
    while (t3 <= table.currentSnapshot().timestampMillis()) {
      t3 = System.currentTimeMillis();
    }

    Set<String> deletedFiles = Sets.newHashSet();

    Actions.forTable(table).expireSnapshots()
        .expireOlderThan(t3)
        .deleteWith(deletedFiles::add)
        .execute();

    Assert.assertTrue("FILE_A should be deleted", deletedFiles.contains(FILE_A.path().toString()));
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

    TableMetadata base = ((BaseTable) table).operations().current();
    Snapshot snapshotA = base.snapshots().get(0);
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    Set<String> deletedFiles = new HashSet<>();

    // Expire all commits including dangling staged snapshot.
    Actions.forTable(table).expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotB.timestampMillis() + 1)
        .execute();

    Set<String> expectedDeletes = new HashSet<>();
    expectedDeletes.add(snapshotA.manifestListLocation());

    // Files should be deleted of dangling staged snapshot
    snapshotB.addedFiles().forEach(i -> {
      expectedDeletes.add(i.path().toString());
    });

    // ManifestList should be deleted too
    expectedDeletes.add(snapshotB.manifestListLocation());
    snapshotB.dataManifests().forEach(file -> {
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
    Snapshot snapshotB = table.currentSnapshot();

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    Snapshot snapshotC = table.currentSnapshot();

    // Move the table back to `A`
    table.manageSnapshots()
        .setCurrentSnapshot(snapshotA.snapshotId())
        .commit();

    // Generate A -> `D (B)`
    table.manageSnapshots()
        .cherrypick(snapshotB.snapshotId())
        .commit();
    Snapshot snapshotD = table.currentSnapshot();

    // Move the table back to `C`
    table.manageSnapshots()
        .setCurrentSnapshot(snapshotC.snapshotId())
        .commit();
    List<String> deletedFiles = new ArrayList<>();

    // Expire `C`
    Actions.forTable(table).expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(snapshotC.timestampMillis() + 1)
        .execute();

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
    TableMetadata base = ((BaseTable) table).operations().current();
    Snapshot snapshotB = base.snapshots().get(1);

    // `C` commit to let cherry-pick take effect, and avoid fast-forward of `B` with cherry-pick
    table.newAppend()
        .appendFile(FILE_C)
        .commit();

    // `D (B)` cherry-pick commit
    table.manageSnapshots()
        .cherrypick(snapshotB.snapshotId())
        .commit();

    base = ((BaseTable) table).operations().current();
    Snapshot snapshotD = base.snapshots().get(3);

    List<String> deletedFiles = new ArrayList<>();

    // Expire `B` commit.
    Actions.forTable(table).expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireSnapshotId(snapshotB.snapshotId())
        .execute();

    // Make sure no dataFiles are deleted for the staged snapshot
    Lists.newArrayList(snapshotB).forEach(i -> {
      i.addedFiles().forEach(item -> {
        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
      });
    });

    // Expire all snapshots including cherry-pick
    Actions.forTable(table).expireSnapshots()
        .deleteWith(deletedFiles::add)
        .expireOlderThan(table.currentSnapshot().timestampMillis() + 1)
        .execute();

    // Make sure no dataFiles are deleted for the staged and cherry-pick
    Lists.newArrayList(snapshotB, snapshotD).forEach(i -> {
      i.addedFiles().forEach(item -> {
        Assert.assertFalse(deletedFiles.contains(item.path().toString()));
      });
    });
  }

}

