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

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.iceberg.exceptions.NotFoundException;
import org.junit.Assert;
import org.junit.Test;

public class TestRemoveSnapshots extends TableTestBase {

  @Test
  public void testRetainLastWithExpireOlderThan() {
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
  public void testExpireMetadata() {
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

    List<TableMetadata.MetadataLogEntry> previousFiles = Lists.newArrayList();
    previousFiles.addAll(table.ops().current().previousFiles());

    // Retain last 2 metadata files
    table.expireSnapshots()
        .expireMetadataRetainLast(2)
        .commit();

    AssertHelpers.assertThrows("Should fail read v1.metadata.json metadata file, as file is deleted",
        NotFoundException.class,
        "Failed to read file: " + previousFiles.get(1).file(),
        () -> TableMetadataParser.read(table.ops().io(), table.ops().io().newInputFile(previousFiles.get(1).file())));

    TableMetadata latestPreviousMetadata =
        TableMetadataParser.read(table.ops().io(), table.ops().io().newInputFile(previousFiles.get(2).file()));
    Assert.assertNotNull(latestPreviousMetadata);
  }

  @Test
  public void testExpireSnapshotsAndMetadata() {
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

    List<TableMetadata.MetadataLogEntry> previousFiles = Lists.newArrayList();
    previousFiles.addAll(table.ops().current().previousFiles());

    // Expire snapshots older than t3 and retain last 1 metadata file
    table.expireSnapshots()
        .expireOlderThan(t3)
        .expireMetadataRetainLast(1)
        .commit();

    Assert.assertEquals("Should have one snapshots.",
        1, Lists.newArrayList(table.snapshots()).size());

    AssertHelpers.assertThrows("Should fail read v2.metadata.json metadata file, as file is deleted",
        NotFoundException.class,
        "Failed to read file: " + previousFiles.get(2).file(),
        () -> TableMetadataParser.read(table.ops().io(), table.ops().io().newInputFile(previousFiles.get(2).file())));
  }

  @Test
  public void testExpireMetadataRetainMoreThanAvailable() {
    table.newAppend()
        .appendFile(FILE_A) // data_bucket=0
        .commit();

    long t0 = System.currentTimeMillis();
    while (t0 <= table.currentSnapshot().timestampMillis()) {
      t0 = System.currentTimeMillis();
    }

    table.updateProperties()
        .set(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "20")
        .set(TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, "true")
        .commit();

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

    // Retain at least last 10 metadata files
    table.expireSnapshots()
        .expireMetadataRetainLast(10)
        .commit();

    List<TableMetadata.MetadataLogEntry> previousMetadata = Lists.newArrayList();
    previousMetadata.addAll(table.ops().current().previousFiles());

    Assert.assertEquals("Should have 4 previous metadata files.",
        4, previousMetadata.size());

    TableMetadata oldestPreviousMetadata =
        TableMetadataParser.read(table.ops().io(), table.ops().io().newInputFile(previousMetadata.get(0).file()));
    Assert.assertNotNull(oldestPreviousMetadata);

    TableMetadata latestPreviousMetadata =
        TableMetadataParser.read(table.ops().io(), table.ops().io().newInputFile(previousMetadata.get(3).file()));
    Assert.assertNotNull(latestPreviousMetadata);
  }
}
