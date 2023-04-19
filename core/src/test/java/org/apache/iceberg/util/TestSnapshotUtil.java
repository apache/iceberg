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
package org.apache.iceberg.util;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSnapshotUtil {
  @Rule public TemporaryFolder temp = new TemporaryFolder();

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          required(3, "id", Types.IntegerType.get()), required(4, "data", Types.StringType.get()));

  // Partition spec used to create tables
  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  protected File tableDir = null;
  protected File metadataDir = null;
  public TestTables.TestTable table = null;

  static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  private long snapshotAId;

  private long snapshotATimestamp;
  private long snapshotBId;
  private long snapshotCId;
  private long snapshotDId;

  @Before
  public void before() throws Exception {
    this.tableDir = temp.newFolder();
    tableDir.delete(); // created by table create

    this.metadataDir = new File(tableDir, "metadata");

    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);
    table.newFastAppend().appendFile(FILE_A).commit();
    Snapshot snapshotA = table.currentSnapshot();
    this.snapshotAId = snapshotA.snapshotId();
    this.snapshotATimestamp = snapshotA.timestampMillis();

    TestHelpers.waitUntilAfter(snapshotATimestamp);

    table.newFastAppend().appendFile(FILE_A).commit();
    this.snapshotBId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_A).commit();
    this.snapshotDId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    table.newFastAppend().appendFile(FILE_A).toBranch(branchName).commit();
    this.snapshotCId = table.snapshot(branchName).snapshotId();
  }

  @After
  public void cleanupTables() {
    TestTables.clearTables();
  }

  @Test
  public void isParentAncestorOf() {
    Assert.assertTrue(SnapshotUtil.isParentAncestorOf(table, snapshotBId, snapshotAId));
    Assert.assertFalse(SnapshotUtil.isParentAncestorOf(table, snapshotCId, snapshotBId));
  }

  @Test
  public void isAncestorOf() {
    Assert.assertTrue(SnapshotUtil.isAncestorOf(table, snapshotBId, snapshotAId));
    Assert.assertFalse(SnapshotUtil.isAncestorOf(table, snapshotCId, snapshotBId));

    Assert.assertTrue(SnapshotUtil.isAncestorOf(table, snapshotBId));
    Assert.assertFalse(SnapshotUtil.isAncestorOf(table, snapshotCId));
  }

  @Test
  public void currentAncestors() {
    Iterable<Snapshot> snapshots = SnapshotUtil.currentAncestors(table);
    expectedSnapshots(new long[] {snapshotDId, snapshotBId, snapshotAId}, snapshots);

    List<Long> snapshotList = SnapshotUtil.currentAncestorIds(table);
    Assert.assertArrayEquals(
        new Long[] {snapshotDId, snapshotBId, snapshotAId}, snapshotList.toArray(new Long[0]));
  }

  @Test
  public void oldestAncestor() {
    Snapshot snapshot = SnapshotUtil.oldestAncestor(table);
    Assert.assertEquals(snapshotAId, snapshot.snapshotId());

    snapshot = SnapshotUtil.oldestAncestorOf(table, snapshotDId);
    Assert.assertEquals(snapshotAId, snapshot.snapshotId());

    snapshot = SnapshotUtil.oldestAncestorAfter(table, snapshotATimestamp + 1);
    Assert.assertEquals(snapshotBId, snapshot.snapshotId());
  }

  @Test
  public void snapshotsBetween() {
    List<Long> snapshotIdsBetween =
        SnapshotUtil.snapshotIdsBetween(table, snapshotAId, snapshotDId);
    Assert.assertArrayEquals(
        new Long[] {snapshotDId, snapshotBId}, snapshotIdsBetween.toArray(new Long[0]));

    Iterable<Snapshot> ancestorsBetween =
        SnapshotUtil.ancestorsBetween(table, snapshotDId, snapshotBId);
    expectedSnapshots(new long[] {snapshotDId}, ancestorsBetween);

    ancestorsBetween = SnapshotUtil.ancestorsBetween(table, snapshotDId, snapshotCId);
    expectedSnapshots(new long[] {snapshotDId, snapshotBId, snapshotAId}, ancestorsBetween);
  }

  private void expectedSnapshots(long[] snapshotIdExpected, Iterable<Snapshot> snapshotsActual) {
    long[] actualSnapshots =
        StreamSupport.stream(snapshotsActual.spliterator(), false)
            .mapToLong(Snapshot::snapshotId)
            .toArray();
    Assert.assertArrayEquals(snapshotIdExpected, actualSnapshots);
  }
}
