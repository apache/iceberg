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


import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

public class TestBaseIncrementalAppendScan extends ScanTestBase<IncrementalAppendScan> {
  public TestBaseIncrementalAppendScan(int formatVersion) {
    super(formatVersion);
  }

  @Override
  protected IncrementalAppendScan newScan() {
    return table.newIncrementalAppendScan();
  }

  @Test
  public void testFromSnapshotInclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan()
        .fromSnapshotInclusive(snapshotAId);
    Assert.assertEquals(3, Iterables.size(scan.planFiles()));
  }

  @Test
  public void testFromSnapshotExclusiveAndToSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan()
        .fromSnapshotExclusive(snapshotAId)
        .toSnapshot(snapshotBId);
    Assert.assertEquals(1, Iterables.size(scan.planFiles()));
  }

  @Test
  public void testToSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan()
        .toSnapshot(snapshotBId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));
  }

  @Test
  public void testMultipleRootSnapshots() throws Exception {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    long expireTimestampSnapshotA = TestHelpers.waitUntilAfter(table.currentSnapshot().timestampMillis());

    // append file B in a staged branch
    table.newFastAppend().appendFile(FILE_B).stageOnly().commit();
    List<Snapshot> snapshotsAfterB = Lists.newArrayList(table.snapshots().iterator());
    Assert.assertEquals(2, snapshotsAfterB.size());
    long snapshotBId = snapshotsAfterB.stream()
        .filter(snapshot -> snapshot.snapshotId() != snapshotAId)
        .collect(Collectors.toList())
        .get(0)
        .snapshotId();

    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_D).commit();
    long snapshotDId = table.currentSnapshot().snapshotId();

    // expiring snapshotA should create two root snapshots (B and C)
    table.expireSnapshots().expireOlderThan(expireTimestampSnapshotA).commit();

    // scan should discover snapshot C and D
    IncrementalAppendScan scan = newScan()
        .toSnapshot(snapshotDId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));

    // scan should fail because snapshot B is not an ancestor of snapshot D
    IncrementalAppendScan scanShouldFail = newScan()
        .fromSnapshotExclusive(snapshotBId)
        .toSnapshot(snapshotDId);
    AssertHelpers.assertThrows("Should throw exception",
        IllegalArgumentException.class,
        String.format("Starting snapshot (exclusive) %%d is not an ancestor of end snapshot %%d [%d, %d]",
            snapshotBId, snapshotDId),
        () -> Iterables.size(scanShouldFail.planFiles()));
  }
}
