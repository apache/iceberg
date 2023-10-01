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

import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestBaseIncrementalAppendScan
    extends ScanTestBase<IncrementalAppendScan, FileScanTask, CombinedScanTask> {
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

    IncrementalAppendScan scan = newScan().fromSnapshotInclusive(snapshotAId);
    Assert.assertEquals(3, Iterables.size(scan.planFiles()));

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(snapshotCId);
    Assert.assertEquals(3, Iterables.size(scanWithToSnapshot.planFiles()));
  }

  @Test
  public void fromSnapshotInclusiveWithNonExistingRef() {
    Assertions.assertThatThrownBy(() -> newScan().fromSnapshotInclusive("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @Test
  public void fromSnapshotInclusiveWithTag() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagSnapshotBName = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagSnapshotBName, snapshotBId).commit();
    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_C).commit();

    /*
              files:FILE_A         files:FILE_B FILE_B       files:FILE_C FILE_C
     ---- snapshotAId(tag:t1) ---- snapshotMainB(tag:t2) ----  currentSnapshot
    */
    IncrementalAppendScan scan = newScan().fromSnapshotInclusive(tagSnapshotAName);
    Assertions.assertThat(scan.planFiles()).hasSize(5);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotInclusive(tagSnapshotAName).toSnapshot(tagSnapshotBName);
    Assertions.assertThat(scanWithToSnapshot.planFiles()).hasSize(3);
  }

  @Test
  public void fromSnapshotInclusiveWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    Assertions.assertThatThrownBy(() -> newScan().fromSnapshotInclusive(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));

    Assertions.assertThatThrownBy(
            () -> newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @Test
  public void testUseBranch() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    String tagSnapshotAName = "t1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagName2 = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();
    long snapshotMainBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagName2, snapshotMainBId).commit();

    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();

    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchBId = table.snapshot(branchName).snapshotId();

    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchCId = table.snapshot(branchName).snapshotId();

    /*

            files:FILE_A         files:FILE_B FILE_B       files:FILE_B FILE_B
     ---- snapshotA(tag:t1) ---- snapshotMainB(tag:t2) ----  currentSnapshot
                        \
                         \
                          \files:FILE_C
                          snapshotBranchB
                            \
                             \
                              \files:FILE_C
                          snapshotBranchC(branch:b1)
    */
    IncrementalAppendScan scan = newScan().fromSnapshotInclusive(tagSnapshotAName);
    Assertions.assertThat(scan.planFiles()).hasSize(5);

    IncrementalAppendScan scan2 =
        newScan().fromSnapshotInclusive(tagSnapshotAName).useBranch(branchName);
    Assertions.assertThat(scan2.planFiles()).hasSize(3);

    IncrementalAppendScan scan3 = newScan().toSnapshot(snapshotBranchBId).useBranch(branchName);
    Assertions.assertThat(scan3.planFiles()).hasSize(2);

    IncrementalAppendScan scan4 = newScan().toSnapshot(snapshotBranchCId).useBranch(branchName);
    Assertions.assertThat(scan4.planFiles()).hasSize(3);

    IncrementalAppendScan scan5 =
        newScan()
            .fromSnapshotExclusive(tagSnapshotAName)
            .toSnapshot(snapshotBranchBId)
            .useBranch(branchName);
    Assertions.assertThat(scan5.planFiles()).hasSize(1);
  }

  @Test
  public void testUseBranchWithTagShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    Assertions.assertThatThrownBy(
            () -> newScan().fromSnapshotInclusive(snapshotAId).useBranch(tagSnapshotAName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a branch", tagSnapshotAName));
  }

  @Test
  public void testUseBranchWithInvalidSnapshotShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();
    long snapshotMainBId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchBId = table.snapshot(branchName).snapshotId();

    /*

          files:FILE_A            files:FILE_B FILE_B
          ---- snapshotA  ------ snapshotMainB
                        \
                         \
                          \files:FILE_C
                          snapshotBranchB(branch:b1)
    */
    Assertions.assertThatThrownBy(
            () -> newScan().toSnapshot(snapshotMainBId).useBranch(branchName).planFiles())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("End snapshot is not a valid snapshot on the current branch");

    Assertions.assertThatThrownBy(
            () ->
                newScan().fromSnapshotInclusive(snapshotMainBId).useBranch(branchName).planFiles())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "Starting snapshot (inclusive) %s is not an ancestor of end snapshot %s",
                snapshotMainBId, snapshotBranchBId));
  }

  @Test
  public void testUseBranchWithNonExistingRef() {
    Assertions.assertThatThrownBy(() -> newScan().useBranch("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @Test
  public void testFromSnapshotExclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan().fromSnapshotExclusive(snapshotAId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(snapshotAId).toSnapshot(snapshotBId);
    Assert.assertEquals(1, Iterables.size(scanWithToSnapshot.planFiles()));
  }

  @Test
  public void testFromSnapshotExclusiveForExpiredParent() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    long expireTimestampSnapshotA =
        TestHelpers.waitUntilAfter(table.currentSnapshot().timestampMillis());
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();
    table.expireSnapshots().expireOlderThan(expireTimestampSnapshotA).commit();

    IncrementalAppendScan scan = newScan().fromSnapshotExclusive(snapshotAId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(snapshotAId).toSnapshot(snapshotBId);
    Assert.assertEquals(1, Iterables.size(scanWithToSnapshot.planFiles()));
  }

  @Test
  public void fromSnapshotExclusiveWithNonExistingRef() {
    Assertions.assertThatThrownBy(() -> newScan().fromSnapshotExclusive("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @Test
  public void testFromSnapshotExclusiveWithTag() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagSnapshotBName = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagSnapshotBName, snapshotBId).commit();
    table.newFastAppend().appendFile(FILE_C).appendFile(FILE_C).commit();

    /*
              files:FILE_A         files:FILE_B FILE_B       files:FILE_C FILE_C
     ---- snapshotAId(tag:t1) ---- snapshotMainB(tag:t2) ----  currentSnapshot
    */
    IncrementalAppendScan scan = newScan().fromSnapshotExclusive(tagSnapshotAName);
    Assertions.assertThat(scan.planFiles()).hasSize(4);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(tagSnapshotAName).toSnapshot(tagSnapshotBName);
    Assertions.assertThat(scanWithToSnapshot.planFiles()).hasSize(2);
  }

  @Test
  public void fromSnapshotExclusiveWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    Assertions.assertThatThrownBy(() -> newScan().fromSnapshotExclusive(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @Test
  public void testToSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan().toSnapshot(snapshotBId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));
  }

  @Test
  public void testToSnapshotWithTag() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotBId).commit();

    String tagSnapshotMainBName = "t1";
    table.manageSnapshots().createTag(tagSnapshotMainBName, snapshotBId).commit();
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_B).commit();

    String tagSnapshotBranchBName = "t2";
    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchBId = table.snapshot(branchName).snapshotId();
    table.manageSnapshots().createTag(tagSnapshotBranchBName, snapshotBranchBId).commit();

    /*

          files:FILE_A            files:FILE_B              files:FILE_B FILE_B
          ----snapshotA  ------ snapshotMainB(tag:t1) --------  currentSnapshot
                                        \
                                         \
                                          \files:FILE_C
                                          snapshotBranchB(branch:b1, tag:t2)
    */
    IncrementalAppendScan scan = newScan().toSnapshot(tagSnapshotMainBName);
    Assertions.assertThat(scan.planFiles()).hasSize(2);

    IncrementalAppendScan scan2 = newScan().toSnapshot(tagSnapshotBranchBName);
    Assertions.assertThat(scan2.planFiles()).hasSize(3);
  }

  @Test
  public void testToSnapshotWithNonExistingRef() {
    Assertions.assertThatThrownBy(() -> newScan().toSnapshot("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @Test
  public void testToSnapshotWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotId).commit();

    Assertions.assertThatThrownBy(() -> newScan().toSnapshot(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @Test
  public void testMultipleRootSnapshots() throws Exception {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    long expireTimestampSnapshotA =
        TestHelpers.waitUntilAfter(table.currentSnapshot().timestampMillis());

    // append file B in a staged branch
    AppendFiles appendFiles = table.newFastAppend().appendFile(FILE_B).stageOnly();
    long snapshotBId = appendFiles.apply().snapshotId();
    appendFiles.commit();

    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_D).commit();
    long snapshotDId = table.currentSnapshot().snapshotId();

    // expiring snapshotA should create two root snapshots (B and C)
    table.expireSnapshots().expireOlderThan(expireTimestampSnapshotA).commit();

    // scan should discover snapshot C and D
    IncrementalAppendScan scan = newScan().toSnapshot(snapshotDId);
    Assert.assertEquals(2, Iterables.size(scan.planFiles()));

    // scan should fail because snapshot B is not an ancestor of snapshot D
    IncrementalAppendScan scanShouldFail =
        newScan().fromSnapshotExclusive(snapshotBId).toSnapshot(snapshotDId);
    Assertions.assertThatThrownBy(() -> Iterables.size(scanShouldFail.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Starting snapshot (exclusive) %d is not a parent ancestor of end snapshot %d",
                snapshotBId, snapshotDId));

    // scan should fail because snapshot B is not an ancestor of snapshot D
    IncrementalAppendScan scanShouldFailInclusive =
        newScan().fromSnapshotInclusive(snapshotBId).toSnapshot(snapshotDId);
    Assertions.assertThatThrownBy(() -> Iterables.size(scanShouldFailInclusive.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Starting snapshot (inclusive) %d is not an ancestor of end snapshot %d",
                snapshotBId, snapshotDId));
  }
}
