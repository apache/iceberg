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

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestBaseIncrementalAppendScan
    extends ScanTestBase<IncrementalAppendScan, FileScanTask, CombinedScanTask> {

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @Override
  protected IncrementalAppendScan newScan() {
    return table.newIncrementalAppendScan();
  }

  @TestTemplate
  public void testFromSnapshotInclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan().fromSnapshotInclusive(snapshotAId);
    assertThat(scan.planFiles()).hasSize(3);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(snapshotCId);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(3);
  }

  @TestTemplate
  public void fromSnapshotInclusiveWithNonExistingRef() {
    assertThatThrownBy(() -> newScan().fromSnapshotInclusive("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @TestTemplate
  public void fromSnapshotInclusiveWithTag() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagSnapshotBName = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagSnapshotBName, snapshotBId).commit();
    table.newFastAppend().appendFile(FILE_D).appendFile(FILE_A2).commit();

    /*
              files:FILE_A         files:FILE_B FILE_C       files:FILE_D FILE_A2
     ---- snapshotAId(tag:t1) ---- snapshotMainB(tag:t2) ----  currentSnapshot
    */
    IncrementalAppendScan scan = newScan().fromSnapshotInclusive(tagSnapshotAName);
    assertThat(scan.planFiles()).hasSize(5);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotInclusive(tagSnapshotAName).toSnapshot(tagSnapshotBName);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(3);
  }

  @TestTemplate
  public void fromSnapshotInclusiveWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    assertThatThrownBy(() -> newScan().fromSnapshotInclusive(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));

    assertThatThrownBy(() -> newScan().fromSnapshotInclusive(snapshotAId).toSnapshot(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @TestTemplate
  public void testUseBranch() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    String tagSnapshotAName = "t1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagName2 = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    long snapshotMainBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagName2, snapshotMainBId).commit();

    table.newFastAppend().appendFile(FILE_D).appendFile(FILE_A2).commit();

    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchBId = table.snapshot(branchName).snapshotId();

    table.newFastAppend().appendFile(FILE_C).toBranch(branchName).commit();
    long snapshotBranchCId = table.snapshot(branchName).snapshotId();

    /*

            files:FILE_A         files:FILE_B FILE_C       files:FILE_D FILE_A2
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
    assertThat(scan.planFiles()).hasSize(5);

    IncrementalAppendScan scan2 =
        newScan().fromSnapshotInclusive(tagSnapshotAName).useBranch(branchName);
    assertThat(scan2.planFiles()).hasSize(3);

    IncrementalAppendScan scan3 = newScan().toSnapshot(snapshotBranchBId).useBranch(branchName);
    assertThat(scan3.planFiles()).hasSize(2);

    IncrementalAppendScan scan4 = newScan().toSnapshot(snapshotBranchCId).useBranch(branchName);
    assertThat(scan4.planFiles()).hasSize(3);

    IncrementalAppendScan scan5 =
        newScan()
            .fromSnapshotExclusive(tagSnapshotAName)
            .toSnapshot(snapshotBranchBId)
            .useBranch(branchName);
    assertThat(scan5.planFiles()).hasSize(1);
  }

  @TestTemplate
  public void testUseBranchWithTagShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    assertThatThrownBy(
            () -> newScan().fromSnapshotInclusive(snapshotAId).useBranch(tagSnapshotAName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a branch", tagSnapshotAName));
  }

  @TestTemplate
  public void testUseBranchWithInvalidSnapshotShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    long snapshotMainBId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    table.newFastAppend().appendFile(FILE_D).toBranch(branchName).commit();
    long snapshotBranchBId = table.snapshot(branchName).snapshotId();

    /*

          files:FILE_A            files:FILE_B FILE_C
          ---- snapshotA  ------ snapshotMainB
                        \
                         \
                          \files:FILE_D
                          snapshotBranchB(branch:b1)
    */
    assertThatThrownBy(
            () -> newScan().toSnapshot(snapshotMainBId).useBranch(branchName).planFiles())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("End snapshot is not a valid snapshot on the current branch");

    assertThatThrownBy(
            () ->
                newScan().fromSnapshotInclusive(snapshotMainBId).useBranch(branchName).planFiles())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            String.format(
                "Starting snapshot (inclusive) %s is not an ancestor of end snapshot %s",
                snapshotMainBId, snapshotBranchBId));
  }

  @TestTemplate
  public void testUseBranchWithNonExistingRef() {
    assertThatThrownBy(() -> newScan().useBranch("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @TestTemplate
  public void testFromSnapshotExclusive() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan().fromSnapshotExclusive(snapshotAId);
    assertThat(scan.planFiles()).hasSize(2);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(snapshotAId).toSnapshot(snapshotBId);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(1);
  }

  @TestTemplate
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
    assertThat(scan.planFiles()).hasSize(2);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(snapshotAId).toSnapshot(snapshotBId);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(1);
  }

  @TestTemplate
  public void fromSnapshotExclusiveWithNonExistingRef() {
    assertThatThrownBy(() -> newScan().fromSnapshotExclusive("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @TestTemplate
  public void testFromSnapshotExclusiveWithTag() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String tagSnapshotAName = "t1";
    table.manageSnapshots().createTag(tagSnapshotAName, snapshotAId).commit();

    String tagSnapshotBName = "t2";
    table.newFastAppend().appendFile(FILE_B).appendFile(FILE_C).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag(tagSnapshotBName, snapshotBId).commit();
    table.newFastAppend().appendFile(FILE_D).appendFile(FILE_A2).commit();

    /*
              files:FILE_A         files:FILE_B FILE_C       files:FILE_D FILE_A2
     ---- snapshotAId(tag:t1) ---- snapshotMainB(tag:t2) ----  currentSnapshot
    */
    IncrementalAppendScan scan = newScan().fromSnapshotExclusive(tagSnapshotAName);
    assertThat(scan.planFiles()).hasSize(4);

    IncrementalAppendScan scanWithToSnapshot =
        newScan().fromSnapshotExclusive(tagSnapshotAName).toSnapshot(tagSnapshotBName);
    assertThat(scanWithToSnapshot.planFiles()).hasSize(2);
  }

  @TestTemplate
  public void fromSnapshotExclusiveWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    assertThatThrownBy(() -> newScan().fromSnapshotExclusive(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @TestTemplate
  public void testToSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotAId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotBId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_C).commit();
    long snapshotCId = table.currentSnapshot().snapshotId();

    IncrementalAppendScan scan = newScan().toSnapshot(snapshotBId);
    assertThat(scan.planFiles()).hasSize(2);
  }

  @TestTemplate
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
    assertThat(scan.planFiles()).hasSize(2);

    IncrementalAppendScan scan2 = newScan().toSnapshot(tagSnapshotBranchBName);
    assertThat(scan2.planFiles()).hasSize(3);
  }

  @TestTemplate
  public void testToSnapshotWithNonExistingRef() {
    assertThatThrownBy(() -> newScan().toSnapshot("nonExistingRef"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find ref: nonExistingRef");
  }

  @TestTemplate
  public void testToSnapshotWithBranchShouldFail() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.newFastAppend().appendFile(FILE_B).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    String branchName = "b1";
    table.manageSnapshots().createBranch(branchName, snapshotId).commit();

    assertThatThrownBy(() -> newScan().toSnapshot(branchName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(String.format("Ref %s is not a tag", branchName));
  }

  @TestTemplate
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
    assertThat(scan.planFiles()).hasSize(2);

    // scan should fail because snapshot B is not an ancestor of snapshot D
    IncrementalAppendScan scanShouldFail =
        newScan().fromSnapshotExclusive(snapshotBId).toSnapshot(snapshotDId);
    assertThatThrownBy(() -> Iterables.size(scanShouldFail.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Starting snapshot (exclusive) %d is not a parent ancestor of end snapshot %d",
                snapshotBId, snapshotDId));

    // scan should fail because snapshot B is not an ancestor of snapshot D
    IncrementalAppendScan scanShouldFailInclusive =
        newScan().fromSnapshotInclusive(snapshotBId).toSnapshot(snapshotDId);
    assertThatThrownBy(() -> Iterables.size(scanShouldFailInclusive.planFiles()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            String.format(
                "Starting snapshot (inclusive) %d is not an ancestor of end snapshot %d",
                snapshotBId, snapshotDId));
  }
}
