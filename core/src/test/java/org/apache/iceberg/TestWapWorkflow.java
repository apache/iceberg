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

import com.google.common.collect.Iterables;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestWapWorkflow extends TableTestBase {

  @Before
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
  }

  @Test
  public void testWithRollback() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();
    base = readMetadata();

    Snapshot wapSnapshot = base.snapshots().get(1);

    Assert.assertEquals("Metadata should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Snapshot should have wap id in summary", "123456789",
        wapSnapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // do rollback
    table.manageSnapshots().rollback(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    Assert.assertEquals("Current snapshot should be what we rolled back to",
        wapSnapshot.snapshotId(), base.currentSnapshot().snapshotId());
    Assert.assertEquals("Metadata should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());
  }

  @Test
  public void testRollbackAndFastForward() {
    // first snapshot
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    Snapshot firstSnapshot = base.currentSnapshot();
    long firstSnapshotId = firstSnapshot.snapshotId();

    // second snapshot
    table.newAppend()
        .appendFile(FILE_B)
        .commit();
    base = readMetadata();
    Snapshot secondSnapshot = base.currentSnapshot();

    // third snapshot
    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    base = readMetadata();
    Snapshot thirdSnapshot = base.currentSnapshot();

    // rollback to first snapshot
    table.manageSnapshots().rollback(firstSnapshotId).commit();
    base = readMetadata();
    Assert.assertEquals("Should be at first snapshot", firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should have all three snapshots in the system", 3, base.snapshots().size());

    // fast forward to third snapshot
    table.manageSnapshots().cherrypick(thirdSnapshot.snapshotId()).commit();
    base = readMetadata();
    Assert.assertEquals("Current state should be at third snapshot", 4,
        base.currentSnapshot().snapshotId());

    // fast forward to 2nd snapshot
    table.manageSnapshots().cherrypick(secondSnapshot.snapshotId()).commit();
    base = readMetadata();
    Assert.assertEquals("Current state should be at second snapshot", 5,
        base.currentSnapshot().snapshotId());
    Assert.assertEquals("Count all snapshots", 5,
        base.snapshots().size());
  }

  @Test
  public void testRollbackNoWAP() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    Snapshot firstSnapshot = base.currentSnapshot();
    long firstSnapshotId = firstSnapshot.snapshotId();

    table.newAppend()
        .appendFile(FILE_B)
        .commit();
    base = readMetadata();

    // do rollback
    table.manageSnapshots().rollback(firstSnapshotId).commit();
    base = readMetadata();

    Assert.assertEquals("Current snapshot should be what we rolled back to",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Metadata should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Should contain manifests for both files", 1, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 3,
        base.snapshotLog().size());
  }

  @Test
  public void testWithCherryPicking() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    // first WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = base.snapshots().get(1);

    Assert.assertEquals("Should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wapSnapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        wapSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should have three snapshots", 3, base.snapshots().size());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());
  }

  @Test
  public void testWithTwoPhaseCherryPicking() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();

    // second WAP commit
    table.newAppend()
        .appendFile(FILE_C)
        .set("wap.id", "987654321")
        .stageOnly()
        .commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);
    Snapshot wap2Snapshot = base.snapshots().get(2);

    Assert.assertEquals("Should have three snapshots", 3, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wap1Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Should have second wap id in summary", "987654321",
        wap2Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot id should be same for first WAP snapshot",
        firstSnapshotId, wap1Snapshot.parentId().longValue());
    Assert.assertEquals("Parent snapshot id should be same for second WAP snapshot",
        firstSnapshotId, wap2Snapshot.parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        parentSnapshot.snapshotId() + 2 /* two staged snapshots */ + 1,
        base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 2,
        base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should change to latest snapshot before commit",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick second snapshot
    table.manageSnapshots().cherrypick(wap2Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        parentSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 3, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should change to latest snapshot before commit",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 3,
        base.snapshotLog().size());
  }

  @Test
  public void testWithCommitsBetweenCherryPicking() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();

    // second WAP commit
    table.newAppend()
        .appendFile(FILE_C)
        .set("wap.id", "987654321")
        .stageOnly()
        .commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);
    Snapshot wap2Snapshot = base.snapshots().get(2);

    Assert.assertEquals("Should have three snapshots", 3, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wap1Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Should have second wap id in summary", "987654321",
        wap2Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot id should be same for first WAP snapshot",
        firstSnapshotId, wap1Snapshot.parentId().longValue());
    Assert.assertEquals("Parent snapshot id should be same for second WAP snapshot",
        firstSnapshotId, wap2Snapshot.parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // load current snapshot
    parentSnapshot = base.currentSnapshot();

    // table has new commit
    table.newAppend()
        .appendFile(FILE_D)
        .commit();
    base = readMetadata();

    Assert.assertEquals("Should have four snapshots", 4, base.snapshots().size());
    Assert.assertEquals("Current snapshot should carry over the parent snapshot",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Should contain manifests for two files", 2,
        base.currentSnapshot().manifests().size());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Should have five snapshots", 5, base.snapshots().size());
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        parentSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for three files", 3,
        base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should point to same snapshot",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 3,
        base.snapshotLog().size());

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap2Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Should have all the snapshots", 6, base.snapshots().size());
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        parentSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for four files", 4,
        base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should point to same snapshot",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 4,
        base.snapshotLog().size());
  }


  @Test
  public void testWithCherryPickingWithCommitRetry() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();

    // // second WAP commit
    // table.newAppend()
    //     .appendFile(FILE_C)
    //     .set(SnapshotSummary.STAGED_WAP_ID_PROP, "987654321")
    //     .stageOnly()
    //     .commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);
    // Snapshot wap2Snapshot = base.snapshots().get(2);

    Assert.assertEquals("Should have three snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wap1Snapshot.summary().get("wap.id"));
    // Assert.assertEquals("Should have second wap id in summary", "987654321",
    //     wap2Snapshot.summary().get(SnapshotSummary.STAGED_WAP_ID_PROP));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Parent snapshot id should be same for first WAP snapshot",
        firstSnapshotId, wap1Snapshot.parentId().longValue());
    // Assert.assertEquals("Parent snapshot id should be same for second WAP snapshot",
    //     firstSnapshotId, wap2Snapshot.parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // load current snapshot
    base = readMetadata();
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.ops().failCommits(3);
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        parentSnapshot.snapshotId() + 1 /* one staged snapshot */ + 1,
        base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 2,
        base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should not contain redundant append due to retry", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Parent snapshot id should change to latest snapshot before commit",
        parentSnapshot.snapshotId(), base.currentSnapshot().parentId().longValue());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());
  }


  @Test
  public void testWithDuplicateCherryPicking() {

    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    // first WAP commit
    table.newAppend()
        .appendFile(FILE_B)
        .set("wap.id", "123456789")
        .stageOnly()
        .commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = base.snapshots().get(1);

    Assert.assertEquals("Should have both snapshots", 2, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wapSnapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 1,
        base.snapshotLog().size());

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        wapSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should have three snapshots", 3, base.snapshots().size());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
    Assert.assertEquals("Snapshot log should indicate number of snapshots committed", 2,
        base.snapshotLog().size());

    AssertHelpers.assertThrows("should throw exception", ValidationException.class,
        String.format("Duplicate request to cherry pick wap id that was published already: %s", 12345678), () -> {
          // duplicate cherry-pick snapshot
          table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();
        }
    );
  }
}
