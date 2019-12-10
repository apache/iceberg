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
import org.junit.Assert;
import org.junit.Test;

public class TestWapWorkflow extends TableTestBase {

  @Test
  public void testWithRollback() {

    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
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

    // do rollback
    table.rollback().toSnapshotId(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    Assert.assertEquals("Current snapshot should be what we rolled back to",
        wapSnapshot.snapshotId(), base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
  }

  @Test
  public void testWithCherrypicking() {

    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
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

    // cherry-pick snapshot
    table.cherrypick().fromSnapshotId(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        wapSnapshot.snapshotId() + 1, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
  }

  @Test
  public void testWithTwoPhaseCherrypicking() {

    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
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

    Assert.assertEquals("Should have both snapshots", 3, base.snapshots().size());
    Assert.assertEquals("Should have first wap id in summary", "123456789",
        wap1Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Should have second wap id in summary", "987654321",
        wap2Snapshot.summary().get("wap.id"));
    Assert.assertEquals("Current snapshot should be first commit's snapshot",
        firstSnapshotId, base.currentSnapshot().snapshotId());

    // cherry-pick first snapshot
    table.cherrypick().fromSnapshotId(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        4, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 2, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));


    // cherry-pick second snapshot
    table.cherrypick().fromSnapshotId(wap2Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    Assert.assertEquals("Current snapshot should be set to one after wap snapshot",
        5, base.currentSnapshot().snapshotId());
    Assert.assertEquals("Should contain manifests for both files", 3, base.currentSnapshot().manifests().size());
    Assert.assertEquals("Should contain append from last commit", 1,
        Iterables.size(base.currentSnapshot().addedFiles()));
  }

}
