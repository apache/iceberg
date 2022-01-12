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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestUpdateSnapshotRefs extends TableTestBase {

  private static final String TEST_BRANCH = "testBranch";
  private static final String TEST_TAG = "testTag";

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 2 };
  }

  public TestUpdateSnapshotRefs(int formatVersion) {
    super(formatVersion);
  }

  @Before
  public void setupTable() throws Exception {
    super.setupTable();

    TableMetadata base = table.ops().current();

    Map<String, SnapshotRef> refs = ImmutableMap.of(
        TEST_BRANCH, SnapshotRef.branchBuilder(123).maxSnapshotAgeMs(1L).minSnapshotsToKeep(1).build(),
        TEST_TAG, SnapshotRef.tagBuilder(456).maxRefAgeMs(1L).build());

    List<Snapshot> snapshots = Lists.newArrayList(
        new BaseSnapshot(table.ops().io(), 789, null, "file:/tmp/manifest1.avro"),
        new BaseSnapshot(table.ops().io(), 456, null, "file:/tmp/manifest1.avro"),
        new BaseSnapshot(table.ops().io(), 123, null, "file:/tmp/manifest1.avro"));

    List<HistoryEntry> snapshotLogs = snapshots.stream()
        .map(snapshot -> new TableMetadata.SnapshotLogEntry(snapshot.snapshotId(), snapshot.snapshotId()))
        .collect(Collectors.toList());

    TableMetadata newTableMetadata = new TableMetadata(
        metadataDir.getAbsolutePath(), base.formatVersion(), base.uuid(), base.location(),
        base.lastSequenceNumber(), base.lastUpdatedMillis(), base.lastColumnId(), base.currentSchemaId(),
        base.schemas(), base.defaultSpecId(), base.specs(), base.lastAssignedPartitionId(),
        base.defaultSortOrderId(), base.sortOrders(), base.properties(), 123, snapshots,
        snapshotLogs, base.previousFiles(), refs, ImmutableList.of());

    table.ops().commit(base, newTableMetadata);
    table.refresh();
  }

  @Test
  public void testCreateTag() {
    String tag = "newTag";
    table.updateRefs().tag(tag, 789).commit();
    table.refresh();
    assertExpectedReferenceAtSnapshot(tag, 789, SnapshotRefType.TAG);
  }

  @Test
  public void testCreateTagNameInvalid() {
    AssertHelpers.assertThrows("Should fail when tag name is null",
        IllegalArgumentException.class,
        "Tag name must not be null",
        () -> table.updateRefs().tag(null, 789));

    AssertHelpers.assertThrows("Should fail when tag name already exists as a tag",
        IllegalArgumentException.class,
        "Cannot tag snapshot, ref already exists: testTag",
        () -> table.updateRefs().tag("testTag", 789));

    AssertHelpers.assertThrows("Should fail when tag name already exists as a branch",
        IllegalArgumentException.class,
        "Cannot tag snapshot, ref already exists: testBranch",
        () -> table.updateRefs().tag("testBranch", 789));
  }

  @Test
  public void testCreateBranch() {
    String branch = "newBranch";
    table.updateRefs().branch(branch, 789).commit();
    table.refresh();
    assertExpectedReferenceAtSnapshot(branch, 789, SnapshotRefType.BRANCH);
  }

  @Test
  public void testCreateMultipleRefsWithRetention() {
    String branch = "newBranch";
    String tag = "newTag";
    long maxSnapshotAgeMs = 1000;
    int minSnapshots = 10;
    long tagLifetime = 1000;
    long branchSnapshot = 789;
    long tagSnapshot = 123;
    table.updateRefs()
        .branch(branch, branchSnapshot)
        .setBranchSnapshotLifetime(branch, maxSnapshotAgeMs)
        .setMinSnapshotsInBranch(branch, minSnapshots)
        .tag(tag, tagSnapshot)
        .setLifetime(tag, tagLifetime)
        .commit();
    table.refresh();
    assertExpectedReferenceAtSnapshot(branch, branchSnapshot, SnapshotRefType.BRANCH);
    assertExpectedReferenceAtSnapshot(tag, tagSnapshot, SnapshotRefType.TAG);
    Assert.assertTrue("Branch should have the expected max snapshot age",
        table.ref(branch).maxSnapshotAgeMs() == maxSnapshotAgeMs);
    Assert.assertTrue("Branch should have the expected min number of snapshots",
        table.ref(branch).minSnapshotsToKeep() == minSnapshots);
    Assert.assertTrue("Tag should have the expected min number of snapshots",
            table.ref(branch).minSnapshotsToKeep() == minSnapshots);
  }

  @Test
  public void testCreateBranchNameInvalid() {
    AssertHelpers.assertThrows("Should fail when branch name is null",
        IllegalArgumentException.class,
        "Branch name must not be null",
        () -> table.updateRefs().branch(null, 789));

    AssertHelpers.assertThrows("Should fail when branch name already exists as a tag",
        IllegalArgumentException.class,
        "Cannot create branch, ref already exists: testTag",
        () -> table.updateRefs().branch(TEST_TAG, 789));

    AssertHelpers.assertThrows("Should fail when branch name already exists as a branch",
        IllegalArgumentException.class,
        "Cannot create branch, ref already exists: testBranch",
        () -> table.updateRefs().branch(TEST_BRANCH, 789));
  }

  @Test
  public void testCreateRefSnapshotNotExist() {
    AssertHelpers.assertThrows("Should fail when snapshot to create tag does not exist",
        IllegalArgumentException.class,
        "Cannot find snapshot with ID: 233",
        () -> table.updateRefs().tag("tag", 233));

    AssertHelpers.assertThrows("Should fail when snapshot to create branch does not exist",
        IllegalArgumentException.class,
        "Cannot find snapshot with ID: 233",
        () -> table.updateRefs().branch("branch", 233));
  }

  @Test
  public void testSetRefLifetime() {
    table.updateRefs()
        .setLifetime(TEST_BRANCH, 10000)
        .setLifetime(TEST_TAG, 20000)
        .commit();
    table.refresh();
    Assert.assertEquals("Should have latest lifetime config for branch",
        10000, (long) table.refs().get(TEST_BRANCH).maxRefAgeMs());
    Assert.assertEquals("Should have latest lifetime config for tag",
        20000, (long) table.refs().get(TEST_TAG).maxRefAgeMs());
  }

  @Test
  public void testSetMainBranchLifetimeShouldFail() {
    AssertHelpers.assertThrows("Should not be able to set lifetime for main branch",
        IllegalArgumentException.class,
        "Main branch is retained forever",
        () -> table.updateRefs().setLifetime(SnapshotRef.MAIN_BRANCH, 100));
  }

  @Test
  public void testSetBranchSnapshotLifetime() {
    table.updateRefs()
        .setBranchSnapshotLifetime(TEST_BRANCH, 10000)
        .commit();
    table.refresh();
    Assert.assertEquals("Should have latest snapshot lifetime config for branch",
        10000, (long) table.refs().get(TEST_BRANCH).maxSnapshotAgeMs());
  }

  @Test
  public void testSetMinSnapshotsInBranch() {
    table.updateRefs()
        .setMinSnapshotsInBranch(TEST_BRANCH, 10000)
        .commit();
    table.refresh();
    Assert.assertEquals("Should have latest snapshot in branch config for branch",
        10000, (int) table.refs().get(TEST_BRANCH).minSnapshotsToKeep());
  }

  @Test
  public void testRenameTag() {
    SnapshotRef tagRef = table.refs().get(TEST_TAG);
    table.updateRefs().rename(TEST_TAG, "tag2").commit();
    Assert.assertEquals("Renamed tag should have the same config", tagRef, table.refs().get("tag2"));
  }

  @Test
  public void testRenameBranch() {
    SnapshotRef tagRef = table.refs().get(TEST_BRANCH);
    table.updateRefs().rename(TEST_BRANCH, "branch2").commit();
    Assert.assertEquals("Renamed branch should have the same config", tagRef, table.refs().get("branch2"));
  }

  @Test
  public void testInvalidRename() {
    AssertHelpers.assertThrows("Should not have null from name",
        IllegalArgumentException.class,
        "Names must not be null",
        () -> table.updateRefs().rename(null, "to"));

    AssertHelpers.assertThrows("Should not have null to name",
        IllegalArgumentException.class,
        "Names must not be null",
        () -> table.updateRefs().rename("from", null));

    AssertHelpers.assertThrows("From ref must exist",
        IllegalArgumentException.class,
        "Cannot find ref to rename from: tag",
        () -> table.updateRefs().rename("tag", "tag2"));

    AssertHelpers.assertThrows("To ref must not exist",
        IllegalArgumentException.class,
        "Cannot rename to an existing ref: " + TEST_BRANCH,
        () -> table.updateRefs().rename(TEST_TAG, TEST_BRANCH));
  }

  @Test
  public void testRemoveTag() {
    table.updateRefs().remove(TEST_TAG).commit();
    Assert.assertNull("Removed tag should not exist", table.refs().get(TEST_TAG));
  }

  @Test
  public void testRemoveBranch() {
    table.updateRefs().remove(TEST_BRANCH).commit();
    Assert.assertNull("Removed branch should not exist", table.refs().get(TEST_BRANCH));
  }

  @Test
  public void testRemoveMainBranchShouldFail() {
    AssertHelpers.assertThrows("Should fail when removing main branch",
        IllegalArgumentException.class,
        "Main branch must not be removed",
        () -> table.updateRefs().remove(SnapshotRef.MAIN_BRANCH));
  }

  @Test
  public void testRemoveRefNotExist() {
    AssertHelpers.assertThrows("Should fail when removing ref not exist",
        IllegalArgumentException.class,
        "Cannot find ref to remove",
        () -> table.updateRefs().remove("tag"));
  }

  private void assertExpectedReferenceAtSnapshot(String name, long snapshotId, SnapshotRefType refType) {
    SnapshotRef ref = table.ref(name);
    Assert.assertTrue("Should contain reference with expected name", ref != null);
    Assert.assertTrue("Should be referring to the expected snapshotId", ref.snapshotId() == snapshotId);
    Assert.assertTrue("Should be a reference with expected type", ref.type() == refType);
  }
}
