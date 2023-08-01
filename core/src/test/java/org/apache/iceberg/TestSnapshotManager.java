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

import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotManager extends TableTestBase {

  // replacement for FILE_A
  static final DataFile REPLACEMENT_FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a-replacement.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  // conflict in the same partition as FILE_A
  static final DataFile CONFLICT_FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a-conflict.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=0") // easy way to set partition data for now
          .withRecordCount(1)
          .build();

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestSnapshotManager(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testCherryPickDynamicOverwrite() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();

    // pick the snapshot into the current state
    table.manageSnapshots().cherrypick(staged.snapshotId()).commit();

    Assert.assertNotEquals(
        "Should not fast-forward", staged.snapshotId(), table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteWithoutParent() {
    Assert.assertNull("Table should not have a current snapshot", table.currentSnapshot());

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();

    // pick the snapshot into the current state
    table.manageSnapshots().cherrypick(staged.snapshotId()).commit();

    Assert.assertNotEquals(
        "Should not fast-forward", staged.snapshotId(), table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteConflict() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(CONFLICT_FILE_A).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot cherry-pick replace partitions with changed partition");

    Assert.assertEquals(
        "Failed cherry-pick should not change the table state",
        lastSnapshotId,
        table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A, CONFLICT_FILE_A);
  }

  @Test
  public void testCherryPickDynamicOverwriteDeleteConflict() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add FILE_B s
    table.newAppend().appendFile(FILE_B).commit();

    // delete FILE_A so the replace operation is no longer valid
    table.newDelete().deleteFile(FILE_A).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Missing required files to delete");

    Assert.assertEquals(
        "Failed cherry-pick should not change the table state",
        lastSnapshotId,
        table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_B);
  }

  @Test
  public void testCherryPickFromBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long branchSnapshotId = table.currentSnapshot().snapshotId();

    // add a second commit before replacing FILE_A
    table.newAppend().appendFile(FILE_B).commit();

    // replace FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).commit();
    long replaceSnapshotId = table.currentSnapshot().snapshotId();

    // rewrite history so the replacement is in a branch, not base directly on an ancestor of the
    // current state
    table.manageSnapshots().rollbackTo(branchSnapshotId).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().cherrypick(replaceSnapshotId).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Cannot cherry-pick overwrite not based on an ancestor of the current state");

    Assert.assertEquals(
        "Failed cherry-pick should not change the table state",
        lastSnapshotId,
        table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A);
  }

  @Test
  public void testCherryPickOverwrite() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite to replace FILE_A
    table.newOverwrite().deleteFile(FILE_A).addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    Assert.assertEquals(
        "Should find the staged overwrite snapshot", DataOperations.OVERWRITE, staged.operation());

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("not append, dynamic overwrite, or fast-forward");

    Assert.assertEquals(
        "Failed cherry-pick should not change the table state",
        lastSnapshotId,
        table.currentSnapshot().snapshotId());
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @Test
  public void testCreateBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a branch
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    SnapshotRef expectedBranch = table.ops().refresh().ref("branch1");
    Assert.assertTrue(
        expectedBranch != null
            && expectedBranch.equals(SnapshotRef.branchBuilder(snapshotId).build()));
  }

  @Test
  public void testCreateBranchWithoutSnapshotId() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a branch
    table.manageSnapshots().createBranch("branch1").commit();
    SnapshotRef actualBranch = table.ops().refresh().ref("branch1");
    Assertions.assertThat(actualBranch).isNotNull();
    Assertions.assertThat(actualBranch).isEqualTo(SnapshotRef.branchBuilder(snapshotId).build());
  }

  @Test
  public void testCreateBranchOnEmptyTable() {
    table.manageSnapshots().createBranch("branch1").commit();

    SnapshotRef mainSnapshotRef = table.ops().refresh().ref(SnapshotRef.MAIN_BRANCH);
    Assertions.assertThat(mainSnapshotRef).isNull();

    SnapshotRef branch1SnapshotRef = table.ops().refresh().ref("branch1");
    Assertions.assertThat(branch1SnapshotRef).isNotNull();
    Assertions.assertThat(branch1SnapshotRef.minSnapshotsToKeep()).isNull();
    Assertions.assertThat(branch1SnapshotRef.maxSnapshotAgeMs()).isNull();
    Assertions.assertThat(branch1SnapshotRef.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(branch1SnapshotRef.snapshotId());
    Assertions.assertThat(snapshot.parentId()).isNull();
    Assertions.assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    Assertions.assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @Test
  public void testCreateBranchFailsWhenRefAlreadyExists() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    // Trying to create a branch with an existing name should fail
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().createBranch("branch1", snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch1 already exists");

    // Trying to create another branch within the same chain
    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createBranch("branch2", snapshotId)
                    .createBranch("branch2", snapshotId)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch2 already exists");
  }

  @Test
  public void testCreateTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a tag
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    SnapshotRef expectedTag = table.ops().refresh().ref("tag1");

    Assert.assertTrue(
        expectedTag != null && expectedTag.equals(SnapshotRef.tagBuilder(snapshotId).build()));
  }

  @Test
  public void testCreateTagFailsWhenRefAlreadyExists() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("tag1", snapshotId).commit();

    // Trying to create a tag with an existing name should fail
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().createTag("tag1", snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref tag1 already exists");

    // Trying to create another tag within the same chain
    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag2", snapshotId)
                    .createTag("tag2", snapshotId)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref tag2 already exists");
  }

  @Test
  public void testRemoveBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating and then removing a branch and tag
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().removeBranch("branch1").commit();

    TableMetadata updated = table.ops().refresh();
    SnapshotRef expectedBranch = updated.ref("branch1");
    Assert.assertNull(expectedBranch);

    // Test chained creating and removal of branch and tag
    table.manageSnapshots().createBranch("branch2", snapshotId).removeBranch("branch2").commit();
    updated = table.ops().refresh();
    Assert.assertNull(updated.ref("branch2"));
  }

  @Test
  public void testRemovingNonExistingBranchFails() {
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().removeBranch("non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: non-existing");
  }

  @Test
  public void testRemovingMainBranchFails() {
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().removeBranch(SnapshotRef.MAIN_BRANCH).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove main branch");
  }

  @Test
  public void testRemoveTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating and then removing a branch and tag
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    table.manageSnapshots().removeTag("tag1").commit();
    TableMetadata updated = table.ops().refresh();
    SnapshotRef expectedTag = updated.ref("tag1");
    Assert.assertNull(expectedTag);

    // Test chained creating and removal of a tag
    table.manageSnapshots().createTag("tag2", snapshotId).removeTag("tag2").commit();
    Assert.assertEquals(updated, table.ops().refresh());
    Assert.assertNull(updated.ref("tag2"));
  }

  @Test
  public void testRemovingNonExistingTagFails() {
    Assertions.assertThatThrownBy(() -> table.manageSnapshots().removeTag("non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tag does not exist: non-existing");
  }

  @Test
  public void testReplaceBranch() {
    table.newAppend().appendFile(FILE_A).set("wap.id", "123").stageOnly().commit();
    Snapshot firstSnapshot = Iterables.getOnlyElement(table.snapshots());
    table.manageSnapshots().createBranch("branch1", firstSnapshot.snapshotId()).commit();
    table.newAppend().appendFile(FILE_B).set("wap.id", "456").stageOnly().commit();
    Snapshot secondSnapshot = Iterables.get(table.snapshots(), 1);
    table.manageSnapshots().createBranch("branch2", secondSnapshot.snapshotId()).commit();
    table.manageSnapshots().replaceBranch("branch1", "branch2").commit();
    Assert.assertEquals(
        table.ops().refresh().ref("branch1").snapshotId(), secondSnapshot.snapshotId());
  }

  @Test
  public void testReplaceBranchNonExistingTargetBranchFails() {
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().replaceBranch("non-existing", "other-branch").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Target branch does not exist: non-existing");
  }

  @Test
  public void testReplaceBranchNonExistingSourceFails() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    Assertions.assertThatThrownBy(
            () -> table.manageSnapshots().replaceBranch("branch1", "non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref does not exist: non-existing");
  }

  @Test
  public void testFastForward() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    Assert.assertEquals(table.currentSnapshot().snapshotId(), 1);

    table.manageSnapshots().createBranch("new-branch-at-staged-snapshot", 2).commit();
    table
        .manageSnapshots()
        .fastForwardBranch(SnapshotRef.MAIN_BRANCH, "new-branch-at-staged-snapshot")
        .commit();

    Assert.assertEquals(table.currentSnapshot().snapshotId(), 2);
  }

  @Test
  public void testFastForwardWhenTargetIsNotAncestorFails() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    long snapshot = table.currentSnapshot().snapshotId();

    // Commit a snapshot on main to deviate the branches
    table.newAppend().appendFile(FILE_C).commit();

    final String newBranch = "new-branch-at-staged-snapshot";
    table.manageSnapshots().createBranch(newBranch, snapshot).commit();

    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .fastForwardBranch(SnapshotRef.MAIN_BRANCH, newBranch)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot fast-forward: main is not an ancestor of new-branch-at-staged-snapshot");
  }

  @Test
  public void testReplaceTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    // Create a new snapshot and replace the tip of branch1 to be the new snapshot
    table.newAppend().appendFile(FILE_B).commit();
    long currentSnapshot = table.ops().refresh().currentSnapshot().snapshotId();
    table.manageSnapshots().replaceTag("tag1", currentSnapshot).commit();
    Assert.assertEquals(table.ops().refresh().ref("tag1").snapshotId(), currentSnapshot);
  }

  @Test
  public void testUpdatingBranchRetention() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test creating and updating independently
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table
        .manageSnapshots()
        .setMinSnapshotsToKeep("branch1", 10)
        .setMaxSnapshotAgeMs("branch1", 20000)
        .commit();
    TableMetadata updated = table.ops().refresh();
    Assert.assertEquals(20000, (long) updated.ref("branch1").maxSnapshotAgeMs());
    Assert.assertEquals(10, (long) updated.ref("branch1").minSnapshotsToKeep());
    // Test creating and updating in a chain
    table
        .manageSnapshots()
        .createBranch("branch2", snapshotId)
        .setMinSnapshotsToKeep("branch2", 10)
        .setMaxSnapshotAgeMs("branch2", 20000)
        .commit();
    updated = table.ops().refresh();
    Assert.assertEquals(20000, (long) updated.ref("branch2").maxSnapshotAgeMs());
    Assert.assertEquals(10, (long) updated.ref("branch2").minSnapshotsToKeep());
  }

  @Test
  public void testSettingBranchRetentionOnTagFails() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag1", snapshotId)
                    .setMinSnapshotsToKeep("tag1", 10)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting minSnapshotsToKeep");

    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag1", snapshotId)
                    .setMaxSnapshotAgeMs("tag1", 10)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting maxSnapshotAgeMs");
  }

  @Test
  public void testUpdatingBranchMaxRefAge() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    final long maxRefAgeMs = 10000;

    // Test creating and updating independently
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().setMaxRefAgeMs("branch1", 10000).commit();
    TableMetadata updated = table.ops().refresh();
    Assert.assertEquals(maxRefAgeMs, (long) updated.ref("branch1").maxRefAgeMs());
    Assert.assertEquals(maxRefAgeMs, (long) updated.ref("branch1").maxRefAgeMs());
  }

  @Test
  public void testUpdatingTagMaxRefAge() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    final long maxRefAgeMs = 10000;

    // Test creating and updating independently
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    table.manageSnapshots().setMaxRefAgeMs("tag1", maxRefAgeMs).commit();

    TableMetadata updated = table.ops().refresh();
    Assert.assertEquals(maxRefAgeMs, (long) updated.ref("tag1").maxRefAgeMs());

    // Test creating and updating in a chain
    table
        .manageSnapshots()
        .createTag("tag2", snapshotId)
        .setMaxRefAgeMs("tag2", maxRefAgeMs)
        .commit();
    updated = table.ops().refresh();
    Assert.assertEquals(maxRefAgeMs, (long) updated.ref("tag2").maxRefAgeMs());
  }

  @Test
  public void testRenameBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    // Test creating and renaming independently
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().renameBranch("branch1", "branch2").commit();
    TableMetadata updated = table.ops().refresh();
    Assert.assertNull(updated.ref("branch1"));
    Assert.assertEquals(updated.ref("branch2"), SnapshotRef.branchBuilder(snapshotId).build());

    table
        .manageSnapshots()
        .createBranch("branch3", snapshotId)
        .renameBranch("branch3", "branch4")
        .commit();

    updated = table.ops().refresh();
    Assert.assertNull(updated.ref("branch3"));
    Assert.assertEquals(updated.ref("branch4"), SnapshotRef.branchBuilder(snapshotId).build());
  }

  @Test
  public void testFailRenamingMainBranch() {
    Assertions.assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .renameBranch(SnapshotRef.MAIN_BRANCH, "some-branch")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename main branch");
  }

  @Test
  public void testRenamingNonExistingBranchFails() {
    Assertions.assertThatThrownBy(
            () ->
                table.manageSnapshots().renameBranch("some-missing-branch", "some-branch").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: some-missing-branch");
  }

  @Test
  public void testCreateReferencesAndRollback() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotPriorToRollback = table.currentSnapshot().snapshotId();

    table
        .manageSnapshots()
        .createBranch("branch1", snapshotPriorToRollback)
        .createTag("tag1", snapshotPriorToRollback)
        .rollbackTo(1)
        .commit();

    TableMetadata current = table.ops().current();
    Assert.assertEquals(current.currentSnapshot().snapshotId(), 1);
    SnapshotRef actualTag = current.ref("tag1");
    SnapshotRef actualBranch = current.ref("branch1");
    Assert.assertEquals(1, current.currentSnapshot().snapshotId());
    Assert.assertEquals(SnapshotRef.branchBuilder(snapshotPriorToRollback).build(), actualBranch);
    Assert.assertEquals(SnapshotRef.tagBuilder(snapshotPriorToRollback).build(), actualTag);
  }

  @Test
  public void testCreateReferencesAndCherrypick() {
    table.newAppend().appendFile(FILE_A).commit();

    long currentSnapshot = table.currentSnapshot().snapshotId();
    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();
    Snapshot staged = Iterables.getLast(table.snapshots());

    table
        .manageSnapshots()
        .createBranch("branch1", currentSnapshot)
        .createTag("tag1", currentSnapshot)
        .cherrypick(staged.snapshotId())
        .commit();

    TableMetadata current = table.ops().current();
    Assert.assertEquals(current.currentSnapshot().snapshotId(), 2);
    SnapshotRef actualTag = current.ref("tag1");
    SnapshotRef actualBranch = current.ref("branch1");
    Assert.assertEquals(2, current.currentSnapshot().snapshotId());
    Assert.assertEquals(SnapshotRef.branchBuilder(1).build(), actualBranch);
    Assert.assertEquals(SnapshotRef.tagBuilder(1).build(), actualTag);
  }

  @Test
  public void testAttemptToRollbackToCurrentSnapshot() {
    table.newAppend().appendFile(FILE_A).commit();

    long currentSnapshotTimestampPlus100 = table.currentSnapshot().timestampMillis() + 100;
    table.manageSnapshots().rollbackToTime(currentSnapshotTimestampPlus100).commit();

    long currentSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().rollbackTo(currentSnapshotId).commit();
  }

  @Test
  public void testSnapshotManagerThroughTransaction() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotAfterFirstAppend = readMetadata().currentSnapshot();
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);

    table.newAppend().appendFile(FILE_B).commit();
    validateSnapshot(snapshotAfterFirstAppend, readMetadata().currentSnapshot(), FILE_B);
    Assert.assertEquals("Table should be on version 2 after appending twice", 2, (int) version());

    TableMetadata base = readMetadata();
    Transaction txn = table.newTransaction();

    Assert.assertSame(
        "Base metadata should not change when transaction is created", base, readMetadata());
    Assert.assertEquals(
        "Table should be on version 2 after creating transaction", 2, (int) version());

    ManageSnapshots manageSnapshots = txn.manageSnapshots();
    Assert.assertNotNull(manageSnapshots);

    Assert.assertSame(
        "Base metadata should not change when manageSnapshots is created", base, readMetadata());
    Assert.assertEquals(
        "Table should be on version 2 after creating manageSnapshots", 2, (int) version());

    manageSnapshots.rollbackTo(snapshotAfterFirstAppend.snapshotId()).commit();

    Assert.assertSame(
        "Base metadata should not change when invoking rollbackTo", base, readMetadata());
    Assert.assertEquals(
        "Table should be on version 2 after invoking rollbackTo", 2, (int) version());

    txn.commitTransaction();

    Assert.assertEquals(snapshotAfterFirstAppend, readMetadata().currentSnapshot());
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);
    Assert.assertEquals(
        "Table should be on version 3 after invoking rollbackTo", 3, (int) version());
  }

  @Test
  public void testSnapshotManagerThroughTransactionMultiOperation() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotAfterFirstAppend = readMetadata().currentSnapshot();
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);

    table.newAppend().appendFile(FILE_B).commit();
    validateSnapshot(snapshotAfterFirstAppend, readMetadata().currentSnapshot(), FILE_B);
    Assert.assertEquals("Table should be on version 2 after appending twice", 2, (int) version());

    TableMetadata base = readMetadata();
    Transaction txn = table.newTransaction();

    txn.manageSnapshots().rollbackTo(snapshotAfterFirstAppend.snapshotId()).commit();
    txn.updateProperties().set("some_prop", "some_prop_value").commit();
    Assert.assertSame(
        "Base metadata should not change when transaction is not committed", base, readMetadata());
    Assert.assertEquals(
        "Table should remain on version 2 when transaction is not committed", 2, (int) version());

    txn.commitTransaction();

    Assert.assertEquals(snapshotAfterFirstAppend, readMetadata().currentSnapshot());
    Assert.assertEquals(
        "Table should be on version 3 after invoking rollbackTo", 3, (int) version());
  }

  @Test
  public void testSnapshotManagerInvalidParameters() throws Exception {
    Assert.assertThrows(
        "Incorrect input transaction: null",
        IllegalArgumentException.class,
        () -> {
          new SnapshotManager(null);
        });
  }
}
