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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotManager extends TestBase {

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

  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testCherryPickDynamicOverwrite() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();

    // pick the snapshot into the current state
    table.manageSnapshots().cherrypick(staged.snapshotId()).commit();

    assertThat(table.currentSnapshot().snapshotId())
        .as("Should not fast-forward")
        .isNotEqualTo(staged.snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @TestTemplate
  public void testCherryPickDynamicOverwriteWithoutParent() {
    assertThat(table.currentSnapshot()).isNull();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();

    // pick the snapshot into the current state
    table.manageSnapshots().cherrypick(staged.snapshotId()).commit();

    assertThat(table.currentSnapshot().snapshotId())
        .as("Should not fast-forward")
        .isNotEqualTo(staged.snapshotId());
    validateTableFiles(table, FILE_B, REPLACEMENT_FILE_A);
  }

  @TestTemplate
  public void testCherryPickDynamicOverwriteConflict() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(CONFLICT_FILE_A).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot cherry-pick replace partitions with changed partition");

    assertThat(table.currentSnapshot().snapshotId())
        .as("Failed cherry-pick should not change the table state")
        .isEqualTo(lastSnapshotId);
    validateTableFiles(table, FILE_A, CONFLICT_FILE_A);
  }

  @TestTemplate
  public void testCherryPickDynamicOverwriteDeleteConflict() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite that replaces FILE_A
    table.newReplacePartitions().addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);

    // add FILE_B s
    table.newAppend().appendFile(FILE_B).commit();

    // delete FILE_A so the replace operation is no longer valid
    table.newDelete().deleteFile(FILE_A).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Missing required files to delete");

    assertThat(table.currentSnapshot().snapshotId())
        .as("Failed cherry-pick should not change the table state")
        .isEqualTo(lastSnapshotId);
    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
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
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(replaceSnapshotId).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Cannot cherry-pick overwrite not based on an ancestor of the current state");

    assertThat(table.currentSnapshot().snapshotId())
        .as("Failed cherry-pick should not change the table state")
        .isEqualTo(lastSnapshotId);
    validateTableFiles(table, FILE_A);
  }

  @TestTemplate
  public void testCherryPickOverwrite() {
    table.newAppend().appendFile(FILE_A).commit();

    // stage an overwrite to replace FILE_A
    table.newOverwrite().deleteFile(FILE_A).addFile(REPLACEMENT_FILE_A).stageOnly().commit();

    Snapshot staged = Iterables.getLast(table.snapshots());
    assertThat(staged.operation())
        .as("Should find the staged overwrite snapshot")
        .isEqualTo(DataOperations.OVERWRITE);

    // add another append so that the original commit can't be fast-forwarded
    table.newAppend().appendFile(FILE_B).commit();
    long lastSnapshotId = table.currentSnapshot().snapshotId();

    // pick the snapshot into the current state
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(staged.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageEndingWith("not append, dynamic overwrite, or fast-forward");

    assertThat(table.currentSnapshot().snapshotId())
        .as("Failed cherry-pick should not change the table state")
        .isEqualTo(lastSnapshotId);
    validateTableFiles(table, FILE_A, FILE_B);
  }

  @TestTemplate
  public void testCreateBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a branch
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    SnapshotRef expectedBranch = table.ops().refresh().ref("branch1");
    assertThat(expectedBranch).isNotNull();
    assertThat(expectedBranch).isEqualTo(SnapshotRef.branchBuilder(snapshotId).build());
  }

  @TestTemplate
  public void testCreateBranchWithoutSnapshotId() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a branch
    table.manageSnapshots().createBranch("branch1").commit();
    SnapshotRef actualBranch = table.ops().refresh().ref("branch1");
    assertThat(actualBranch).isNotNull();
    assertThat(actualBranch).isEqualTo(SnapshotRef.branchBuilder(snapshotId).build());
  }

  @TestTemplate
  public void testCreateBranchOnEmptyTable() {
    table.manageSnapshots().createBranch("branch1").commit();

    SnapshotRef mainSnapshotRef = table.ops().refresh().ref(SnapshotRef.MAIN_BRANCH);
    assertThat(mainSnapshotRef).isNull();

    SnapshotRef branch1SnapshotRef = table.ops().refresh().ref("branch1");
    assertThat(branch1SnapshotRef).isNotNull();
    assertThat(branch1SnapshotRef.minSnapshotsToKeep()).isNull();
    assertThat(branch1SnapshotRef.maxSnapshotAgeMs()).isNull();
    assertThat(branch1SnapshotRef.maxRefAgeMs()).isNull();

    Snapshot snapshot = table.snapshot(branch1SnapshotRef.snapshotId());
    assertThat(snapshot.parentId()).isNull();
    assertThat(snapshot.addedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDataFiles(table.io())).isEmpty();
    assertThat(snapshot.addedDeleteFiles(table.io())).isEmpty();
    assertThat(snapshot.removedDeleteFiles(table.io())).isEmpty();
  }

  @TestTemplate
  public void testCreateBranchOnEmptyTableFailsWhenRefAlreadyExists() {
    table.manageSnapshots().createBranch("branch1").commit();

    // Trying to create a branch with an existing name should fail
    assertThatThrownBy(() -> table.manageSnapshots().createBranch("branch1").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch1 already exists");

    // Trying to create another branch within the same chain
    assertThatThrownBy(
            () -> table.manageSnapshots().createBranch("branch2").createBranch("branch2").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch2 already exists");
  }

  @TestTemplate
  public void testCreateBranchFailsWhenRefAlreadyExists() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    // Trying to create a branch with an existing name should fail
    assertThatThrownBy(() -> table.manageSnapshots().createBranch("branch1", snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch1 already exists");

    // Trying to create another branch within the same chain
    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createBranch("branch2", snapshotId)
                    .createBranch("branch2", snapshotId)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref branch2 already exists");
  }

  @TestTemplate
  public void testCreateTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating a tag
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    SnapshotRef expectedTag = table.ops().refresh().ref("tag1");

    assertThat(expectedTag).isNotNull();
    assertThat(expectedTag).isEqualTo(SnapshotRef.tagBuilder(snapshotId).build());
  }

  @TestTemplate
  public void testCreateTagFailsWhenRefAlreadyExists() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("tag1", snapshotId).commit();

    // Trying to create a tag with an existing name should fail
    assertThatThrownBy(() -> table.manageSnapshots().createTag("tag1", snapshotId).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref tag1 already exists");

    // Trying to create another tag within the same chain
    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag2", snapshotId)
                    .createTag("tag2", snapshotId)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref tag2 already exists");
  }

  @TestTemplate
  public void testRemoveBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating and then removing a branch and tag
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().removeBranch("branch1").commit();

    TableMetadata updated = table.ops().refresh();
    SnapshotRef expectedBranch = updated.ref("branch1");
    assertThat(expectedBranch).isNull();

    // Test chained creating and removal of branch and tag
    table.manageSnapshots().createBranch("branch2", snapshotId).removeBranch("branch2").commit();
    updated = table.ops().refresh();
    assertThat(updated.ref("branch2")).isNull();
  }

  @TestTemplate
  public void testRemovingNonExistingBranchFails() {
    assertThatThrownBy(() -> table.manageSnapshots().removeBranch("non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: non-existing");
  }

  @TestTemplate
  public void testRemovingMainBranchFails() {
    assertThatThrownBy(() -> table.manageSnapshots().removeBranch(SnapshotRef.MAIN_BRANCH).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove main branch");
  }

  @TestTemplate
  public void testRemoveTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    // Test a basic case of creating and then removing a branch and tag
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    table.manageSnapshots().removeTag("tag1").commit();
    TableMetadata updated = table.ops().refresh();
    SnapshotRef expectedTag = updated.ref("tag1");
    assertThat(expectedTag).isNull();

    // Test chained creating and removal of a tag
    table.manageSnapshots().createTag("tag2", snapshotId).removeTag("tag2").commit();
    assertThat(table.ops().refresh()).isEqualTo(updated);
    assertThat(updated.ref("tag2")).isNull();
  }

  @TestTemplate
  public void testRemovingNonExistingTagFails() {
    assertThatThrownBy(() -> table.manageSnapshots().removeTag("non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tag does not exist: non-existing");
  }

  @TestTemplate
  public void testReplaceBranch() {
    table.newAppend().appendFile(FILE_A).set("wap.id", "123").stageOnly().commit();
    Snapshot firstSnapshot = Iterables.getOnlyElement(table.snapshots());
    table.manageSnapshots().createBranch("branch1", firstSnapshot.snapshotId()).commit();
    table.newAppend().appendFile(FILE_B).set("wap.id", "456").stageOnly().commit();
    Snapshot secondSnapshot = Iterables.get(table.snapshots(), 1);
    table.manageSnapshots().createBranch("branch2", secondSnapshot.snapshotId()).commit();
    table.manageSnapshots().replaceBranch("branch1", "branch2").commit();
    assertThat(secondSnapshot.snapshotId())
        .isEqualTo(table.ops().refresh().ref("branch1").snapshotId());
  }

  @TestTemplate
  public void testReplaceBranchNonExistingToBranchFails() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    assertThatThrownBy(
            () -> table.manageSnapshots().replaceBranch("branch1", "non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref does not exist: non-existing");
  }

  @TestTemplate
  public void testFastForwardBranchNonExistingFromBranchCreatesTheBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().fastForwardBranch("new-branch", "branch1").commit();

    assertThat(table.ops().current().ref("new-branch").isBranch()).isTrue();
    assertThat(table.ops().current().ref("new-branch").snapshotId()).isEqualTo(snapshotId);
  }

  @TestTemplate
  public void testReplaceBranchNonExistingFromBranchCreatesTheBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().replaceBranch("new-branch", "branch1").commit();

    assertThat(table.ops().current().ref("new-branch").isBranch()).isTrue();
    assertThat(table.ops().current().ref("new-branch").snapshotId()).isEqualTo(snapshotId);
  }

  @TestTemplate
  public void testFastForwardBranchNonExistingToFails() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    assertThatThrownBy(
            () -> table.manageSnapshots().fastForwardBranch("branch1", "non-existing").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Ref does not exist: non-existing");
  }

  @TestTemplate
  public void testFastForward() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(1);

    table.manageSnapshots().createBranch("new-branch-at-staged-snapshot", 2).commit();
    table
        .manageSnapshots()
        .fastForwardBranch(SnapshotRef.MAIN_BRANCH, "new-branch-at-staged-snapshot")
        .commit();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(2);
  }

  @TestTemplate
  public void testFastForwardWhenFromIsNotAncestorFails() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    long snapshot = table.currentSnapshot().snapshotId();

    // Commit a snapshot on main to deviate the branches
    table.newAppend().appendFile(FILE_C).commit();

    final String newBranch = "new-branch-at-staged-snapshot";
    table.manageSnapshots().createBranch(newBranch, snapshot).commit();

    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .fastForwardBranch(SnapshotRef.MAIN_BRANCH, newBranch)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot fast-forward: main is not an ancestor of new-branch-at-staged-snapshot");
  }

  @TestTemplate
  public void testReplaceTag() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    // Create a new snapshot and replace the tip of branch1 to be the new snapshot
    table.newAppend().appendFile(FILE_B).commit();
    long currentSnapshot = table.ops().refresh().currentSnapshot().snapshotId();
    table.manageSnapshots().replaceTag("tag1", currentSnapshot).commit();
    assertThat(currentSnapshot).isEqualTo(table.ops().refresh().ref("tag1").snapshotId());
  }

  @TestTemplate
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
    assertThat(updated.ref("branch1").maxSnapshotAgeMs()).isEqualTo(20000);
    assertThat(updated.ref("branch1").minSnapshotsToKeep()).isEqualTo(10);
    // Test creating and updating in a chain
    table
        .manageSnapshots()
        .createBranch("branch2", snapshotId)
        .setMinSnapshotsToKeep("branch2", 10)
        .setMaxSnapshotAgeMs("branch2", 20000)
        .commit();
    updated = table.ops().refresh();
    assertThat(updated.ref("branch2").maxSnapshotAgeMs()).isEqualTo(20000);
    assertThat(updated.ref("branch2").minSnapshotsToKeep()).isEqualTo(10);
  }

  @TestTemplate
  public void testSettingBranchRetentionOnTagFails() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag1", snapshotId)
                    .setMinSnapshotsToKeep("tag1", 10)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting minSnapshotsToKeep");

    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .createTag("tag1", snapshotId)
                    .setMaxSnapshotAgeMs("tag1", 10)
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Tags do not support setting maxSnapshotAgeMs");
  }

  @TestTemplate
  public void testUpdatingBranchMaxRefAge() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    final long maxRefAgeMs = 10000;

    // Test creating and updating independently
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().setMaxRefAgeMs("branch1", 10000).commit();
    TableMetadata updated = table.ops().refresh();
    assertThat(updated.ref("branch1").maxRefAgeMs()).isEqualTo(maxRefAgeMs);
  }

  @TestTemplate
  public void testUpdatingTagMaxRefAge() {
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    final long maxRefAgeMs = 10000;

    // Test creating and updating independently
    table.manageSnapshots().createTag("tag1", snapshotId).commit();
    table.manageSnapshots().setMaxRefAgeMs("tag1", maxRefAgeMs).commit();

    TableMetadata updated = table.ops().refresh();
    assertThat(updated.ref("tag1").maxRefAgeMs()).isEqualTo(maxRefAgeMs);

    // Test creating and updating in a chain
    table
        .manageSnapshots()
        .createTag("tag2", snapshotId)
        .setMaxRefAgeMs("tag2", maxRefAgeMs)
        .commit();
    updated = table.ops().refresh();
    assertThat(updated.ref("tag2").maxRefAgeMs()).isEqualTo(maxRefAgeMs);
  }

  @TestTemplate
  public void testRenameBranch() {
    table.newAppend().appendFile(FILE_A).commit();
    table.newAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();

    // Test creating and renaming independently
    table.manageSnapshots().createBranch("branch1", snapshotId).commit();
    table.manageSnapshots().renameBranch("branch1", "branch2").commit();
    TableMetadata updated = table.ops().refresh();
    assertThat(updated.ref("branch1")).isNull();
    assertThat(SnapshotRef.branchBuilder(snapshotId).build()).isEqualTo(updated.ref("branch2"));

    table
        .manageSnapshots()
        .createBranch("branch3", snapshotId)
        .renameBranch("branch3", "branch4")
        .commit();

    updated = table.ops().refresh();
    assertThat(updated.ref("branch3")).isNull();
    assertThat(SnapshotRef.branchBuilder(snapshotId).build()).isEqualTo(updated.ref("branch4"));
  }

  @TestTemplate
  public void testFailRenamingMainBranch() {
    assertThatThrownBy(
            () ->
                table
                    .manageSnapshots()
                    .renameBranch(SnapshotRef.MAIN_BRANCH, "some-branch")
                    .commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename main branch");
  }

  @TestTemplate
  public void testRenamingNonExistingBranchFails() {
    assertThatThrownBy(
            () ->
                table.manageSnapshots().renameBranch("some-missing-branch", "some-branch").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Branch does not exist: some-missing-branch");
  }

  @TestTemplate
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
    assertThat(current.currentSnapshot().snapshotId()).isEqualTo(1);
    SnapshotRef actualTag = current.ref("tag1");
    SnapshotRef actualBranch = current.ref("branch1");
    assertThat(current.currentSnapshot().snapshotId()).isEqualTo(1);
    assertThat(actualBranch).isEqualTo(SnapshotRef.branchBuilder(snapshotPriorToRollback).build());
    assertThat(actualTag).isEqualTo(SnapshotRef.tagBuilder(snapshotPriorToRollback).build());
  }

  @TestTemplate
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
    assertThat(current.currentSnapshot().snapshotId()).isEqualTo(2);
    SnapshotRef actualTag = current.ref("tag1");
    SnapshotRef actualBranch = current.ref("branch1");
    assertThat(current.currentSnapshot().snapshotId()).isEqualTo(2);
    assertThat(actualBranch).isEqualTo(SnapshotRef.branchBuilder(1).build());
    assertThat(actualTag).isEqualTo(SnapshotRef.tagBuilder(1).build());
  }

  @TestTemplate
  public void testAttemptToRollbackToCurrentSnapshot() {
    table.newAppend().appendFile(FILE_A).commit();

    long currentSnapshotTimestampPlus100 = table.currentSnapshot().timestampMillis() + 100;
    table.manageSnapshots().rollbackToTime(currentSnapshotTimestampPlus100).commit();

    long currentSnapshotId = table.currentSnapshot().snapshotId();
    table.manageSnapshots().rollbackTo(currentSnapshotId).commit();
  }

  @TestTemplate
  public void testSnapshotManagerThroughTransaction() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotAfterFirstAppend = readMetadata().currentSnapshot();
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);

    table.newAppend().appendFile(FILE_B).commit();
    validateSnapshot(snapshotAfterFirstAppend, readMetadata().currentSnapshot(), FILE_B);
    assertThat(version()).as("Table should be on version 2 after appending twice").isEqualTo(2);

    TableMetadata base = readMetadata();
    Transaction txn = table.newTransaction();

    assertThat(readMetadata())
        .as("Base metadata should not change when transaction is created")
        .isEqualTo(base);
    assertThat(version())
        .as("Table should be on version 2 after creating transaction")
        .isEqualTo(2);

    ManageSnapshots manageSnapshots = txn.manageSnapshots();
    assertThat(manageSnapshots).isNotNull();

    assertThat(readMetadata())
        .as("Base metadata should not change when manageSnapshots is created")
        .isEqualTo(base);
    assertThat(version())
        .as("Table should be on version 2 after creating manageSnapshots")
        .isEqualTo(2);

    manageSnapshots.rollbackTo(snapshotAfterFirstAppend.snapshotId()).commit();

    assertThat(readMetadata())
        .as("Base metadata should not change when invoking rollbackTo")
        .isEqualTo(base);
    assertThat(version()).as("Table should be on version 2 after invoking rollbackTo").isEqualTo(2);

    txn.commitTransaction();

    assertThat(readMetadata().currentSnapshot()).isEqualTo(snapshotAfterFirstAppend);
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);
    assertThat(version()).as("Table should be on version 3 after invoking rollbackTo").isEqualTo(3);
  }

  @TestTemplate
  public void testSnapshotManagerThroughTransactionMultiOperation() {
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot snapshotAfterFirstAppend = readMetadata().currentSnapshot();
    validateSnapshot(null, snapshotAfterFirstAppend, FILE_A);

    table.newAppend().appendFile(FILE_B).commit();
    validateSnapshot(snapshotAfterFirstAppend, readMetadata().currentSnapshot(), FILE_B);
    assertThat(version()).as("Table should be on version 2 after appending twice").isEqualTo(2);

    TableMetadata base = readMetadata();
    Transaction txn = table.newTransaction();

    txn.manageSnapshots().rollbackTo(snapshotAfterFirstAppend.snapshotId()).commit();
    txn.updateProperties().set("some_prop", "some_prop_value").commit();
    assertThat(readMetadata())
        .as("Base metadata should not change when transaction is not committed")
        .isEqualTo(base);
    assertThat(version())
        .as("Table should remain on version 2 when transaction is not committed")
        .isEqualTo(2);

    txn.commitTransaction();

    assertThat(readMetadata().currentSnapshot()).isEqualTo(snapshotAfterFirstAppend);
    assertThat(version()).as("Table should be on version 3 after invoking rollbackTo").isEqualTo(3);
  }

  @TestTemplate
  public void testSnapshotManagerInvalidParameters() {
    assertThatThrownBy(() -> new SnapshotManager(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid input transaction: null");
  }
}
