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
import org.apache.iceberg.exceptions.CherrypickAncestorCommitException;
import org.apache.iceberg.exceptions.DuplicateWAPCommitException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestWapWorkflow extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @BeforeEach
  public void setupTableProperties() {
    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
  }

  @TestTemplate
  public void testCherryPickOverwrite() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();

    // the overwrite should only be staged
    validateTableFiles(table, FILE_A);

    Snapshot overwrite =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get();

    // cherry-pick the overwrite; this works because it is a fast-forward commit
    table.manageSnapshots().cherrypick(overwrite.snapshotId()).commit();

    // the overwrite should only be staged
    validateTableFiles(table, FILE_B);
  }

  @TestTemplate
  public void testCherryPickOverwriteFailsIfCurrentHasChanged() {
    table.newAppend().appendFile(FILE_A).commit();

    table.newOverwrite().deleteFile(FILE_A).addFile(FILE_B).stageOnly().commit();

    // add a commit after the overwrite that prevents it from being a fast-forward operation
    table.newAppend().appendFile(FILE_C).commit();

    // the overwrite should only be staged
    validateTableFiles(table, FILE_A, FILE_C);

    Snapshot overwrite =
        Streams.stream(table.snapshots())
            .filter(snap -> DataOperations.OVERWRITE.equals(snap.operation()))
            .findFirst()
            .get();

    // try to cherry-pick, which should fail because the overwrite's parent is no longer current
    assertThatThrownBy(() -> table.manageSnapshots().cherrypick(overwrite.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot cherry-pick snapshot 2: not append, dynamic overwrite, or fast-forward");

    // the table state should not have changed
    validateTableFiles(table, FILE_A, FILE_C);
  }

  @TestTemplate
  public void testCurrentSnapshotOperation() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();
    base = readMetadata();

    Snapshot wapSnapshot = base.snapshots().get(1);

    assertThat(base.snapshots()).hasSize(2);
    assertThat(wapSnapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // do setCurrentSnapshot
    table.manageSnapshots().setCurrentSnapshot(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(wapSnapshot.snapshotId());
    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);
  }

  @TestTemplate
  public void testSetCurrentSnapshotNoWAP() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    Snapshot firstSnapshot = base.currentSnapshot();
    long firstSnapshotId = firstSnapshot.snapshotId();

    table.newAppend().appendFile(FILE_B).commit();

    // do setCurrentSnapshot
    table.manageSnapshots().setCurrentSnapshot(firstSnapshotId).commit();
    base = readMetadata();

    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(3);
  }

  @TestTemplate
  public void testRollbackOnInvalidNonAncestor() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();
    base = readMetadata();

    Snapshot wapSnapshot = base.snapshots().get(1);

    assertThat(base.snapshots()).hasSize(2);
    assertThat(wapSnapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // do rollback
    assertThatThrownBy(
            // rollback to snapshot that is not an ancestor
            () -> table.manageSnapshots().rollbackTo(wapSnapshot.snapshotId()).commit())
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot roll back to snapshot, not an ancestor of the current state: 2");
    base = readMetadata();

    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);
  }

  @TestTemplate
  public void testRollbackAndCherrypick() {
    // first snapshot
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    Snapshot firstSnapshot = base.currentSnapshot();
    long firstSnapshotId = firstSnapshot.snapshotId();

    // second snapshot
    table.newAppend().appendFile(FILE_B).commit();
    base = readMetadata();
    Snapshot secondSnapshot = base.currentSnapshot();

    // third snapshot
    table.newAppend().appendFile(FILE_C).commit();
    base = readMetadata();
    Snapshot thirdSnapshot = base.currentSnapshot();

    // rollback to first snapshot
    table.manageSnapshots().rollbackTo(firstSnapshotId).commit();
    base = readMetadata();
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshots()).hasSize(3);

    // fast forward to third snapshot
    table.manageSnapshots().cherrypick(thirdSnapshot.snapshotId()).commit();
    base = readMetadata();
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(4);

    // fast forward to 2nd snapshot
    table.manageSnapshots().cherrypick(secondSnapshot.snapshotId()).commit();
    base = readMetadata();
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(5);
    assertThat(base.snapshots()).hasSize(5);
  }

  @TestTemplate
  public void testRollbackToTime() {

    // first snapshot
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    Snapshot firstSnapshot = base.currentSnapshot();
    long firstSnapshotId = firstSnapshot.snapshotId();

    // second snapshot
    table.newAppend().appendFile(FILE_B).commit();
    base = readMetadata();
    Snapshot secondSnapshot = base.currentSnapshot();

    // third snapshot
    table.newAppend().appendFile(FILE_C).commit();

    // rollback to before the second snapshot's time
    table.manageSnapshots().rollbackToTime(secondSnapshot.timestampMillis()).commit();
    base = readMetadata();

    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshots()).hasSize(3);
  }

  @TestTemplate
  public void testWithCherryPicking() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    // first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = base.snapshots().get(1);

    assertThat(base.snapshots()).hasSize(2);
    assertThat(wapSnapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(wapSnapshot.snapshotId());
    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);
  }

  @TestTemplate
  public void testWithTwoPhaseCherryPicking() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    // second WAP commit
    table.newAppend().appendFile(FILE_C).set("wap.id", "987654321").stageOnly().commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);
    Snapshot wap2Snapshot = base.snapshots().get(2);

    assertThat(base.snapshots()).hasSize(3);
    assertThat(wap1Snapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(wap2Snapshot.summary()).containsEntry("wap.id", "987654321");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(wap1Snapshot.parentId()).isEqualTo(firstSnapshotId);
    assertThat(wap2Snapshot.parentId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(parentSnapshot.snapshotId() + 1);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().parentId())
        .as("Parent snapshot id should change to latest snapshot before commit")
        .isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick second snapshot
    table.manageSnapshots().cherrypick(wap2Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.currentSnapshot().snapshotId())
        .isEqualTo(parentSnapshot.snapshotId() + 1 /* one fast-forwarded snapshot */ + 1);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(3);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().parentId())
        .as("Parent snapshot id should change to latest snapshot before commit")
        .isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(3);
  }

  @TestTemplate
  public void testWithCommitsBetweenCherryPicking() {
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    // second WAP commit
    table.newAppend().appendFile(FILE_C).set("wap.id", "987654321").stageOnly().commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);
    Snapshot wap2Snapshot = base.snapshots().get(2);

    assertThat(base.snapshots()).hasSize(3);
    assertThat(wap1Snapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(wap2Snapshot.summary()).containsEntry("wap.id", "987654321");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(wap1Snapshot.parentId()).isEqualTo(firstSnapshotId);
    assertThat(wap2Snapshot.parentId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // load current snapshot
    parentSnapshot = base.currentSnapshot();

    // table has new commit
    table.newAppend().appendFile(FILE_D).commit();
    base = readMetadata();

    assertThat(base.snapshots()).hasSize(4);
    assertThat(base.currentSnapshot().parentId()).isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.snapshots()).hasSize(5);
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(parentSnapshot.snapshotId() + 1);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(3);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().parentId()).isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(3);

    // load current snapshot
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.manageSnapshots().cherrypick(wap2Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.snapshots()).hasSize(6);
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(parentSnapshot.snapshotId() + 1);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(4);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().parentId()).isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(4);
  }

  @TestTemplate
  public void testWithCherryPickingWithCommitRetry() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    // load current snapshot
    Snapshot parentSnapshot = base.currentSnapshot();
    long firstSnapshotId = parentSnapshot.snapshotId();

    // first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();

    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wap1Snapshot = base.snapshots().get(1);

    assertThat(base.snapshots()).hasSize(2);
    assertThat(wap1Snapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(wap1Snapshot.parentId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // load current snapshot
    base = readMetadata();
    parentSnapshot = base.currentSnapshot();
    // cherry-pick first snapshot
    table.ops().failCommits(3);
    table.manageSnapshots().cherrypick(wap1Snapshot.snapshotId()).commit();
    base = readMetadata();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(parentSnapshot.snapshotId() + 1);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.currentSnapshot().parentId()).isEqualTo(parentSnapshot.snapshotId());
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);
  }

  @TestTemplate
  public void testCherrypickingAncestor() {

    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    // first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot = base.snapshots().get(1);

    assertThat(base.snapshots()).hasSize(2);
    assertThat(wapSnapshot.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot.snapshotId()).commit();
    base = readMetadata();
    long wapPublishedId = table.currentSnapshot().snapshotId();

    // check if the effective current snapshot is set to the new snapshot created
    //   as a result of the cherry-pick operation
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(wapPublishedId);
    assertThat(base.snapshots()).hasSize(2);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);

    assertThatThrownBy(
            // duplicate cherry-pick snapshot
            () -> table.manageSnapshots().cherrypick(firstSnapshotId).commit())
        .isInstanceOf(CherrypickAncestorCommitException.class)
        .hasMessage("Cannot cherrypick snapshot 1: already an ancestor");
  }

  @TestTemplate
  public void testDuplicateCherrypick() {
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    // stage first WAP commit
    table.newAppend().appendFile(FILE_B).set("wap.id", "123456789").stageOnly().commit();
    // stage second WAP commit with same wap.id
    table.newAppend().appendFile(FILE_C).set("wap.id", "123456789").stageOnly().commit();
    base = readMetadata();

    // pick the snapshot that's staged but not committed
    Snapshot wapSnapshot1 = base.snapshots().get(1);
    Snapshot wapSnapshot2 = base.snapshots().get(2);

    assertThat(base.snapshots()).hasSize(3);
    assertThat(wapSnapshot1.summary()).containsEntry("wap.id", "123456789");
    assertThat(wapSnapshot2.summary()).containsEntry("wap.id", "123456789");
    assertThat(base.currentSnapshot().snapshotId()).isEqualTo(firstSnapshotId);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(1);

    // cherry-pick snapshot
    table.manageSnapshots().cherrypick(wapSnapshot1.snapshotId()).commit();
    base = readMetadata();

    assertThat(base.snapshots()).hasSize(3);
    assertThat(base.currentSnapshot().allManifests(table.io())).hasSize(2);
    assertThat(base.currentSnapshot().addedDataFiles(table.io())).hasSize(1);
    assertThat(base.snapshotLog())
        .as("Snapshot log should indicate number of snapshots committed")
        .hasSize(2);

    assertThatThrownBy(
            // duplicate cherry-pick snapshot
            () -> table.manageSnapshots().cherrypick(wapSnapshot2.snapshotId()).commit())
        .isInstanceOf(DuplicateWAPCommitException.class)
        .hasMessage(
            "Duplicate request to cherry pick wap id that was published already: 123456789");
  }

  @TestTemplate
  public void testNonWapCherrypick() {
    table.newAppend().appendFile(FILE_A).commit();
    TableMetadata base = readMetadata();
    long firstSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_B).commit();
    base = readMetadata();
    long secondSnapshotId = base.currentSnapshot().snapshotId();

    table.newAppend().appendFile(FILE_C).commit();
    base = readMetadata();
    long thirdSnapshotId = base.currentSnapshot().snapshotId();

    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(thirdSnapshotId);

    // NOOP commit
    table.manageSnapshots().commit();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(thirdSnapshotId);

    // Rollback to second snapshot
    table.manageSnapshots().rollbackTo(secondSnapshotId).commit();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(secondSnapshotId);

    // Cherrypick down to third
    table.manageSnapshots().cherrypick(thirdSnapshotId).commit();
    assertThat(table.currentSnapshot().snapshotId()).isEqualTo(3);

    // try double cherrypicking of the third snapshot
    assertThatThrownBy(
            // double cherrypicking of second snapshot
            () -> table.manageSnapshots().cherrypick(thirdSnapshotId).commit())
        .isInstanceOf(CherrypickAncestorCommitException.class)
        .hasMessage("Cannot cherrypick snapshot 3: already an ancestor");

    // try cherrypicking an ancestor
    assertThatThrownBy(
            // double cherrypicking of second snapshot
            () -> table.manageSnapshots().cherrypick(firstSnapshotId).commit())
        .isInstanceOf(CherrypickAncestorCommitException.class)
        .hasMessage(
            String.format("Cannot cherrypick snapshot %s: already an ancestor", firstSnapshotId));
  }
}
