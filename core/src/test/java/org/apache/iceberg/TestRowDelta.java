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

import static org.apache.iceberg.SnapshotSummary.ADDED_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.ADDED_POS_DELETES_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_COUNT_PROP;
import static org.apache.iceberg.SnapshotSummary.CHANGED_PARTITION_PREFIX;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DATA_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_DELETE_FILES_PROP;
import static org.apache.iceberg.SnapshotSummary.TOTAL_POS_DELETES_PROP;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRowDelta extends TestBase {

  @Parameter(index = 1)
  private String branch;

  @Parameters(name = "formatVersion = {0}, branch = {1}")
  protected static List<Object> parameters() {
    return TestHelpers.V2_AND_ABOVE.stream()
        .flatMap(v -> Stream.of(new Object[] {v, "main"}, new Object[] {v, "testBranch"}))
        .collect(Collectors.toList());
  }

  @TestTemplate
  public void addOnlyDeleteFilesProducesDeleteOperation() {
    SnapshotUpdate<?> rowDelta =
        table.newRowDelta().addDeletes(fileADeletes()).addDeletes(fileBDeletes());

    commit(table, rowDelta, branch);
    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(1);
    assertThat(snap.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snap.deleteManifests(table.io())).hasSize(1);
  }

  @TestTemplate
  public void testAddRemoveRows() {
    SnapshotUpdate<?> rowDelta =
        table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()).addDeletes(fileBDeletes());

    commit(table, rowDelta, branch);
    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);
    assertThat(snap.operation())
        .as("Delta commit should use operation 'overwrite'")
        .isEqualTo(DataOperations.OVERWRITE);
    assertThat(snap.dataManifests(table.io())).hasSize(1);

    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.ADDED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(fileADeletes(), fileBDeletes()),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testValidateDataFilesExistDefaults() {
    SnapshotUpdate<?> rowDelta1 = table.newAppend().appendFile(FILE_A).appendFile(FILE_B);

    commit(table, rowDelta1, branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A
    SnapshotUpdate<?> rowDelta2 = table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2);

    commit(table, rowDelta2, branch);

    // delete FILE_B
    SnapshotUpdate<?> rowDelta3 = table.newDelete().deleteFile(FILE_B);

    commit(table, rowDelta3, branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(deleteSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(fileBDeletes())
            .validateDataFilesExist(ImmutableList.of(FILE_B.location()))
            .validateFromSnapshot(validateFromSnapshotId),
        branch);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).hasSize(1);
    ManifestFile deletes = latestSnapshot(table, branch).deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(latestSnapshot(table, branch).snapshotId()),
        files(fileBDeletes()),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testValidateDataFilesExistOverwrite() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A
    commit(table, table.newOverwrite().deleteFile(FILE_A).addFile(FILE_A2), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(deleteSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testValidateDataFilesExistReplacePartitions() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A's partition
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(deleteSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testValidateDataFilesExistFromSnapshot() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    // overwrite FILE_A's partition
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    // test changes to the table back to the snapshot where FILE_A was overwritten
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();
    long replaceSnapshotId = latestSnapshot(table, branch).snapshotId();

    // even though FILE_A was deleted, it happened before the "from" snapshot, so the validation
    // allows it
    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(fileADeletes())
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(3);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(3);

    assertThat(snap.dataManifests(table.io())).hasSize(2);
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(replaceSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A deleted
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(replaceSnapshotId, appendSnapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(snap.snapshotId()),
        files(fileADeletes()),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testFileDeleteAndRowDelete() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);
    long initialCommit = latestSnapshot(table, branch).snapshotId();

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(fileADeletes())
            .addRows(FILE_A2)
            .removeRows(FILE_B)
            .validateFromSnapshot(initialCommit)
            .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(snap.dataManifests(table.io())).hasSize(2);
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A deleted
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(initialCommit, snap.snapshotId()),
        files(FILE_A, FILE_B),
        statuses(Status.EXISTING, Status.DELETED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snap.snapshotId()),
        files(fileADeletes()),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testValidateFileDeleteAndRowDelete() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);
    long initialCommit = latestSnapshot(table, branch).snapshotId();

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(fileBDeletes())
            .validateFromSnapshot(initialCommit)
            .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
        branch);

    assertThatThrownBy(
            () -> {
              commit(
                  table,
                  table
                      .newRowDelta()
                      .addDeletes(fileADeletes())
                      .addRows(FILE_A2)
                      .removeRows(FILE_B)
                      .validateFromSnapshot(initialCommit)
                      .validateNoConflictingDeleteFiles()
                      .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                  branch);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("found new delete for replaced data file: " + FILE_B);
  }

  @TestTemplate
  public void testValidateFileDeleteAndRowDeleteSameFile() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);
    long initialCommit = latestSnapshot(table, branch).snapshotId();

    // test adding a delete vector to a deleted file
    assertThatThrownBy(
            () -> {
              commit(
                  table,
                  table
                      .newRowDelta()
                      .addDeletes(fileADeletes())
                      .removeRows(FILE_A)
                      .validateFromSnapshot(initialCommit)
                      .validateDeletedFiles()
                      .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                  branch);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot delete data files")
        .hasMessageContaining(FILE_A.location());
  }

  @TestTemplate
  public void testValidateRemoveRows() {
    commit(table, table.newAppend().appendFile(FILE_B), branch);
    long initialCommit = latestSnapshot(table, branch).snapshotId();

    // Remove a file which does not exist
    assertThatThrownBy(
            () -> {
              commit(
                  table,
                  table
                      .newRowDelta()
                      .removeRows(FILE_A)
                      .validateFromSnapshot(initialCommit)
                      .validateDeletedFiles(),
                  branch);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Missing required files to delete:")
        .hasMessageContaining(FILE_A.location());

    // Should succeed if validation is ignored
    commit(
        table, table.newRowDelta().removeRows(FILE_A).validateFromSnapshot(initialCommit), branch);

    // Commit should be a no-op
    Snapshot snap = latestSnapshot(table, branch);
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(initialCommit),
        files(FILE_B),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testValidateDataFilesExistRewrite() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // rewrite FILE_A
    commit(
        table,
        table.newRewrite().rewriteFiles(Sets.newHashSet(FILE_A), Sets.newHashSet(FILE_A2)),
        branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(deleteSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testValidateDataFilesExistValidateDeletes() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    long deleteSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateDeletedFiles()
                        .validateFromSnapshot(validateFromSnapshotId)
                        .validateDataFilesExist(ImmutableList.of(FILE_A.location())),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(deleteSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testValidateNoConflicts() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    // test changes to the table back to the snapshot where FILE_A and FILE_B existed
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newAppend().appendFile(FILE_A2), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRowDelta()
                        .addDeletes(fileADeletes())
                        .validateFromSnapshot(validateFromSnapshotId)
                        .conflictDetectionFilter(
                            Expressions.equal("data", "u")) // bucket16("u") -> 0
                        .validateNoConflictingDataFiles(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");

    assertThat(latestSnapshot(table, branch).snapshotId())
        .as("Table state should not be modified by failed RowDelta operation")
        .isEqualTo(appendSnapshotId);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).isEmpty();
  }

  @TestTemplate
  public void testValidateNoConflictsFromSnapshot() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    long appendSnapshotId = latestSnapshot(table, branch).snapshotId();

    // delete FILE_A
    commit(table, table.newAppend().appendFile(FILE_A2), branch);

    // even though FILE_A2 was added, it happened before the "from" snapshot, so the validation
    // allows it
    long validateFromSnapshotId = latestSnapshot(table, branch).snapshotId();

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(fileADeletes())
            .validateDeletedFiles()
            .validateFromSnapshot(validateFromSnapshotId)
            .validateDataFilesExist(ImmutableList.of(FILE_A.location()))
            .conflictDetectionFilter(Expressions.equal("data", "u")) // bucket16("u") -> 0
            .validateNoConflictingDataFiles(),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(3);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(3);

    assertThat(snap.dataManifests(table.io())).hasSize(2);
    // manifest with FILE_A2 added
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(validateFromSnapshotId),
        files(FILE_A2),
        statuses(Status.ADDED));

    // manifest with FILE_A added
    validateManifest(
        snap.dataManifests(table.io()).get(1),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(appendSnapshotId),
        files(FILE_A),
        statuses(Status.ADDED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(snap.snapshotId()),
        files(fileADeletes()),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testOverwriteWithRemoveRows() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()).addDeletes(fileBDeletes()),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);

    // overwriting by a filter will also remove delete files that match because all matching data
    // files are removed.
    commit(
        table,
        table
            .newOverwrite()
            .overwriteByRowFilter(Expressions.equal(Expressions.bucket("data", 16), 0)),
        branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(snap.dataManifests(table.io())).hasSize(1);
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(fileADeletes(), fileBDeletes()),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testReplacePartitionsWithRemoveRows() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()).addDeletes(fileBDeletes()),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);

    // overwriting the partition will also remove delete files that match because all matching data
    // files are removed.
    commit(table, table.newReplacePartitions().addFile(FILE_A2), branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(snap.dataManifests(table.io())).hasSize(2);
    int deleteManifestPos = snap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        snap.dataManifests(table.io()).get(deleteManifestPos),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        snap.dataManifests(table.io()).get(appendManifestPos),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snap.snapshotId()),
        files(FILE_A2),
        statuses(Status.ADDED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), deltaSnapshotId),
        files(fileADeletes(), fileBDeletes()),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testDeleteByExpressionWithRemoveRows() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()).addDeletes(fileBDeletes()),
        branch);

    assertThat(latestSnapshot(table, branch).sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);

    // deleting with a filter will also remove delete files that match because all matching data
    // files are removed.
    commit(table, table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()), branch);

    Snapshot snap = latestSnapshot(table, branch);
    assertThat(snap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateManifest(
        snap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    assertThat(snap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        snap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snap.snapshotId(), snap.snapshotId()),
        files(fileADeletes(), fileBDeletes()),
        statuses(Status.DELETED, Status.DELETED));
  }

  @TestTemplate
  public void testDeleteDataFileWithRemoveRows() {
    commit(
        table,
        table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()).addDeletes(fileBDeletes()),
        branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);

    // deleting a specific data file will not affect a delete file in v2 or less
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    Snapshot deleteSnap = latestSnapshot(table, branch);
    assertThat(deleteSnap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(deleteSnap.deleteManifests(table.io())).hasSize(1);
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    Iterator<Long> ids =
        formatVersion >= 3
            ? ids(deleteSnap.snapshotId(), deltaSnapshotId)
            : ids(deltaSnapshotId, deltaSnapshotId);
    Iterator<Status> statuses =
        formatVersion >= 3
            ? statuses(Status.DELETED, Status.EXISTING)
            : statuses(Status.ADDED, Status.ADDED);
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids,
        files(fileADeletes(), fileBDeletes()),
        statuses);

    // the manifest that removed FILE_A will be dropped next commit, causing the min sequence number
    // of all data files to be 2, the largest known sequence number. This will cause FILE_A_DELETES
    // to be removed because it is too old to apply to any data files.
    commit(table, table.newRowDelta().removeDeletes(fileBDeletes()), branch);

    Snapshot nextSnap = latestSnapshot(table, branch);
    assertThat(nextSnap.sequenceNumber()).isEqualTo(3);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(3);

    assertThat(nextSnap.dataManifests(table.io())).isEmpty();
    assertThat(nextSnap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(nextSnap.snapshotId(), nextSnap.snapshotId()),
        formatVersion >= 3 ? files(fileBDeletes()) : files(fileADeletes(), fileBDeletes()),
        statuses(Status.DELETED, Status.DELETED));
  }

  @TestTemplate
  public void testFastAppendDoesNotRemoveStaleDeleteFiles() {
    commit(table, table.newRowDelta().addRows(FILE_A).addDeletes(fileADeletes()), branch);

    long deltaSnapshotId = latestSnapshot(table, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).sequenceNumber()).isEqualTo(1);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(1);

    // deleting a specific data file will not affect a delete file
    commit(table, table.newDelete().deleteFile(FILE_A), branch);

    Snapshot deleteSnap = latestSnapshot(table, branch);
    assertThat(deleteSnap.sequenceNumber()).isEqualTo(2);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(2);

    assertThat(deleteSnap.deleteManifests(table.io())).hasSize(1);
    validateManifest(
        deleteSnap.dataManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));

    assertThat(deleteSnap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        deleteSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(formatVersion >= 3 ? deleteSnap.snapshotId() : deltaSnapshotId),
        files(fileADeletes()),
        statuses(formatVersion >= 3 ? Status.DELETED : Status.ADDED));

    // the manifest that removed FILE_A will be dropped next merging commit, but FastAppend will not
    // remove it
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    Snapshot nextSnap = latestSnapshot(table, branch);
    assertThat(nextSnap.sequenceNumber()).isEqualTo(3);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(3);

    assertThat(nextSnap.dataManifests(table.io())).hasSize(2);
    int deleteManifestPos =
        nextSnap.dataManifests(table.io()).get(0).deletedFilesCount() > 0 ? 0 : 1;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(deleteManifestPos),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(deleteSnap.snapshotId()),
        files(FILE_A),
        statuses(Status.DELETED));
    int appendManifestPos = deleteManifestPos == 0 ? 1 : 0;
    validateManifest(
        nextSnap.dataManifests(table.io()).get(appendManifestPos),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(nextSnap.snapshotId()),
        files(FILE_B),
        statuses(Status.ADDED));

    assertThat(nextSnap.deleteManifests(table.io())).hasSize(1);
    validateDeleteManifest(
        nextSnap.deleteManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(formatVersion >= 3 ? deleteSnap.snapshotId() : deltaSnapshotId),
        files(fileADeletes()),
        statuses(formatVersion >= 3 ? Status.DELETED : Status.ADDED));
  }

  @TestTemplate
  public void testValidateDataFilesExistWithConflictDetectionFilter() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile2), branch);

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add a delete file for partition A
    DeleteFile deleteFile = newDeletes(dataFile1);

    Expression conflictDetectionFilter = Expressions.equal("data", "a");
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile)
            .validateDataFilesExist(ImmutableList.of(dataFile1.location()))
            .validateDeletedFiles()
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles();

    // concurrently delete the file for partition B
    commit(table, table.newDelete().deleteFile(dataFile2), branch);

    // commit the delta for partition A
    commit(table, rowDelta, branch);

    assertThat(latestSnapshot(table, branch).deleteManifests(table.io())).hasSize(1);
    ManifestFile deletes = latestSnapshot(table, branch).deleteManifests(table.io()).get(0);
    validateDeleteManifest(
        deletes,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(latestSnapshot(table, branch).snapshotId()),
        files(deleteFile),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testValidateDataFilesDoNotExistWithConflictDetectionFilter() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // use this snapshot as the starting snapshot in rowDelta
    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add a delete file for partition A
    DeleteFile deleteFile = newDeletes(dataFile1);

    Expression conflictDetectionFilter = Expressions.equal("data", "a");
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile)
            .validateDataFilesExist(ImmutableList.of(dataFile1.location()))
            .validateDeletedFiles()
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles();

    // concurrently delete the file for partition A
    commit(table, table.newDelete().deleteFile(dataFile1), branch);

    assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");
  }

  @TestTemplate
  public void testAddDeleteFilesMultipleSpecs() {
    // enable partition summaries
    table.updateProperties().set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "10").commit();

    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    assertThat(table.spec().isUnpartitioned()).isTrue();

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // evolve the spec and add a new partition field
    table.updateSpec().addField("data").commit();

    // append a data file with the new spec
    DataFile thirdSnapshotDataFile = newDataFile("data=abc");
    commit(table, table.newAppend().appendFile(thirdSnapshotDataFile), branch);

    assertThat(table.specs()).hasSize(3);

    // commit a row delta with 1 data file and 3 delete files where delete files have different
    // specs
    DataFile dataFile = newDataFile("data=xyz");
    DeleteFile firstDeleteFile = newDeletes(firstSnapshotDataFile);
    DeleteFile secondDeleteFile = newDeletes(secondSnapshotDataFile);
    DeleteFile thirdDeleteFile = newDeletes(thirdSnapshotDataFile);

    commit(
        table,
        table
            .newRowDelta()
            .addRows(dataFile)
            .addDeletes(firstDeleteFile)
            .addDeletes(secondDeleteFile)
            .addDeletes(thirdDeleteFile),
        branch);

    Snapshot snapshot = latestSnapshot(table, branch);
    assertThat(snapshot.sequenceNumber()).isEqualTo(4);
    assertThat(table.ops().current().lastSequenceNumber()).isEqualTo(4);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.OVERWRITE);

    Map<String, String> summary = snapshot.summary();
    long posDeletesCount = recordCount(firstDeleteFile, secondDeleteFile, thirdDeleteFile);

    assertThat(summary)
        .containsEntry(CHANGED_PARTITION_COUNT_PROP, "4")
        .containsEntry(ADDED_FILES_PROP, "1")
        .containsEntry(TOTAL_DATA_FILES_PROP, "4")
        .containsEntry(ADDED_DELETE_FILES_PROP, "3")
        .containsEntry(TOTAL_DELETE_FILES_PROP, "3")
        .containsEntry(ADDED_POS_DELETES_PROP, String.valueOf(posDeletesCount))
        .containsEntry(TOTAL_POS_DELETES_PROP, String.valueOf(posDeletesCount))
        .hasEntrySatisfying(
            CHANGED_PARTITION_PREFIX + "data_bucket=0",
            v -> assertThat(v).contains(ADDED_DELETE_FILES_PROP + "=1"))
        .hasEntrySatisfying(
            CHANGED_PARTITION_PREFIX + "data=abc",
            v -> assertThat(v).contains(ADDED_DELETE_FILES_PROP + "=1"))
        .hasEntrySatisfying(
            CHANGED_PARTITION_PREFIX + "data=xyz",
            v -> assertThat(v).contains(ADDED_FILES_PROP + "=1"));

    // 3 appends + 1 row delta
    assertThat(snapshot.dataManifests(table.io())).hasSize(4);
    validateManifest(
        snapshot.dataManifests(table.io()).get(0),
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(dataFile),
        statuses(Status.ADDED));

    // each delete file goes into a separate manifest as the specs are different
    assertThat(snapshot.deleteManifests(table.io())).hasSize(3);

    ManifestFile firstDeleteManifest = snapshot.deleteManifests(table.io()).get(2);
    assertThat(firstDeleteManifest.partitionSpecId()).isEqualTo(firstSnapshotDataFile.specId());
    validateDeleteManifest(
        firstDeleteManifest,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(firstDeleteFile),
        statuses(Status.ADDED));

    ManifestFile secondDeleteManifest = snapshot.deleteManifests(table.io()).get(1);
    assertThat(secondDeleteManifest.partitionSpecId()).isEqualTo(secondSnapshotDataFile.specId());
    validateDeleteManifest(
        secondDeleteManifest,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(secondDeleteFile),
        statuses(Status.ADDED));

    ManifestFile thirdDeleteManifest = snapshot.deleteManifests(table.io()).get(0);
    assertThat(thirdDeleteManifest.partitionSpecId()).isEqualTo(thirdSnapshotDataFile.specId());
    validateDeleteManifest(
        thirdDeleteManifest,
        dataSeqs(4L),
        fileSeqs(4L),
        ids(snapshot.snapshotId()),
        files(thirdDeleteFile),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testManifestMergingMultipleSpecs() {
    // make sure we enable manifest merging
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        .commit();

    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    assertThat(table.spec().isUnpartitioned()).isTrue();

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // commit two delete files to two specs in a single operation
    DeleteFile firstDeleteFile = newDeletes(firstSnapshotDataFile);
    DeleteFile secondDeleteFile = newDeletes(secondSnapshotDataFile);

    commit(
        table,
        table.newRowDelta().addDeletes(firstDeleteFile).addDeletes(secondDeleteFile),
        branch);

    Snapshot thirdSnapshot = latestSnapshot(table, branch);

    // 2 appends and 1 row delta where delete files belong to different specs
    assertThat(thirdSnapshot.dataManifests(table.io())).hasSize(2);
    assertThat(thirdSnapshot.deleteManifests(table.io())).hasSize(2);

    // commit two more delete files to the same specs to trigger merging
    DeleteFile thirdDeleteFile = newDeletes(firstSnapshotDataFile);
    DeleteFile fourthDeleteFile = newDeletes(secondSnapshotDataFile);

    commit(
        table,
        table
            .newRowDelta()
            .removeDeletes(firstDeleteFile)
            .addDeletes(thirdDeleteFile)
            .removeDeletes(secondDeleteFile)
            .addDeletes(fourthDeleteFile)
            .validateFromSnapshot(thirdSnapshot.snapshotId()),
        branch);

    Snapshot fourthSnapshot = latestSnapshot(table, branch);

    // make sure merging respects spec boundaries
    assertThat(fourthSnapshot.dataManifests(table.io())).hasSize(2);
    assertThat(fourthSnapshot.deleteManifests(table.io())).hasSize(2);

    ManifestFile firstDeleteManifest = fourthSnapshot.deleteManifests(table.io()).get(1);
    assertThat(firstDeleteManifest.partitionSpecId()).isEqualTo(firstSnapshotDataFile.specId());
    validateDeleteManifest(
        firstDeleteManifest,
        dataSeqs(4L, 3L),
        fileSeqs(4L, 3L),
        ids(fourthSnapshot.snapshotId(), fourthSnapshot.snapshotId()),
        files(thirdDeleteFile, firstDeleteFile),
        statuses(Status.ADDED, Status.DELETED));

    ManifestFile secondDeleteManifest = fourthSnapshot.deleteManifests(table.io()).get(0);
    assertThat(secondDeleteManifest.partitionSpecId()).isEqualTo(secondSnapshotDataFile.specId());
    validateDeleteManifest(
        secondDeleteManifest,
        dataSeqs(4L, 3L),
        fileSeqs(4L, 3L),
        ids(fourthSnapshot.snapshotId(), fourthSnapshot.snapshotId()),
        files(fourthDeleteFile, secondDeleteFile),
        statuses(Status.ADDED, Status.DELETED));
  }

  @TestTemplate
  public void testAbortMultipleSpecs() {
    // append a partitioned data file
    DataFile firstSnapshotDataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(firstSnapshotDataFile), branch);

    // remove the only partition field to make the spec unpartitioned
    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();

    assertThat(table.spec().isUnpartitioned()).isTrue();

    // append an unpartitioned data file
    DataFile secondSnapshotDataFile = newDataFile("");
    commit(table, table.newAppend().appendFile(secondSnapshotDataFile), branch);

    // prepare two delete files that belong to different specs
    DeleteFile firstDeleteFile = newDeletes(firstSnapshotDataFile);
    DeleteFile secondDeleteFile = newDeletes(secondSnapshotDataFile);

    // capture all deletes
    Set<String> deletedFiles = Sets.newHashSet();

    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addDeletes(firstDeleteFile)
            .addDeletes(secondDeleteFile)
            .deleteWith(deletedFiles::add)
            .validateDeletedFiles()
            .validateDataFilesExist(ImmutableList.of(firstSnapshotDataFile.location()));

    rowDelta.apply();

    // perform a conflicting concurrent operation
    commit(table, table.newDelete().deleteFile(firstSnapshotDataFile), branch);

    assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, missing data files");

    // we should clean up 1 manifest list and 2 delete manifests
    assertThat(deletedFiles).hasSize(3);
  }

  @TestTemplate
  public void testConcurrentConflictingRowDelta() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    Expression conflictDetectionFilter = Expressions.alwaysTrue();

    // mock a MERGE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addRows(FILE_B)
            .addDeletes(fileADeletes())
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    table
        .newRowDelta()
        .toBranch(branch)
        .addDeletes(fileADeletes())
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
  }

  @TestTemplate
  public void testConcurrentConflictingRowDeltaWithoutAppendValidation() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    Expression conflictDetectionFilter = Expressions.alwaysTrue();

    // mock a MERGE operation with snapshot isolation (i.e. no append validation)
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(fileADeletes())
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDeleteFiles();

    table
        .newRowDelta()
        .toBranch(branch)
        .addDeletes(fileADeletes())
        .validateFromSnapshot(firstSnapshot.snapshotId())
        .conflictDetectionFilter(conflictDetectionFilter)
        .validateNoConflictingDataFiles()
        .commit();

    assertThatThrownBy(() -> commit(table, rowDelta, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
  }

  @TestTemplate
  public void testConcurrentNonConflictingRowDelta() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-a.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=a")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    // add a data file to partition B
    DataFile dataFile2 =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-b.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data=b")
            .withRecordCount(1)
            .build();

    commit(table, table.newAppend().appendFile(dataFile2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    Expression conflictDetectionFilter = Expressions.equal("data", "a");

    // add a delete file for partition A
    DeleteFile deleteFile1 = newDeletes(dataFile1);

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // add a delete file for partition B
    DeleteFile deleteFile2 = newDeletes(dataFile2);

    table
        .newRowDelta()
        .toBranch(branch)
        .addDeletes(deleteFile2)
        .validateFromSnapshot(baseSnapshot.snapshotId())
        .commit();

    commit(table, rowDelta, branch);

    validateBranchDeleteFiles(table, branch, deleteFile1, deleteFile2);
  }

  @TestTemplate
  public void testConcurrentNonConflictingRowDeltaAndRewriteFilesWithSequenceNumber() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add an equality delete file
    DeleteFile deleteFile1 =
        newEqualityDeleteFile(
            table.spec().specId(), "data=a", table.schema().asStruct().fields().get(0).fieldId());

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .toBranch(branch)
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    commit(table, rowDelta, branch);
    commit(table, rewriteFiles, branch);

    validateBranchDeleteFiles(table, branch, deleteFile1);
    validateBranchFiles(table, branch, dataFile2);
  }

  @TestTemplate
  public void testRowDeltaAndRewriteFilesMergeManifestsWithSequenceNumber() {
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add an equality delete file
    DeleteFile deleteFile1 =
        newEqualityDeleteFile(
            table.spec().specId(), "data=a", table.schema().asStruct().fields().get(0).fieldId());

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    commit(table, rowDelta, branch);
    commit(table, rewriteFiles, branch);

    table.refresh();
    List<ManifestFile> dataManifests = latestSnapshot(table, branch).dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    ManifestFile mergedDataManifest = dataManifests.get(0);
    assertThat(mergedDataManifest.sequenceNumber()).isEqualTo(3);

    long currentSnapshotId = latestSnapshot(table, branch).snapshotId();

    validateManifest(
        mergedDataManifest,
        dataSeqs(1L, 1L),
        fileSeqs(3L, 1L),
        ids(currentSnapshotId, currentSnapshotId),
        files(dataFile2, dataFile1),
        statuses(Status.ADDED, Status.DELETED));
  }

  @TestTemplate
  public void testConcurrentConflictingRowDeltaAndRewriteFilesWithSequenceNumber() {
    // change the spec to be partitioned by data
    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.ref("data"))
        .commit();

    // add a data file to partition A
    DataFile dataFile1 = newDataFile("data=a");

    commit(table, table.newAppend().appendFile(dataFile1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    // add position deletes
    DeleteFile deleteFile1 = newDeletes(dataFile1);

    // mock a DELETE operation with serializable isolation
    RowDelta rowDelta =
        table
            .newRowDelta()
            .addDeletes(deleteFile1)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // mock a REWRITE operation with serializable isolation
    DataFile dataFile2 = newDataFile("data=a");

    RewriteFiles rewriteFiles =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(dataFile1),
                ImmutableSet.of(dataFile2),
                baseSnapshot.sequenceNumber())
            .validateFromSnapshot(baseSnapshot.snapshotId());

    commit(table, rowDelta, branch);

    assertThatThrownBy(() -> commit(table, rewriteFiles, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, found new position delete for replaced data file");
  }

  @TestTemplate
  public void testRowDeltaCaseSensitivity() {
    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_A2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    commit(table, table.newRowDelta().addDeletes(fileADeletes()), branch);

    Expression conflictDetectionFilter = Expressions.equal(Expressions.bucket("dAtA", 16), 0);

    assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtA'");

    assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .caseSensitive(true)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtA'");

    // binding should succeed and trigger the validation
    assertThatThrownBy(
            () ->
                table
                    .newRowDelta()
                    .toBranch(branch)
                    .caseSensitive(false)
                    .addRows(FILE_B)
                    .addDeletes(FILE_A2_DELETES)
                    .validateFromSnapshot(firstSnapshot.snapshotId())
                    .conflictDetectionFilter(conflictDetectionFilter)
                    .validateNoConflictingDataFiles()
                    .validateNoConflictingDeleteFiles()
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files");
  }

  @TestTemplate
  public void testRewrittenDeleteFiles() {
    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile deleteFile = newDeletes(dataFile);
    RowDelta baseRowDelta = table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    Snapshot baseSnapshot = commit(table, baseRowDelta, branch);
    assertThat(baseSnapshot.operation()).isEqualTo(DataOperations.OVERWRITE);

    DeleteFile newDeleteFile = newDeletes(dataFile);
    RowDelta rowDelta =
        table
            .newRowDelta()
            .removeDeletes(deleteFile)
            .addDeletes(newDeleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId());
    Snapshot snapshot = commit(table, rowDelta, branch);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);

    List<ManifestFile> dataManifests = snapshot.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseSnapshot.snapshotId()),
        files(dataFile),
        statuses(Status.ADDED));

    List<ManifestFile> deleteManifests = snapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snapshot.snapshotId()),
        files(newDeleteFile),
        statuses(Status.ADDED));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snapshot.snapshotId()),
        files(deleteFile),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testRewrittenDeleteFilesReadFromManifest() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);
    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile deleteFile = newDeleteFile(dataFile.specId(), "data_bucket=0");
    RowDelta baseRowDelta = table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    Snapshot baseSnapshot = commit(table, baseRowDelta, branch);
    assertThat(baseSnapshot.operation()).isEqualTo(DataOperations.OVERWRITE);
    List<ManifestFile> deleteManifests = baseSnapshot.deleteManifests(table.io());
    try (ManifestReader<DeleteFile> deleteReader =
        ManifestFiles.readDeleteManifest(deleteManifests.get(0), table.io(), table.specs())) {
      deleteFile = deleteReader.iterator().next();
    }

    assertThat(deleteFile.manifestLocation()).isEqualTo(deleteManifests.get(0).path());
    DeleteFile newDeleteFile = newDeleteFile(dataFile.specId(), "data_bucket=0");
    RowDelta rowDelta =
        table
            .newRowDelta()
            .removeDeletes(deleteFile)
            .addDeletes(newDeleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId());
    Snapshot snapshot = commit(table, rowDelta, branch);
    assertThat(snapshot.operation()).isEqualTo(DataOperations.DELETE);

    List<ManifestFile> dataManifests = snapshot.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseSnapshot.snapshotId()),
        files(dataFile),
        statuses(Status.ADDED));

    deleteManifests = snapshot.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snapshot.snapshotId()),
        files(newDeleteFile),
        statuses(Status.ADDED));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snapshot.snapshotId()),
        files(deleteFile),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testConcurrentDeletesRewriteSameRemoveRows() {
    assumeThat(formatVersion).isEqualTo(2);

    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile deleteFile = newDeletes(dataFile);
    RowDelta baseRowDelta = table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    Snapshot baseSnapshot = commit(table, baseRowDelta, branch);
    assertThat(baseSnapshot.operation()).isEqualTo(DataOperations.OVERWRITE);

    // commit the first DELETE operation that replaces `deleteFile`
    DeleteFile newDeleteFile1 = newDeletes(dataFile);
    RowDelta delete1 =
        table
            .newRowDelta()
            .addDeletes(newDeleteFile1)
            .removeDeletes(deleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles();
    Snapshot snapshot1 = commit(table, delete1, branch);
    assertThat(snapshot1.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot1.sequenceNumber()).isEqualTo(2L);

    // commit the second DELETE operation that replaces `deleteFile`
    DeleteFile newDeleteFile2 = newDeletes(dataFile);
    RowDelta delete2 =
        table
            .newRowDelta()
            .addDeletes(newDeleteFile2)
            .removeDeletes(deleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles();
    Snapshot snapshot2 = commit(table, delete2, branch);
    assertThat(snapshot2.operation()).isEqualTo(DataOperations.DELETE);
    assertThat(snapshot2.sequenceNumber()).isEqualTo(3L);

    List<ManifestFile> dataManifests = snapshot2.dataManifests(table.io());
    assertThat(dataManifests).hasSize(1);
    validateManifest(
        dataManifests.get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseSnapshot.snapshotId()),
        files(dataFile),
        statuses(Status.ADDED));

    // verify both new delete files have been added
    List<ManifestFile> deleteManifests = snapshot2.deleteManifests(table.io());
    assertThat(deleteManifests).hasSize(2);
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(snapshot2.snapshotId()),
        files(newDeleteFile2),
        statuses(Status.ADDED));
    validateDeleteManifest(
        deleteManifests.get(1),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(snapshot1.snapshotId()),
        files(newDeleteFile1),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testConcurrentManifestRewriteWithRemoveRowsRemoval() throws IOException {
    assumeThat(formatVersion).isEqualTo(2);
    // Manifest rewrite isn't supported on branches currently
    assumeThat(branch).isEqualTo("main");

    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile deleteFile = newDeleteFile(dataFile.specId(), "data_bucket=0");
    RowDelta rowDelta = table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    Snapshot first = commit(table, rowDelta, branch);

    DeleteFile secondDeleteFile = newDeleteFile(dataFile.specId(), "data_bucket=0");
    Snapshot secondRowDelta =
        commit(table, table.newRowDelta().addRows(dataFile).addDeletes(secondDeleteFile), branch);
    List<ManifestFile> secondRowDeltaDeleteManifests = secondRowDelta.deleteManifests(table.io());
    assertThat(secondRowDeltaDeleteManifests).hasSize(2);

    // Read the manifest entries before the manifest rewrite is committed
    List<ManifestEntry<DeleteFile>> readEntries = Lists.newArrayList();
    for (ManifestFile manifest : secondRowDeltaDeleteManifests) {
      try (ManifestReader<DeleteFile> deleteManifestReader =
          ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
        deleteManifestReader.entries().forEach(readEntries::add);
      }
    }

    RowDelta removeDeletes =
        table
            .newRowDelta()
            .removeDeletes(readEntries.get(0).file())
            .removeDeletes(readEntries.get(1).file())
            .validateFromSnapshot(secondRowDelta.snapshotId());

    RewriteManifests rewriteManifests =
        table
            .rewriteManifests()
            .addManifest(
                writeManifest(
                    "new_delete_manifest.avro",
                    // Specify data sequence number so that the delete files don't get aged out
                    // first
                    manifestEntry(
                        ManifestEntry.Status.EXISTING, first.snapshotId(), 3L, 0L, deleteFile),
                    manifestEntry(
                        ManifestEntry.Status.EXISTING,
                        secondRowDelta.snapshotId(),
                        3L,
                        0L,
                        secondDeleteFile)))
            .deleteManifest(secondRowDeltaDeleteManifests.get(0))
            .deleteManifest(secondRowDeltaDeleteManifests.get(1));
    commit(table, rewriteManifests, branch);

    Snapshot remove = commit(table, removeDeletes, branch);
    List<ManifestFile> deleteManifests = remove.deleteManifests(table.io());
    validateDeleteManifest(
        deleteManifests.get(0),
        dataSeqs(3L, 3L),
        fileSeqs(0L, 0L),
        ids(remove.snapshotId(), remove.snapshotId()),
        files(deleteFile, secondDeleteFile),
        statuses(Status.DELETED, Status.DELETED));
  }

  @TestTemplate
  public void testConcurrentMergeRewriteSameRemoveRows() {
    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile deleteFile = newDeletes(dataFile);
    RowDelta baseRowDelta = table.newRowDelta().addRows(dataFile).addDeletes(deleteFile);
    Snapshot baseSnapshot = commit(table, baseRowDelta, branch);
    assertThat(baseSnapshot.operation()).isEqualTo(DataOperations.OVERWRITE);

    // commit a DELETE operation that replaces `deleteFile`
    DeleteFile newDeleteFile1 = newDeletes(dataFile);
    RowDelta delete =
        table
            .newRowDelta()
            .addDeletes(newDeleteFile1)
            .removeDeletes(deleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles();
    commit(table, delete, branch);

    // attempt to commit a MERGE operation that replaces `deleteFile`
    DataFile newDataFile2 = newDataFile("data_bucket=0");
    DeleteFile newDeleteFile2 = newDeletes(dataFile);
    RowDelta merge =
        table
            .newRowDelta()
            .addRows(newDataFile2)
            .addDeletes(newDeleteFile2)
            .removeDeletes(deleteFile)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles();

    // MERGE must fail as DELETE could have deleted more positions
    assertThatThrownBy(() -> commit(table, merge, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete files that can apply");
  }

  @TestTemplate
  public void testConcurrentDVsForSameDataFile() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    DataFile dataFile = newDataFile("data_bucket=0");
    commit(table, table.newRowDelta().addRows(dataFile), branch);

    DeleteFile deleteFile1 = newDeletes(dataFile);
    RowDelta rowDelta1 = table.newRowDelta().addDeletes(deleteFile1);

    DeleteFile deleteFile2 = newDeletes(dataFile);
    RowDelta rowDelta2 = table.newRowDelta().addDeletes(deleteFile2);

    commit(table, rowDelta1, branch);

    assertThatThrownBy(() -> commit(table, rowDelta2, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Found concurrently added DV for %s", dataFile.location());
  }

  @TestTemplate
  public void testManifestMergingAfterUpgradeToV3() {
    assumeThat(formatVersion).isEqualTo(2);

    // enable manifest merging
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MERGE_ENABLED, "true")
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        .commit();

    // add a data file
    DataFile dataFile = newDataFile("data_bucket=0");
    commit(table, table.newAppend().appendFile(dataFile), branch);

    // commit a delete operation using a positional delete file
    DeleteFile deleteFile = newDeleteFileWithRef(dataFile);
    assertThat(deleteFile.format()).isEqualTo(FileFormat.PARQUET);
    RowDelta rowDelta1 = table.newRowDelta().addDeletes(deleteFile);
    Snapshot deleteFileSnapshot = commit(table, rowDelta1, branch);

    // upgrade the table
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    // commit a DV
    DeleteFile dv = newDV(dataFile);
    assertThat(dv.format()).isEqualTo(FileFormat.PUFFIN);
    RowDelta rowDelta2 = table.newRowDelta().addDeletes(dv);
    Snapshot dvSnapshot = commit(table, rowDelta2, branch);

    // both must be part of the table and merged into one manifest
    ManifestFile deleteManifest = Iterables.getOnlyElement(dvSnapshot.deleteManifests(table.io()));
    validateDeleteManifest(
        deleteManifest,
        dataSeqs(3L, 2L),
        fileSeqs(3L, 2L),
        ids(dvSnapshot.snapshotId(), deleteFileSnapshot.snapshotId()),
        files(dv, deleteFile),
        statuses(Status.ADDED, Status.EXISTING));

    // only the DV must be assigned during planning
    List<ScanTask> tasks = planFiles();
    FileScanTask task = Iterables.getOnlyElement(tasks).asFileScanTask();
    assertThat(task.deletes()).hasSize(1);
    DeleteFile taskDV = Iterables.getOnlyElement(task.deletes());
    assertThat(taskDV.location()).isEqualTo(dv.location());
    assertThat(taskDV.referencedDataFile()).isEqualTo(dv.referencedDataFile());
    assertThat(taskDV.contentOffset()).isEqualTo(dv.contentOffset());
    assertThat(taskDV.contentSizeInBytes()).isEqualTo(dv.contentSizeInBytes());
  }

  @TestTemplate
  public void testInabilityToAddPositionDeleteFilesInTablesWithDVs() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    DeleteFile deleteFile = newDeleteFile(table.spec().specId(), "data_bucket=0");
    assertThatThrownBy(() -> table.newRowDelta().addDeletes(deleteFile))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must use DVs for position deletes in V%s", formatVersion);
  }

  @TestTemplate
  public void testInabilityToAddDVToV2Tables() {
    assumeThat(formatVersion).isEqualTo(2);
    DataFile dataFile = newDataFile("data_bucket=0");
    DeleteFile dv = newDV(dataFile);
    assertThatThrownBy(() -> table.newRowDelta().addDeletes(dv))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Must not use DVs for position deletes in V2");
  }

  private List<ScanTask> planFiles() {
    try (CloseableIterable<ScanTask> tasks = table.newBatchScan().useRef(branch).planFiles()) {
      return Lists.newArrayList(tasks);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
