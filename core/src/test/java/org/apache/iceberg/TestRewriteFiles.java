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

import static org.apache.iceberg.ManifestEntry.Status.ADDED;
import static org.apache.iceberg.ManifestEntry.Status.DELETED;
import static org.apache.iceberg.ManifestEntry.Status.EXISTING;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.internal.util.collections.Sets;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRewriteFiles extends TestBase {

  @Parameter(index = 1)
  private String branch;

  @Parameters(name = "formatVersion = {0}, branch = {1}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {1, "main"},
        new Object[] {1, "testBranch"},
        new Object[] {2, "main"},
        new Object[] {2, "testBranch"});
  }

  @TestTemplate
  public void testEmptyTable() {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.ref(branch)).isNull();

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-a.parquet");

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newRewrite()
                        .rewriteFiles(
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_A_DELETES),
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_B_DELETES)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-a-deletes.parquet");
  }

  @TestTemplate
  public void testAddOnly() {
    assertThat(listManifestFiles()).isEmpty();

    assertThatThrownBy(
            () ->
                apply(
                    table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Collections.emptySet()),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-a.parquet");

    assertThatThrownBy(
            () ->
                apply(
                    table
                        .newRewrite()
                        .rewriteFiles(
                            ImmutableSet.of(FILE_A),
                            ImmutableSet.of(),
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_A_DELETES)),
                    branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Delete files to add must be empty because there's no delete file to be rewritten");

    assertThatThrownBy(
            () ->
                apply(
                    table
                        .newRewrite()
                        .rewriteFiles(
                            ImmutableSet.of(FILE_A),
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_B),
                            ImmutableSet.of(FILE_B_DELETES)),
                    branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Delete files to add must be empty because there's no delete file to be rewritten");
  }

  @TestTemplate
  public void testDeleteOnly() {
    assertThat(listManifestFiles()).isEmpty();

    assertThatThrownBy(
            () ->
                apply(
                    table.newRewrite().rewriteFiles(Collections.emptySet(), Sets.newSet(FILE_A)),
                    branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Files to delete cannot be empty");

    assertThatThrownBy(
            () ->
                apply(
                    table
                        .newRewrite()
                        .rewriteFiles(
                            ImmutableSet.of(),
                            ImmutableSet.of(),
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_A_DELETES)),
                    branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Files to delete cannot be empty");

    assertThatThrownBy(
            () ->
                apply(
                    table
                        .newRewrite()
                        .rewriteFiles(
                            ImmutableSet.of(),
                            ImmutableSet.of(),
                            ImmutableSet.of(FILE_A),
                            ImmutableSet.of(FILE_A_DELETES)),
                    branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Files to delete cannot be empty");
  }

  @TestTemplate
  public void testDeleteWithDuplicateEntriesInManifest() {
    assertThat(listManifestFiles()).isEmpty();

    commit(
        table, table.newAppend().appendFile(FILE_A).appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseSnapshotId = latestSnapshot(base, branch).snapshotId();
    assertThat(latestSnapshot(base, branch).allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = latestSnapshot(base, branch).allManifests(table.io()).get(0);

    Snapshot pending =
        apply(table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C)), branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);
    assertThat(pending.allManifests(table.io())).doesNotContain(initialManifest);

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_C), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, pendingId, baseSnapshotId),
        files(FILE_A, FILE_A, FILE_B),
        statuses(DELETED, DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    assertThat(listManifestFiles()).hasSize(3);
  }

  @TestTemplate
  public void testAddAndDelete() {
    assertThat(listManifestFiles()).isEmpty();

    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseSnapshotId = latestSnapshot(base, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = latestSnapshot(table, branch).allManifests(table.io()).get(0);

    Snapshot pending =
        apply(table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C)), branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);
    assertThat(pending.allManifests(table.io())).doesNotContain(initialManifest);

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_C), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, baseSnapshotId),
        files(FILE_A, FILE_B),
        statuses(DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    assertThat(listManifestFiles()).hasSize(3);
  }

  @TestTemplate
  public void testRewriteDataAndDeleteFiles() {
    assumeThat(formatVersion)
        .as("Rewriting delete files is only supported in iceberg format v2 or later")
        .isGreaterThan(1);
    assertThat(listManifestFiles()).isEmpty();

    commit(
        table,
        table
            .newRowDelta()
            .addRows(FILE_A)
            .addRows(FILE_B)
            .addRows(FILE_C)
            .addDeletes(FILE_A_DELETES)
            .addDeletes(FILE_B_DELETES),
        branch);

    TableMetadata base = readMetadata();
    Snapshot baseSnap = latestSnapshot(base, branch);
    long baseSnapshotId = baseSnap.snapshotId();
    assertThat(baseSnap.allManifests(table.io())).hasSize(2);
    List<ManifestFile> initialManifests = baseSnap.allManifests(table.io());

    validateManifestEntries(
        initialManifests.get(0),
        ids(baseSnapshotId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(ADDED, ADDED, ADDED));
    validateDeleteManifest(
        initialManifests.get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseSnapshotId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(ADDED, ADDED));

    // Rewrite the files.
    Snapshot pending =
        apply(
            table
                .newRewrite()
                .validateFromSnapshot(latestSnapshot(table, branch).snapshotId())
                .rewriteFiles(
                    ImmutableSet.of(FILE_A),
                    ImmutableSet.of(FILE_A_DELETES),
                    ImmutableSet.of(FILE_D),
                    ImmutableSet.of()),
            branch);

    assertThat(pending.allManifests(table.io())).hasSize(3);
    assertThat(pending.allManifests(table.io())).doesNotContainAnyElementsOf(initialManifests);

    long pendingId = pending.snapshotId();
    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_D), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(
        pending.allManifests(table.io()).get(2),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(pendingId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, EXISTING));

    // We should only get the 5 manifests that this test is expected to add.
    assertThat(listManifestFiles()).hasSize(5);
  }

  @TestTemplate
  public void testRewriteDataAndAssignOldSequenceNumber() {
    assumeThat(formatVersion)
        .as("Sequence number is only supported in iceberg format v2 or later")
        .isGreaterThan(1);
    assertThat(listManifestFiles()).isEmpty();

    commit(
        table,
        table
            .newRowDelta()
            .addRows(FILE_A)
            .addRows(FILE_B)
            .addRows(FILE_C)
            .addDeletes(FILE_A_DELETES)
            .addDeletes(FILE_B_DELETES),
        branch);

    TableMetadata base = readMetadata();
    Snapshot baseSnap = latestSnapshot(base, branch);
    long baseSnapshotId = baseSnap.snapshotId();
    assertThat(baseSnap.allManifests(table.io())).hasSize(2);
    List<ManifestFile> initialManifests = baseSnap.allManifests(table.io());

    validateManifestEntries(
        initialManifests.get(0),
        ids(baseSnapshotId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(ADDED, ADDED, ADDED));
    validateDeleteManifest(
        initialManifests.get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseSnapshotId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(ADDED, ADDED));

    // Rewrite the files.
    long oldSequenceNumber = latestSnapshot(table, branch).sequenceNumber();
    Snapshot pending =
        apply(
            table
                .newRewrite()
                .validateFromSnapshot(latestSnapshot(table, branch).snapshotId())
                .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_D), oldSequenceNumber),
            branch);

    assertThat(pending.allManifests(table.io())).hasSize(3);
    assertThat(pending.dataManifests(table.io())).doesNotContainAnyElementsOf(initialManifests);

    long pendingId = pending.snapshotId();
    ManifestFile newManifest = pending.allManifests(table.io()).get(0);
    validateManifestEntries(newManifest, ids(pendingId), files(FILE_D), statuses(ADDED));
    assertThat(ManifestFiles.read(newManifest, FILE_IO).entries())
        .allSatisfy(
            entry -> {
              assertThat(entry.dataSequenceNumber()).isEqualTo(oldSequenceNumber);
            });
    assertThat(newManifest.sequenceNumber()).isEqualTo(oldSequenceNumber + 1);

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(
        pending.allManifests(table.io()).get(2),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseSnapshotId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(ADDED, ADDED));

    // We should only get the 4 manifests that this test is expected to add.
    assertThat(listManifestFiles()).hasSize(4);
  }

  @TestTemplate
  public void testFailure() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    table.ops().failCommits(5);

    RewriteFiles rewrite =
        table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2, ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    assertThatThrownBy(() -> commit(table, rewrite, branch))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(new File(manifest1.path())).doesNotExist();
    assertThat(new File(manifest2.path())).doesNotExist();

    // As commit failed all the manifests added with rewrite should be cleaned up
    assertThat(listManifestFiles()).hasSize(1);
  }

  @TestTemplate
  public void testFailureWhenRewriteBothDataAndDeleteFiles() {
    assumeThat(formatVersion)
        .as("Rewriting delete files is only supported in iceberg format v2 or later.")
        .isGreaterThan(1);

    commit(
        table,
        table
            .newRowDelta()
            .addRows(FILE_A)
            .addRows(FILE_B)
            .addRows(FILE_C)
            .addDeletes(FILE_A_DELETES)
            .addDeletes(FILE_B_DELETES),
        branch);

    long baseSnapshotId = latestSnapshot(readMetadata(), branch).snapshotId();
    table.ops().failCommits(5);

    RewriteFiles rewrite =
        table
            .newRewrite()
            .validateFromSnapshot(latestSnapshot(table, branch).snapshotId())
            .rewriteFiles(
                ImmutableSet.of(FILE_A),
                ImmutableSet.of(FILE_A_DELETES, FILE_B_DELETES),
                ImmutableSet.of(FILE_D),
                ImmutableSet.of());
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(3);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);
    ManifestFile manifest3 = pending.allManifests(table.io()).get(2);

    validateManifestEntries(
        pending.allManifests(table.io()).get(0),
        ids(pending.snapshotId()),
        files(FILE_D),
        statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pending.snapshotId(), baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(
        pending.allManifests(table.io()).get(2),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(pending.snapshotId(), pending.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, DELETED));

    assertThatThrownBy(rewrite::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(new File(manifest1.path())).doesNotExist();
    assertThat(new File(manifest2.path())).doesNotExist();
    assertThat(new File(manifest3.path())).doesNotExist();

    // As commit failed all the manifests added with rewrite should be cleaned up
    assertThat(listManifestFiles()).hasSize(2);
  }

  @TestTemplate
  public void testRecovery() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    table.ops().failCommits(3);

    RewriteFiles rewrite =
        table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2, ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    commit(table, rewrite, branch);

    assertThat(new File(manifest1.path())).exists();
    assertThat(new File(manifest2.path())).exists();

    TableMetadata metadata = readMetadata();
    assertThat(latestSnapshot(metadata, branch).allManifests(table.io())).contains(manifest2);

    // 2 manifests added by rewrite and 1 original manifest should be found.
    assertThat(listManifestFiles()).hasSize(3);
  }

  @TestTemplate
  public void testRecoverWhenRewriteBothDataAndDeleteFiles() {
    assumeThat(formatVersion)
        .as("Rewriting delete files is only supported in iceberg format v2 or later.")
        .isGreaterThan(1);

    commit(
        table,
        table
            .newRowDelta()
            .addRows(FILE_A)
            .addRows(FILE_B)
            .addRows(FILE_C)
            .addDeletes(FILE_A_DELETES)
            .addDeletes(FILE_B_DELETES),
        branch);

    long baseSnapshotId = latestSnapshot(readMetadata(), branch).snapshotId();
    table.ops().failCommits(3);

    RewriteFiles rewrite =
        table
            .newRewrite()
            .validateFromSnapshot(latestSnapshot(table, branch).snapshotId())
            .rewriteFiles(
                ImmutableSet.of(FILE_A),
                ImmutableSet.of(FILE_A_DELETES, FILE_B_DELETES),
                ImmutableSet.of(FILE_D),
                ImmutableSet.of());
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(3);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);
    ManifestFile manifest3 = pending.allManifests(table.io()).get(2);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_D), statuses(ADDED));

    validateManifestEntries(
        manifest2,
        ids(pending.snapshotId(), baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(
        manifest3,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(pending.snapshotId(), pending.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, DELETED));

    commit(table, rewrite, branch);

    assertThat(new File(manifest1.path())).exists();
    assertThat(new File(manifest2.path())).exists();
    assertThat(new File(manifest3.path())).exists();

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    assertThat(latestSnapshot(metadata, branch).allManifests(table.io()))
        .isEqualTo(committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    assertThat(listManifestFiles()).hasSize(5);
  }

  @TestTemplate
  public void testReplaceEqualityDeletesWithPositionDeletes() {
    assumeThat(formatVersion)
        .as("Rewriting delete files is only supported in iceberg format v2 or later.")
        .isGreaterThan(1);

    commit(table, table.newRowDelta().addRows(FILE_A2).addDeletes(FILE_A2_DELETES), branch);

    TableMetadata metadata = readMetadata();
    long baseSnapshotId = latestSnapshot(metadata, branch).snapshotId();

    // Apply and commit the rewrite transaction.
    RewriteFiles rewrite =
        table
            .newRewrite()
            .rewriteFiles(
                ImmutableSet.of(), ImmutableSet.of(FILE_A2_DELETES),
                ImmutableSet.of(), ImmutableSet.of(FILE_B_DELETES));
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(3);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);
    ManifestFile manifest3 = pending.allManifests(table.io()).get(2);

    validateManifestEntries(manifest1, ids(baseSnapshotId), files(FILE_A2), statuses(ADDED));

    validateDeleteManifest(
        manifest2,
        dataSeqs(2L),
        fileSeqs(2L),
        ids(pending.snapshotId()),
        files(FILE_B_DELETES),
        statuses(ADDED));

    validateDeleteManifest(
        manifest3,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(pending.snapshotId()),
        files(FILE_A2_DELETES),
        statuses(DELETED));

    commit(table, rewrite, branch);

    assertThat(new File(manifest1.path())).exists();
    assertThat(new File(manifest2.path())).exists();
    assertThat(new File(manifest3.path())).exists();

    metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    assertThat(latestSnapshot(metadata, branch).allManifests(table.io()))
        .isEqualTo(committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    assertThat(listManifestFiles()).hasSize(4);
  }

  @TestTemplate
  public void testRemoveAllDeletes() {
    assumeThat(formatVersion)
        .as("Rewriting delete files is only supported in iceberg format v2 or later.")
        .isGreaterThan(1);

    commit(table, table.newRowDelta().addRows(FILE_A).addDeletes(FILE_A_DELETES), branch);

    // Apply and commit the rewrite transaction.
    RewriteFiles rewrite =
        table
            .newRewrite()
            .validateFromSnapshot(latestSnapshot(table, branch).snapshotId())
            .rewriteFiles(
                ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES),
                ImmutableSet.of(), ImmutableSet.of());
    Snapshot pending = apply(rewrite, branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    validateDeleteManifest(
        manifest2,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(pending.snapshotId()),
        files(FILE_A_DELETES),
        statuses(DELETED));

    commit(table, rewrite, branch);

    assertThat(new File(manifest1.path())).exists();
    assertThat(new File(manifest2.path())).exists();

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2);
    assertThat(latestSnapshot(metadata, branch).allManifests(table.io()))
        .containsAll(committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    assertThat(listManifestFiles()).hasSize(4);
  }

  @TestTemplate
  public void testDeleteNonExistentFile() {
    assertThat(listManifestFiles()).isEmpty();

    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    assertThat(latestSnapshot(base, branch).allManifests(table.io())).hasSize(1);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table.newRewrite().rewriteFiles(Sets.newSet(FILE_C), Sets.newSet(FILE_D)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-c.parquet");

    assertThat(listManifestFiles()).hasSize(1);
  }

  @TestTemplate
  public void testAlreadyDeletedFile() {
    assertThat(listManifestFiles()).isEmpty();

    commit(table, table.newAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    assertThat(latestSnapshot(base, branch).allManifests(table.io())).hasSize(1);

    RewriteFiles rewrite = table.newRewrite();
    Snapshot pending =
        apply(rewrite.rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B)), branch);

    assertThat(pending.allManifests(table.io())).hasSize(2);

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_B), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, latestSnapshot(table, branch).snapshotId()),
        files(FILE_A),
        statuses(DELETED));

    commit(table, rewrite, branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_D)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-a.parquet");

    assertThat(listManifestFiles()).hasSize(3);
  }

  @TestTemplate
  public void testNewDeleteFile() {
    assumeThat(formatVersion)
        .as("Delete files are only supported in iceberg format v2 or later.")
        .isGreaterThan(1);

    commit(table, table.newAppend().appendFile(FILE_A), branch);

    long snapshotBeforeDeletes = latestSnapshot(table, branch).snapshotId();

    commit(table, table.newRowDelta().addDeletes(FILE_A_DELETES), branch);

    long snapshotAfterDeletes = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(
            () ->
                apply(
                    table
                        .newRewrite()
                        .validateFromSnapshot(snapshotBeforeDeletes)
                        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2)),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, found new delete for replaced data file");

    // the rewrite should be valid when validating from the snapshot after the deletes
    apply(
        table
            .newRewrite()
            .validateFromSnapshot(snapshotAfterDeletes)
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2)),
        branch);
  }
}
