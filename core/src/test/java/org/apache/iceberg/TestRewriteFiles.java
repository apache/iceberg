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

import java.io.File;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.internal.util.collections.Sets;

@RunWith(Parameterized.class)
public class TestRewriteFiles extends TableTestBase {

  private final String branch;

  @Parameterized.Parameters(name = "formatVersion = {0}, branch = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {1, "main"},
      new Object[] {1, "testBranch"},
      new Object[] {2, "main"},
      new Object[] {2, "testBranch"}
    };
  }

  public TestRewriteFiles(int formatVersion, String branch) {
    super(formatVersion);
    this.branch = branch;
  }

  @Test
  public void testEmptyTable() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.ref(branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a.parquet",
        () ->
            commit(
                table,
                table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B)),
                branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a-deletes.parquet",
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
                branch));
  }

  @Test
  public void testAddOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows(
        "Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a.parquet",
        () ->
            apply(
                table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Collections.emptySet()),
                branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        IllegalArgumentException.class,
        "Delete files to add must be empty because there's no delete file to be rewritten",
        () ->
            apply(
                table
                    .newRewrite()
                    .rewriteFiles(
                        ImmutableSet.of(FILE_A),
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        ImmutableSet.of(FILE_A_DELETES)),
                branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        IllegalArgumentException.class,
        "Delete files to add must be empty because there's no delete file to be rewritten",
        () ->
            apply(
                table
                    .newRewrite()
                    .rewriteFiles(
                        ImmutableSet.of(FILE_A),
                        ImmutableSet.of(),
                        ImmutableSet.of(FILE_B),
                        ImmutableSet.of(FILE_B_DELETES)),
                branch));
  }

  @Test
  public void testDeleteOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows(
        "Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be empty",
        () ->
            apply(
                table.newRewrite().rewriteFiles(Collections.emptySet(), Sets.newSet(FILE_A)),
                branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be empty",
        () ->
            apply(
                table
                    .newRewrite()
                    .rewriteFiles(
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        ImmutableSet.of(FILE_A_DELETES)),
                branch));

    AssertHelpers.assertThrows(
        "Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be empty",
        () ->
            apply(
                table
                    .newRewrite()
                    .rewriteFiles(
                        ImmutableSet.of(),
                        ImmutableSet.of(),
                        ImmutableSet.of(FILE_A),
                        ImmutableSet.of(FILE_A_DELETES)),
                branch));
  }

  @Test
  public void testDeleteWithDuplicateEntriesInManifest() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    commit(
        table, table.newAppend().appendFile(FILE_A).appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseSnapshotId = latestSnapshot(base, branch).snapshotId();
    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        latestSnapshot(base, branch).allManifests(table.io()).size());
    ManifestFile initialManifest = latestSnapshot(base, branch).allManifests(table.io()).get(0);

    Snapshot pending =
        apply(table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C)), branch);

    Assert.assertEquals("Should contain 2 manifest", 2, pending.allManifests(table.io()).size());
    Assert.assertFalse(
        "Should not contain manifest from initial write",
        pending.allManifests(table.io()).contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_C), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, pendingId, baseSnapshotId),
        files(FILE_A, FILE_A, FILE_B),
        statuses(DELETED, DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testAddAndDelete() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseSnapshotId = latestSnapshot(base, branch).snapshotId();
    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        latestSnapshot(table, branch).allManifests(table.io()).size());
    ManifestFile initialManifest = latestSnapshot(table, branch).allManifests(table.io()).get(0);

    Snapshot pending =
        apply(table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C)), branch);

    Assert.assertEquals("Should contain 2 manifest", 2, pending.allManifests(table.io()).size());
    Assert.assertFalse(
        "Should not contain manifest from initial write",
        pending.allManifests(table.io()).contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_C), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, baseSnapshotId),
        files(FILE_A, FILE_B),
        statuses(DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testRewriteDataAndDeleteFiles() {
    Assume.assumeTrue(
        "Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

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
    Assert.assertEquals(
        "Should create 2 manifests for initial write", 2, baseSnap.allManifests(table.io()).size());
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

    Assert.assertEquals("Should contain 3 manifest", 3, pending.allManifests(table.io()).size());
    Assert.assertFalse(
        "Should not contain manifest from initial write",
        pending.allManifests(table.io()).stream().anyMatch(initialManifests::contains));

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
    Assert.assertEquals("Only 5 manifests should exist", 5, listManifestFiles().size());
  }

  @Test
  public void testRewriteDataAndAssignOldSequenceNumber() {
    Assume.assumeTrue(
        "Sequence number is only supported in iceberg format v2. ", formatVersion > 1);
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

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
    Assert.assertEquals(
        "Should create 2 manifests for initial write", 2, baseSnap.allManifests(table.io()).size());
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

    Assert.assertEquals("Should contain 3 manifest", 3, pending.allManifests(table.io()).size());
    Assert.assertFalse(
        "Should not contain data manifest from initial write",
        pending.dataManifests(table.io()).stream().anyMatch(initialManifests::contains));

    long pendingId = pending.snapshotId();
    ManifestFile newManifest = pending.allManifests(table.io()).get(0);
    validateManifestEntries(newManifest, ids(pendingId), files(FILE_D), statuses(ADDED));
    for (ManifestEntry<DataFile> entry : ManifestFiles.read(newManifest, FILE_IO).entries()) {
      Assert.assertEquals(
          "Should have old sequence number for manifest entries",
          oldSequenceNumber,
          (long) entry.dataSequenceNumber());
    }
    Assert.assertEquals(
        "Should use new sequence number for the manifest file",
        oldSequenceNumber + 1,
        newManifest.sequenceNumber());

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
    Assert.assertEquals("Only 4 manifests should exist", 4, listManifestFiles().size());
  }

  @Test
  public void testFailure() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    table.ops().failCommits(5);

    RewriteFiles rewrite =
        table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = apply(rewrite, branch);

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests(table.io()).size());
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2, ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    AssertHelpers.assertThrows(
        "Should retry 4 times and throw last failure",
        CommitFailedException.class,
        "Injected failure",
        () -> commit(table, rewrite, branch));

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 1 manifest should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testFailureWhenRewriteBothDataAndDeleteFiles() {
    Assume.assumeTrue(
        "Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

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

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests(table.io()).size());
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

    AssertHelpers.assertThrows(
        "Should retry 4 times and throw last failure",
        CommitFailedException.class,
        "Injected failure",
        rewrite::commit);

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest3.path()).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 2 manifest should exist", 2, listManifestFiles().size());
  }

  @Test
  public void testRecovery() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);

    table.ops().failCommits(3);

    RewriteFiles rewrite =
        table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = apply(rewrite, branch);

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests(table.io()).size());
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2, ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    commit(table, rewrite, branch);

    Assert.assertTrue("Should reuse the manifest for appends", new File(manifest1.path()).exists());
    Assert.assertTrue(
        "Should reuse the manifest with deletes", new File(manifest2.path()).exists());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue(
        "Should commit the manifest for append",
        latestSnapshot(metadata, branch).allManifests(table.io()).contains(manifest2));

    // 2 manifests added by rewrite and 1 original manifest should be found.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testRecoverWhenRewriteBothDataAndDeleteFiles() {
    Assume.assumeTrue(
        "Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

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

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests(table.io()).size());
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

    Assert.assertTrue("Should reuse new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest2.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest3.path()).exists());

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    Assert.assertEquals(
        "Should committed the manifests",
        latestSnapshot(metadata, branch).allManifests(table.io()),
        committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("Only 5 manifest should exist", 5, listManifestFiles().size());
  }

  @Test
  public void testReplaceEqualityDeletesWithPositionDeletes() {
    Assume.assumeTrue(
        "Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

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

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests(table.io()).size());
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

    Assert.assertTrue("Should reuse new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest2.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest3.path()).exists());

    metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    Assert.assertEquals(
        "Should committed the manifests",
        latestSnapshot(metadata, branch).allManifests(table.io()),
        committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("4 manifests should exist", 4, listManifestFiles().size());
  }

  @Test
  public void testRemoveAllDeletes() {
    Assume.assumeTrue(
        "Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

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

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests(table.io()).size());
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

    Assert.assertTrue("Should reuse the new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse the new manifest", new File(manifest2.path()).exists());

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2);
    Assert.assertTrue(
        "Should committed the manifests",
        latestSnapshot(metadata, branch).allManifests(table.io()).containsAll(committedManifests));

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("4 manifests should exist", 4, listManifestFiles().size());
  }

  @Test
  public void testDeleteNonExistentFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        latestSnapshot(base, branch).allManifests(table.io()).size());

    AssertHelpers.assertThrows(
        "Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-c.parquet",
        () ->
            commit(
                table,
                table.newRewrite().rewriteFiles(Sets.newSet(FILE_C), Sets.newSet(FILE_D)),
                branch));

    Assert.assertEquals("Only 1 manifests should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testAlreadyDeletedFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    commit(table, table.newAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        latestSnapshot(base, branch).allManifests(table.io()).size());

    RewriteFiles rewrite = table.newRewrite();
    Snapshot pending =
        apply(rewrite.rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B)), branch);

    Assert.assertEquals("Should contain 2 manifest", 2, pending.allManifests(table.io()).size());

    long pendingId = pending.snapshotId();

    validateManifestEntries(
        pending.allManifests(table.io()).get(0), ids(pendingId), files(FILE_B), statuses(ADDED));

    validateManifestEntries(
        pending.allManifests(table.io()).get(1),
        ids(pendingId, latestSnapshot(table, branch).snapshotId()),
        files(FILE_A),
        statuses(DELETED));

    commit(table, rewrite, branch);

    AssertHelpers.assertThrows(
        "Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a.parquet",
        () ->
            commit(
                table,
                table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_D)),
                branch));

    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testNewDeleteFile() {
    Assume.assumeTrue("Delete files are only supported in v2", formatVersion > 1);

    commit(table, table.newAppend().appendFile(FILE_A), branch);

    long snapshotBeforeDeletes = latestSnapshot(table, branch).snapshotId();

    commit(table, table.newRowDelta().addDeletes(FILE_A_DELETES), branch);

    long snapshotAfterDeletes = latestSnapshot(table, branch).snapshotId();

    AssertHelpers.assertThrows(
        "Should fail because deletes were added after the starting snapshot",
        ValidationException.class,
        "Cannot commit, found new delete for replaced data file",
        () ->
            apply(
                table
                    .newRewrite()
                    .validateFromSnapshot(snapshotBeforeDeletes)
                    .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2)),
                branch));

    // the rewrite should be valid when validating from the snapshot after the deletes
    apply(
        table
            .newRewrite()
            .validateFromSnapshot(snapshotAfterDeletes)
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2)),
        branch);
  }
}
