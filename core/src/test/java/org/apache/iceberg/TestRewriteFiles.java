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

import static org.apache.iceberg.ManifestEntry.Status.ADDED;
import static org.apache.iceberg.ManifestEntry.Status.DELETED;
import static org.apache.iceberg.ManifestEntry.Status.EXISTING;

@RunWith(Parameterized.class)
public class TestRewriteFiles extends TableTestBase {
  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] {1, 2};
  }

  public TestRewriteFiles(int formatVersion) {
    super(formatVersion);
  }

  @Test
  public void testEmptyTable() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a.parquet",
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B))
            .commit());

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a-deletes.parquet",
        () -> table.newRewrite()
            .rewriteFiles(ImmutableSet.of(), ImmutableSet.of(FILE_A_DELETES),
                ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_B_DELETES))
            .commit());
  }

  @Test
  public void testAddOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Data files to add can not be empty because there's no delete file to be rewritten",
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_A), Collections.emptySet())
            .apply());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Data files to add can not be empty because there's no delete file to be rewritten",
        () -> table.newRewrite()
            .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(),
                ImmutableSet.of(), ImmutableSet.of(FILE_A_DELETES))
            .apply());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Delete files to add must be empty because there's no delete file to be rewritten",
        () -> table.newRewrite()
            .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(),
                ImmutableSet.of(FILE_B), ImmutableSet.of(FILE_B_DELETES))
            .apply());
  }

  @Test
  public void testDeleteOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be null or empty",
        () -> table.newRewrite()
            .rewriteFiles(Collections.emptySet(), Sets.newSet(FILE_A))
            .apply());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be null or empty",
        () -> table.newRewrite()
            .rewriteFiles(ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(), ImmutableSet.of(FILE_A_DELETES))
            .apply());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be null or empty",
        () -> table.newRewrite()
            .rewriteFiles(ImmutableSet.of(), ImmutableSet.of(),
                ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES))
            .apply());
  }

  @Test
  public void testDeleteWithDuplicateEntriesInManifest() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseSnapshotId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);

    Snapshot pending = table.newRewrite()
        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.allManifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
        pending.allManifests().contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.allManifests().get(0),
        ids(pendingId),
        files(FILE_C),
        statuses(ADDED));

    validateManifestEntries(pending.allManifests().get(1),
        ids(pendingId, pendingId, baseSnapshotId),
        files(FILE_A, FILE_A, FILE_B),
        statuses(DELETED, DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testAddAndDelete() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseSnapshotId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());
    ManifestFile initialManifest = base.currentSnapshot().allManifests().get(0);

    Snapshot pending = table.newRewrite()
        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.allManifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
        pending.allManifests().contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.allManifests().get(0),
        ids(pendingId),
        files(FILE_C),
        statuses(ADDED));

    validateManifestEntries(pending.allManifests().get(1),
        ids(pendingId, baseSnapshotId),
        files(FILE_A, FILE_B),
        statuses(DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testRewriteDataAndDeleteFiles() {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newRowDelta()
        .addRows(FILE_A)
        .addRows(FILE_B)
        .addRows(FILE_C)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    TableMetadata base = readMetadata();
    Snapshot baseSnap = base.currentSnapshot();
    long baseSnapshotId = baseSnap.snapshotId();
    Assert.assertEquals("Should create 2 manifests for initial write", 2, baseSnap.allManifests().size());
    List<ManifestFile> initialManifests = baseSnap.allManifests();

    validateManifestEntries(initialManifests.get(0),
        ids(baseSnapshotId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(ADDED, ADDED, ADDED));
    validateDeleteManifest(initialManifests.get(1),
        seqs(1, 1),
        ids(baseSnapshotId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(ADDED, ADDED));

    // Rewrite the files.
    Snapshot pending = table.newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES),
            ImmutableSet.of(FILE_D), ImmutableSet.of())
        .apply();

    Assert.assertEquals("Should contain 3 manifest", 3, pending.allManifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
        pending.allManifests().stream().anyMatch(initialManifests::contains));

    long pendingId = pending.snapshotId();
    validateManifestEntries(pending.allManifests().get(0),
        ids(pendingId),
        files(FILE_D),
        statuses(ADDED));

    validateManifestEntries(pending.allManifests().get(1),
        ids(pendingId, baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(pending.allManifests().get(2),
        seqs(2, 1),
        ids(pendingId, baseSnapshotId),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, EXISTING));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 5 manifests should exist", 5, listManifestFiles().size());
  }

  @Test
  public void testFailure() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.ops().failCommits(5);

    RewriteFiles rewrite = table.newRewrite()
        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2,
        ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", rewrite::commit);

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 1 manifest should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testFailureWhenRewriteBothDataAndDeleteFiles() {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

    table.newRowDelta()
        .addRows(FILE_A)
        .addRows(FILE_B)
        .addRows(FILE_C)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long baseSnapshotId = readMetadata().currentSnapshot().snapshotId();
    table.ops().failCommits(5);

    RewriteFiles rewrite = table.newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES, FILE_B_DELETES),
            ImmutableSet.of(FILE_D), ImmutableSet.of());
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);
    ManifestFile manifest3 = pending.allManifests().get(2);

    validateManifestEntries(pending.allManifests().get(0),
        ids(pending.snapshotId()),
        files(FILE_D),
        statuses(ADDED));

    validateManifestEntries(pending.allManifests().get(1),
        ids(pending.snapshotId(), baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(pending.allManifests().get(2),
        seqs(2, 2),
        ids(pending.snapshotId(), pending.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, DELETED));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", rewrite::commit);

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest3.path()).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 2 manifest should exist", 2, listManifestFiles().size());
  }

  @Test
  public void testRecovery() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.ops().failCommits(3);

    RewriteFiles rewrite = table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()), files(FILE_B), statuses(ADDED));
    validateManifestEntries(manifest2,
        ids(pending.snapshotId()), files(FILE_A), statuses(DELETED));

    rewrite.commit();

    Assert.assertTrue("Should reuse the manifest for appends", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse the manifest with deletes", new File(manifest2.path()).exists());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should commit the manifest for append",
        metadata.currentSnapshot().allManifests().contains(manifest2));

    // 2 manifests added by rewrite and 1 original manifest should be found.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testRecoverWhenRewriteBothDataAndDeleteFiles() {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

    table.newRowDelta()
        .addRows(FILE_A)
        .addRows(FILE_B)
        .addRows(FILE_C)
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_B_DELETES)
        .commit();

    long baseSnapshotId = readMetadata().currentSnapshot().snapshotId();
    table.ops().failCommits(3);

    RewriteFiles rewrite = table.newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .rewriteFiles(ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES, FILE_B_DELETES),
            ImmutableSet.of(FILE_D), ImmutableSet.of());
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);
    ManifestFile manifest3 = pending.allManifests().get(2);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()),
        files(FILE_D),
        statuses(ADDED));

    validateManifestEntries(manifest2,
        ids(pending.snapshotId(), baseSnapshotId, baseSnapshotId),
        files(FILE_A, FILE_B, FILE_C),
        statuses(DELETED, EXISTING, EXISTING));

    validateDeleteManifest(manifest3,
        seqs(2, 2),
        ids(pending.snapshotId(), pending.snapshotId()),
        files(FILE_A_DELETES, FILE_B_DELETES),
        statuses(DELETED, DELETED));

    rewrite.commit();

    Assert.assertTrue("Should reuse new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest2.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest3.path()).exists());

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    Assert.assertEquals("Should committed the manifests",
        metadata.currentSnapshot().allManifests(), committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("Only 5 manifest should exist", 5, listManifestFiles().size());
  }

  @Test
  public void testReplaceEqualityDeletesWithPositionDeletes() {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

    table.newRowDelta()
        .addRows(FILE_A2)
        .addDeletes(FILE_A2_DELETES)
        .commit();

    TableMetadata metadata = readMetadata();
    long baseSnapshotId = metadata.currentSnapshot().snapshotId();

    // Apply and commit the rewrite transaction.
    RewriteFiles rewrite = table.newRewrite().rewriteFiles(
        ImmutableSet.of(), ImmutableSet.of(FILE_A2_DELETES),
        ImmutableSet.of(), ImmutableSet.of(FILE_B_DELETES)
    );
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 3 manifests", 3, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);
    ManifestFile manifest3 = pending.allManifests().get(2);

    validateManifestEntries(manifest1,
        ids(baseSnapshotId),
        files(FILE_A2),
        statuses(ADDED));

    validateDeleteManifest(manifest2,
        seqs(2),
        ids(pending.snapshotId()),
        files(FILE_B_DELETES),
        statuses(ADDED));

    validateDeleteManifest(manifest3,
        seqs(2),
        ids(pending.snapshotId()),
        files(FILE_A2_DELETES),
        statuses(DELETED));

    rewrite.commit();

    Assert.assertTrue("Should reuse new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest2.path()).exists());
    Assert.assertTrue("Should reuse new manifest", new File(manifest3.path()).exists());

    metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2, manifest3);
    Assert.assertEquals("Should committed the manifests",
        metadata.currentSnapshot().allManifests(), committedManifests);

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("4 manifests should exist", 4, listManifestFiles().size());
  }

  @Test
  public void testRemoveAllDeletes() {
    Assume.assumeTrue("Rewriting delete files is only supported in iceberg format v2. ", formatVersion > 1);

    table.newRowDelta()
        .addRows(FILE_A)
        .addDeletes(FILE_A_DELETES)
        .commit();

    // Apply and commit the rewrite transaction.
    RewriteFiles rewrite = table.newRewrite()
        .validateFromSnapshot(table.currentSnapshot().snapshotId())
        .rewriteFiles(
            ImmutableSet.of(FILE_A), ImmutableSet.of(FILE_A_DELETES),
            ImmutableSet.of(), ImmutableSet.of()
        );
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests().size());
    ManifestFile manifest1 = pending.allManifests().get(0);
    ManifestFile manifest2 = pending.allManifests().get(1);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()),
        files(FILE_A),
        statuses(DELETED));

    validateDeleteManifest(manifest2,
        seqs(2),
        ids(pending.snapshotId()),
        files(FILE_A_DELETES),
        statuses(DELETED));

    rewrite.commit();

    Assert.assertTrue("Should reuse the new manifest", new File(manifest1.path()).exists());
    Assert.assertTrue("Should reuse the new manifest", new File(manifest2.path()).exists());

    TableMetadata metadata = readMetadata();
    List<ManifestFile> committedManifests = Lists.newArrayList(manifest1, manifest2);
    Assert.assertTrue("Should committed the manifests",
        metadata.currentSnapshot().allManifests().containsAll(committedManifests));

    // As commit success all the manifests added with rewrite should be available.
    Assert.assertEquals("4 manifests should exist", 4, listManifestFiles().size());
  }

  @Test
  public void testDeleteNonExistentFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-c.parquet",
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_C), Sets.newSet(FILE_D))
            .commit());

    Assert.assertEquals("Only 1 manifests should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testAlreadyDeletedFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().allManifests().size());

    RewriteFiles rewrite = table.newRewrite();
    Snapshot pending = rewrite
        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.allManifests().size());

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.allManifests().get(0),
        ids(pendingId),
        files(FILE_B),
        statuses(ADDED));

    validateManifestEntries(pending.allManifests().get(1),
        ids(pendingId, base.currentSnapshot().snapshotId()),
        files(FILE_A),
        statuses(DELETED));

    rewrite.commit();

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        "Missing required files to delete: /path/to/data-a.parquet",
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_D))
            .commit());

    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testNewDeleteFile() {
    Assume.assumeTrue("Delete files are only supported in v2", formatVersion > 1);

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    long snapshotBeforeDeletes = table.currentSnapshot().snapshotId();

    table.newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .commit();

    long snapshotAfterDeletes = table.currentSnapshot().snapshotId();

    AssertHelpers.assertThrows("Should fail because deletes were added after the starting snapshot",
        ValidationException.class, "Cannot commit, found new delete for replaced data file",
        () -> table.newRewrite()
            .validateFromSnapshot(snapshotBeforeDeletes)
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2))
            .apply());

    // the rewrite should be valid when validating from the snapshot after the deletes
    table.newRewrite()
        .validateFromSnapshot(snapshotAfterDeletes)
        .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_A2))
        .apply();
  }
}
