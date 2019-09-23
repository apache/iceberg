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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.util.PathUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import static org.apache.iceberg.ManifestEntry.Status.ADDED;
import static org.apache.iceberg.ManifestEntry.Status.DELETED;
import static org.apache.iceberg.ManifestEntry.Status.EXISTING;

public class TestRewriteFiles extends TableTestBase {

  @Test
  public void testEmptyTable() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        String.format("Missing required files to delete: %s", fileA.path().toString()),
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileB))
            .commit());
  }

  @Test
  public void testAddOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Files to add can not be null or empty",
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(fileA), Collections.emptySet())
            .apply());
  }

  @Test
  public void testDeleteOnly() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    AssertHelpers.assertThrows("Expected an exception",
        IllegalArgumentException.class,
        "Files to delete cannot be null or empty",
        () -> table.newRewrite()
            .rewriteFiles(Collections.emptySet(), Sets.newSet(fileA))
            .apply());
  }

  @Test
  public void testDeleteWithDuplicateEntriesInManifest() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseSnapshotId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newRewrite()
        .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileC))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.manifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
        pending.manifests().contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.manifests().get(0),
        ids(pendingId),
        files(fileC),
        statuses(ADDED),
        table.location());

    validateManifestEntries(pending.manifests().get(1),
        ids(pendingId, pendingId, baseSnapshotId),
        files(fileA, fileA, fileB),
        statuses(DELETED, DELETED, EXISTING),
        table.location());

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testAddAndDelete() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseSnapshotId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newRewrite()
        .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileC))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.manifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
        pending.manifests().contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.manifests().get(0),
        ids(pendingId),
        files(fileC),
        statuses(ADDED),
        table.location());

    validateManifestEntries(pending.manifests().get(1),
        ids(pendingId, baseSnapshotId),
        files(fileA, fileB),
        statuses(DELETED, EXISTING),
        table.location());

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testFailure() {
    table.newAppend()
        .appendFile(fileA)
        .commit();

    table.ops().failCommits(5);

    RewriteFiles rewrite = table.newRewrite()
        .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileB));
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.manifests().size());
    ManifestFile manifest1 = pending.manifests().get(0);
    ManifestFile manifest2 = pending.manifests().get(1);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()), files(fileB), statuses(ADDED),
        table.location());
    validateManifestEntries(manifest2,
        ids(pending.snapshotId()), files(fileA), statuses(DELETED),
        table.location());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", rewrite::commit);

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 1 manifest should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testRecovery() {
    table.newAppend()
        .appendFile(fileA)
        .commit();

    table.ops().failCommits(3);

    RewriteFiles rewrite = table.newRewrite().rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileB));
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.manifests().size());
    ManifestFile manifest1 = pending.manifests().get(0);
    ManifestFile manifest2 = pending.manifests().get(1);

    validateManifestEntries(manifest1,
        ids(pending.snapshotId()), files(fileB), statuses(ADDED),
        table.location());
    validateManifestEntries(manifest2,
        ids(pending.snapshotId()), files(fileA), statuses(DELETED),
        table.location());

    rewrite.commit();

    Assert.assertTrue("Should reuse the manifest for appends",
        new File(PathUtil.getAbsolutePath(table.location(), manifest1.path())).exists());
    Assert.assertTrue("Should reuse the manifest with deletes",
        new File(PathUtil.getAbsolutePath(table.location(), manifest2.path())).exists());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should commit the manifest for append",
        metadata.currentSnapshot().manifests().contains(manifest2));

    // 2 manifests added by rewrite and 1 original manifest should be found.
    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }

  @Test
  public void testDeleteNonExistentFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        String.format("Missing required files to delete: %s", fileC.path().toString()),
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(fileC), Sets.newSet(fileD))
            .commit());

    Assert.assertEquals("Only 1 manifests should exist", 1, listManifestFiles().size());
  }

  @Test
  public void testAlreadyDeletedFile() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());

    RewriteFiles rewrite = table.newRewrite();
    Snapshot pending = rewrite
        .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileB))
        .apply();

    Assert.assertEquals("Should contain 2 manifest",
        2, pending.manifests().size());

    long pendingId = pending.snapshotId();

    validateManifestEntries(pending.manifests().get(0),
        ids(pendingId),
        files(fileB),
        statuses(ADDED),
        table.location());

    validateManifestEntries(pending.manifests().get(1),
        ids(pendingId, base.currentSnapshot().snapshotId()),
        files(fileA),
        statuses(DELETED),
        table.location());

    rewrite.commit();

    AssertHelpers.assertThrows("Expected an exception",
        ValidationException.class,
        String.format("Missing required files to delete: %s", fileA.path().toString()),
        () -> table.newRewrite()
            .rewriteFiles(Sets.newSet(fileA), Sets.newSet(fileD))
            .commit());

    Assert.assertEquals("Only 3 manifests should exist", 3, listManifestFiles().size());
  }
}
