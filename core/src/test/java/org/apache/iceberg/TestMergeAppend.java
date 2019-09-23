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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.util.Set;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.util.PathUtil;
import org.junit.Assert;
import org.junit.Test;

import static com.google.common.collect.Iterators.concat;

public class TestMergeAppend extends TableTestBase {
  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .apply();

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, pending.manifests().size());

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0), ids(pendingId, pendingId), files(fileA, fileB), table.location());
  }

  @Test
  public void testEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(fileA, fileB);
    Snapshot pending = table.newAppend()
        .appendManifest(manifest)
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, fileA, fileB);
  }

  @Test
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(fileA, fileB);
    Snapshot pending = table.newAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .appendManifest(manifest)
        .apply();

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0),
        ids(pendingId, pendingId),
        files(fileC, fileD),
        table.location());
    validateManifest(pending.manifests().get(1),
        ids(pendingId, pendingId),
        files(fileA, fileB),
        table.location());
  }

  @Test
  public void testMergeWithAppendFilesAndManifest() throws IOException {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    ManifestFile manifest = writeManifest(fileA, fileB);
    Snapshot pending = table.newAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .appendManifest(manifest)
        .apply();

    long pendingId = pending.snapshotId();

    Assert.assertEquals("Should create 1 merged manifest", 1, pending.manifests().size());
    validateManifest(pending.manifests().get(0),
        ids(pendingId, pendingId, pendingId, pendingId),
        files(fileC, fileD, fileA, fileB),
        table.location());
  }

  @Test
  public void testMergeWithExistingManifest() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, pendingId, baseId, baseId),
        concat(files(fileC, fileD), files(initialManifest, table.location())),
        table.location());
  }

  @Test
  public void testMergeWithExistingManifestAfterDelete() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.newDelete()
        .deleteFile(fileA)
        .commit();

    TableMetadata delete = readMetadata();
    long deleteId = delete.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 filtered manifest for delete",
        1, delete.currentSnapshot().manifests().size());
    ManifestFile deleteManifest = delete.currentSnapshot().manifests().get(0);

    validateManifestEntries(deleteManifest,
        ids(deleteId, baseId),
        files(fileA, fileB),
        statuses(Status.DELETED, Status.EXISTING),
        table.location());

    Snapshot pending = table.newAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();

    // the deleted entry from the previous manifest should be removed
    validateManifestEntries(newManifest,
        ids(pendingId, pendingId, baseId),
        files(fileC, fileD, fileB),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING),
        table.location());
  }

  @Test
  public void testMinMergeCount() {
    // only merge when there are at least 4 manifests
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newFastAppend()
        .appendFile(fileA)
        .commit();
    long idFileA = readMetadata().currentSnapshot().snapshotId();

    table.newFastAppend()
        .appendFile(fileB)
        .commit();
    long idFileB = readMetadata().currentSnapshot().snapshotId();

    Assert.assertEquals("Should have 2 manifests from setup writes",
        2, readMetadata().currentSnapshot().manifests().size());

    table.newAppend()
        .appendFile(fileC)
        .commit();
    long idFileC = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should have 3 unmerged manifests",
        3, base.currentSnapshot().manifests().size());
    Set<ManifestFile> unmerged = Sets.newHashSet(base.currentSnapshot().manifests());

    Snapshot pending = table.newAppend()
        .appendFile(fileD)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest after the 4th write",
        1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertFalse("Should not contain previous manifests", unmerged.contains(newManifest));

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, idFileC, idFileB, idFileA),
        files(fileD, fileC, fileB, fileA),
        table.location());
  }

  @Test
  public void testMergeSizeTargetWithExistingManifest() {
    // use a small limit on manifest size to prevent merging
    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(fileC)
        .appendFile(fileD)
        .apply();

    Assert.assertEquals("Should contain 2 unmerged manifests after second write",
        2, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();
    validateManifest(newManifest, ids(pendingId, pendingId), files(fileC, fileD), table.location());

    validateManifest(
        pending.manifests().get(1),
        ids(baseId, baseId),
        files(initialManifest, table.location()),
        table.location()
    );
  }

  @Test
  public void testChangedPartitionSpec() {
    table.newAppend()
        .appendFile(fileA)
        .appendFile(fileB)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileC = DataFiles.builder(newSpec, null)
        .copy(fileC)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());

    // new manifest comes first
    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC), table.location());

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, pending.manifests().get(1));
  }

  @Test
  public void testChangedPartitionSpecMergeExisting() {
    table.newAppend()
        .appendFile(fileA)
        .commit();
    long id1 = readMetadata().currentSnapshot().snapshotId();

    // create a second compatible manifest
    table.newFastAppend()
        .appendFile(fileB)
        .commit();
    long id2 = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should contain 2 manifests",
        2, base.currentSnapshot().manifests().size());
    ManifestFile manifest = base.currentSnapshot().manifests().get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec = PartitionSpec.builderFor(base.schema())
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));

    DataFile newFileC = DataFiles.builder(newSpec, null)
        .copy(fileC)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());
    Assert.assertFalse("First manifest should not be in the new snapshot",
        pending.manifests().contains(manifest));

    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC), table.location());
    validateManifest(pending.manifests().get(1), ids(id2, id1), files(fileB, fileA), table.location());
  }

  @Test
  public void testFailure() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(fileA)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(5);

    AppendFiles append = table.newAppend().appendFile(fileB);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(fileB), files(initialManifest, table.location())),
        table.location());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest",
            new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
  }

  @Test
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(fileA, fileB);
    AppendFiles append = table.newAppend().appendManifest(manifest);
    Snapshot pending = append.apply();
    ManifestFile newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest",
            new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
  }

  @Test
  public void testRecovery() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(fileA)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    ManifestFile initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(3);

    AppendFiles append = table.newAppend().appendFile(fileB);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    ManifestFile newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(fileB), files(initialManifest, table.location())),
        table.location());

    append.commit();

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should reuse the new manifest",
        new File(PathUtil.getAbsolutePath(table.location(), newManifest.path())).exists());
    Assert.assertEquals("Should commit the same new manifest during retry",
        Lists.newArrayList(newManifest), metadata.currentSnapshot().manifests());
  }
}
