/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.exceptions.CommitFailedException;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Iterators.concat;

public class TestMergeAppend extends TableTestBase {
  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .apply();

    Assert.assertEquals("Should create 1 manifest for initial write",
        1, pending.manifests().size());

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0), ids(pendingId, pendingId), files(FILE_A, FILE_B));
  }

  @Test
  public void testMergeWithExistingManifest() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    String initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest for second write",
        1, pending.manifests().size());
    String newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, pendingId, baseId, baseId),
        concat(files(FILE_C, FILE_D), files(initialManifest)));
  }

  @Test
  public void testMinMergeCount() {
    // only merge when there are at least 4 manifests
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    long idFileA = readMetadata().currentSnapshot().snapshotId();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    long idFileB = readMetadata().currentSnapshot().snapshotId();

    Assert.assertEquals("Should have 2 manifests from setup writes",
        2, readMetadata().currentSnapshot().manifests().size());

    table.newAppend()
        .appendFile(FILE_C)
        .commit();
    long idFileC = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should have 3 unmerged manifests",
        3, base.currentSnapshot().manifests().size());
    Set<String> unmerged = Sets.newHashSet(base.currentSnapshot().manifests());

    Snapshot pending = table.newAppend()
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 1 merged manifest after the 4th write",
        1, pending.manifests().size());
    String newManifest = pending.manifests().get(0);
    Assert.assertFalse("Should not contain previous manifests", unmerged.contains(newManifest));

    long pendingId = pending.snapshotId();

    validateManifest(newManifest,
        ids(pendingId, idFileC, idFileB, idFileA),
        files(FILE_D, FILE_C, FILE_B, FILE_A));
  }

  @Test
  public void testMergeSizeTargetWithExistingManifest() {
    // use a small limit on manifest size to prevent merging
    table.updateProperties()
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10")
        .commit();

    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    String initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertEquals("Should contain 2 unmerged manifests after second write",
        2, pending.manifests().size());
    String newManifest = pending.manifests().get(0);
    Assert.assertNotEquals("Should not contain manifest from initial write",
        initialManifest, newManifest);

    long pendingId = pending.snapshotId();
    validateManifest(newManifest, ids(pendingId, pendingId), files(FILE_C, FILE_D));

    validateManifest(pending.manifests().get(1), ids(baseId, baseId), files(initialManifest));
  }

  @Test
  public void testChangedPartitionSpec() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
        1, base.currentSnapshot().manifests().size());
    String initialManifest = base.currentSnapshot().manifests().get(0);

    PartitionSpec newSpec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    TableMetadata updated = new TableMetadata(table.ops(), null, base.location(),
        System.currentTimeMillis(), base.lastColumnId(), base.schema(), newSpec, base.properties(),
        base.currentSnapshot().snapshotId(), base.snapshots());
    table.ops().commit(base, updated);

    DataFile newFileC = DataFiles.builder(newSpec)
        .copy(FILE_C)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());

    // new manifest comes first
    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC));

    Assert.assertEquals("Second manifest should be the initial manifest with the old spec",
        initialManifest, pending.manifests().get(1));
  }

  @Test
  public void testChangedPartitionSpecMergeExisting() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();
    long id1 = readMetadata().currentSnapshot().snapshotId();

    // create a second compatible manifest
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();
    long id2 = readMetadata().currentSnapshot().snapshotId();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should contain 2 manifests",
        2, base.currentSnapshot().manifests().size());
    String manifest = base.currentSnapshot().manifests().get(0);

    PartitionSpec newSpec = PartitionSpec.builderFor(SCHEMA)
        .bucket("data", 16)
        .bucket("id", 4)
        .build();

    // commit the new partition spec to the table manually
    TableMetadata updated = new TableMetadata(table.ops(), null, base.location(),
        System.currentTimeMillis(), base.lastColumnId(), base.schema(), newSpec, base.properties(),
        base.currentSnapshot().snapshotId(), base.snapshots());
    table.ops().commit(base, updated);

    DataFile newFileC = DataFiles.builder(newSpec)
        .copy(FILE_C)
        .withPartitionPath("data_bucket=2/id_bucket=3")
        .build();

    Snapshot pending = table.newAppend()
        .appendFile(newFileC)
        .apply();

    Assert.assertEquals("Should use 2 manifest files",
        2, pending.manifests().size());
    Assert.assertFalse("First manifest should not be in the new snapshot",
        pending.manifests().contains(manifest));

    validateManifest(pending.manifests().get(0), ids(pending.snapshotId()), files(newFileC));
    validateManifest(pending.manifests().get(1), ids(id2, id1), files(FILE_B, FILE_A));
  }

  @Test
  public void testFailure() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    String initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(5);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    String newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest).exists());
  }

  @Test
  public void testRecovery() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    long baseId = base.currentSnapshot().snapshotId();
    String initialManifest = base.currentSnapshot().manifests().get(0);

    table.ops().failCommits(3);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    Assert.assertEquals("Should merge to 1 manifest", 1, pending.manifests().size());
    String newManifest = pending.manifests().get(0);

    Assert.assertTrue("Should create new manifest", new File(newManifest).exists());
    validateManifest(newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    append.commit();

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should reuse the new manifest", new File(newManifest).exists());
    Assert.assertEquals("Should commit the same new manifest during retry",
        Lists.newArrayList(newManifest), metadata.currentSnapshot().manifests());
  }

  private void validateManifest(String manifest,
                                Iterator<Long> ids,
                                Iterator<DataFile> expectedFiles) {
    for (ManifestEntry entry : ManifestReader.read(Files.localInput(manifest)).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      Assert.assertEquals("Path should match expected",
          expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
          (long) ids.next(), entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }

  private static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  private static Iterator<DataFile> files(DataFile... files) {
    return Iterators.forArray(files);
  }

  private static Iterator<DataFile> files(String manifest) {
    return ManifestReader.read(Files.localInput(manifest)).iterator();
  }
}
