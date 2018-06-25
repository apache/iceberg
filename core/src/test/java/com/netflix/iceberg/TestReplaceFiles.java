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
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;

import static com.google.common.collect.Iterators.concat;
import static com.netflix.iceberg.ManifestEntry.Status.ADDED;
import static com.netflix.iceberg.ManifestEntry.Status.DELETED;
import static com.netflix.iceberg.ManifestEntry.Status.EXISTING;

public class TestReplaceFiles extends TableTestBase {

  @Test(expected = ValidationException.class)
  public void testEmptyTable() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B))
            .apply();
  }

  @Test
  public void testAddOnly() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    AssertHelpers.assertThrows("Expected an exception",
            IllegalArgumentException.class,
            "files to add can not be null or empty",
            () -> {
              table.newRewrite()
                      .rewriteFiles(Sets.newSet(FILE_A), Collections.emptySet())
                      .apply();
            }
    );
  }

  @Test
  public void testDeleteOnly() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    AssertHelpers.assertThrows("Expected an exception",
            IllegalArgumentException.class,
            "files to delete can not be null or empty",
            () -> {
              table.newRewrite()
                      .rewriteFiles(Collections.emptySet(), Sets.newSet(FILE_A))
                      .apply();
            }
    );

  }

  @Test
  public void testAddAndDelete() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newAppend()
            .appendFile(FILE_A)
            .appendFile(FILE_B)
            .commit();

    TableMetadata base = readMetadata();
    final long baseSnapshotId = base.currentSnapshot().snapshotId();
    Assert.assertEquals("Should create 1 manifest for initial write",
            1, base.currentSnapshot().manifests().size());
    String initialManifest = base.currentSnapshot().manifests().get(0);

    Snapshot pending = table.newRewrite()
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_C))
            .apply();

    Assert.assertEquals("Should contain 2 manifest",
            2, pending.manifests().size());
    Assert.assertFalse("Should not contain manifest from initial write",
            pending.manifests().contains(initialManifest));

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0),
            ids(pendingId, baseSnapshotId),
            concat(files(FILE_A, FILE_B)),
            statuses(DELETED, EXISTING));

    validateManifest(pending.manifests().get(1),
            ids(pendingId),
            concat(files(FILE_C)),
            statuses(ADDED));

    // We should only get the 3 manifests that this test is expected to add.
    Assert.assertEquals("Only 3 manifests should exist", 3, listMetadataFiles("avro").size());
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

    Assert.assertEquals("Should produce 2 manifests", 2, pending.manifests().size());
    String manifest1 = pending.manifests().get(0);
    String manifest2 = pending.manifests().get(1);

    validateManifest(manifest1, ids(pending.snapshotId()), concat(files(FILE_A)), statuses(DELETED));
    validateManifest(manifest2, ids(pending.snapshotId()), concat(files(FILE_B)), statuses(ADDED));

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
            CommitFailedException.class, "Injected failure", rewrite::commit);

    Assert.assertFalse("Should clean up new manifest", new File(manifest1).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2).exists());

    // As commit failed all the manifests added with rewrite should be cleaned up
    Assert.assertEquals("Only 1 manifest should exist", 1, listMetadataFiles("avro").size());
  }

  @Test
  public void testRecovery() {
    table.newAppend()
            .appendFile(FILE_A)
            .commit();

    table.ops().failCommits(3);

    RewriteFiles rewrite = table.newRewrite().rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B));
    Snapshot pending = rewrite.apply();

    Assert.assertEquals("Should produce 2 manifests", 2, pending.manifests().size());
    String manifest1 = pending.manifests().get(0);
    String manifest2 = pending.manifests().get(1);

    validateManifest(manifest1, ids(pending.snapshotId()), concat(files(FILE_A)), statuses(DELETED));
    validateManifest(manifest2, ids(pending.snapshotId()), concat(files(FILE_B)), statuses(ADDED));

    rewrite.commit();

    Assert.assertFalse("Should not reuse the manifest with deletes", new File(manifest1).exists());
    Assert.assertTrue("Should reuse the manifest for appends", new File(manifest2).exists());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue("Should commit the manifest for append",
            metadata.currentSnapshot().manifests().contains(manifest2));

    // 2 manifests added by rewrite and 1 original manifest should be found.
    Assert.assertEquals("Only 3 manifests should exist", 3, listMetadataFiles("avro").size());
  }

  @Test
  public void testDeleteNonExistentFile() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newAppend()
            .appendFile(FILE_A)
            .appendFile(FILE_B)
            .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
            1, base.currentSnapshot().manifests().size());

    AssertHelpers.assertThrows("Expected an exception",
            ValidationException.class,
            "files /path/to/data-c.parquet are no longer available in any manifests",
            () -> {
              table.newRewrite()
                      .rewriteFiles(Sets.newSet(FILE_C), Sets.newSet(FILE_D))
                      .commit();
            }
    );

    Assert.assertEquals("Only 1 manifests should exist", 1, listMetadataFiles("avro").size());
  }

  @Test
  public void testAlreadyDeletedFile() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    table.newAppend()
            .appendFile(FILE_A)
            .commit();

    TableMetadata base = readMetadata();
    Assert.assertEquals("Should create 1 manifest for initial write",
            1, base.currentSnapshot().manifests().size());

    final RewriteFiles rewrite = table.newRewrite();
    final Snapshot pending = rewrite
            .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_B))
            .apply();

    Assert.assertEquals("Should contain 2 manifest",
            2, pending.manifests().size());

    long pendingId = pending.snapshotId();

    validateManifest(pending.manifests().get(0),
            ids(pendingId, base.currentSnapshot().snapshotId()),
            concat(files(FILE_A)),
            statuses(DELETED));

    validateManifest(pending.manifests().get(1),
            ids(pendingId),
            concat(files(FILE_B)),
            statuses(ADDED));

    rewrite.commit();

    AssertHelpers.assertThrows("Expected an exception",
            ValidationException.class,
            "files /path/to/data-a.parquet are no longer available in any manifests",
            () -> {
              table.newRewrite()
                      .rewriteFiles(Sets.newSet(FILE_A), Sets.newSet(FILE_D))
                      .commit();
            }
    );

    Assert.assertEquals("Only 3 manifests should exist", 3, listMetadataFiles("avro").size());
  }
  
  private static Iterator<Long> ids(Long... ids) {
    return Iterators.forArray(ids);
  }

  private static Iterator<DataFile> files(DataFile... files) {
    return Iterators.forArray(files);
  }

  private static Iterator<ManifestEntry.Status> statuses(ManifestEntry.Status... statuses) {
    return Iterators.forArray(statuses);
  }

  public static void validateManifest(String manifest,
                                      Iterator<Long> ids,
                                      Iterator<DataFile> expectedFiles,
                                      Iterator<ManifestEntry.Status> expectedStatuses) {
    for (ManifestEntry entry : ManifestReader.read(Files.localInput(manifest)).entries()) {
      DataFile file = entry.file();
      DataFile expected = expectedFiles.next();
      final ManifestEntry.Status expectedStatus = expectedStatuses.next();
      Assert.assertEquals("Path should match expected",
              expected.path().toString(), file.path().toString());
      Assert.assertEquals("Snapshot ID should match expected ID",
              (long) ids.next(), entry.snapshotId());
      Assert.assertEquals("Entry status should match expected ID",
              expectedStatus, entry.status());
    }

    Assert.assertFalse("Should find all files in the manifest", expectedFiles.hasNext());
  }
}
