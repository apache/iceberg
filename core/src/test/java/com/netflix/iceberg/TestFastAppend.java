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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.netflix.iceberg.exceptions.CommitFailedException;
import org.junit.Assert;
import org.junit.Test;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class TestFastAppend extends TableTestBase {

  @Test
  public void testEmptyTableAppend() {
    Assert.assertEquals("Table should start empty", 0, listMetadataFiles("avro").size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", base.currentSnapshot());

    Snapshot pending = table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .apply();

    validateSnapshot(base.currentSnapshot(), pending, FILE_A, FILE_B);
  }

  @Test
  public void testNonEmptyTableAppend() {
    table.newAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<String> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have one existing manifest", 1, v2manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Assert.assertNotEquals("Snapshots should have unique IDs",
        base.currentSnapshot().snapshotId(), pending.snapshotId());
    validateSnapshot(base.currentSnapshot(), pending, FILE_C, FILE_D);
  }

  @Test
  public void testNoMerge() {
    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<String> v3manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 2 existing manifests", 2, v3manifests.size());

    // prepare a new append
    Snapshot pending = table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .apply();

    Set<Long> ids = Sets.newHashSet();
    for (Snapshot snapshot : base.snapshots()) {
      ids.add(snapshot.snapshotId());
    }
    ids.add(pending.snapshotId());
    Assert.assertEquals("Snapshots should have 3 unique IDs", 3, ids.size());

    validateSnapshot(base.currentSnapshot(), pending, FILE_C, FILE_D);
  }

  @Test
  public void testRefreshBeforeApply() {
    // load a new copy of the table that will not be refreshed by the commit
    Table stale = load();

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<String> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    // commit from the stale table
    AppendFiles append = stale.newFastAppend()
        .appendFile(FILE_D);
    Snapshot pending = append.apply();

    // table should have been refreshed before applying the changes
    validateSnapshot(base.currentSnapshot(), pending, FILE_D);
  }

  @Test
  public void testRefreshBeforeCommit() {
    // commit from the stale table
    AppendFiles append = table.newFastAppend()
        .appendFile(FILE_D);
    Snapshot pending = append.apply();

    validateSnapshot(null, pending, FILE_D);

    table.newAppend()
        .appendFile(FILE_A)
        .commit();

    TableMetadata base = readMetadata();
    Assert.assertNotNull("Should have a current snapshot", base.currentSnapshot());
    List<String> v2manifests = base.currentSnapshot().manifests();
    Assert.assertEquals("Should have 1 existing manifest", 1, v2manifests.size());

    append.commit();

    TableMetadata committed = readMetadata();

    // apply was called before the conflicting commit, but the commit was still consistent
    validateSnapshot(base.currentSnapshot(), committed.currentSnapshot(), FILE_D);

    List<String> committedManifests = Lists.newArrayList(committed.currentSnapshot().manifests());
    committedManifests.removeAll(base.currentSnapshot().manifests());
    Assert.assertEquals("Should reused manifest created by apply",
        pending.manifests().get(0), committedManifests.get(0));
  }

  @Test
  public void testFailure() {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();
    String newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest).exists());

    AssertHelpers.assertThrows("Should retry 4 times and throw last failure",
        CommitFailedException.class, "Injected failure", append::commit);

    Assert.assertFalse("Should clean up new manifest", new File(newManifest).exists());
  }

  @Test
  public void testRecovery() {
    // inject 3 failures, the last try will succeed
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(3);

    AppendFiles append = table.newFastAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();
    String newManifest = pending.manifests().get(0);
    Assert.assertTrue("Should create new manifest", new File(newManifest).exists());

    append.commit();

    TableMetadata metadata = readMetadata();

    validateSnapshot(null, metadata.currentSnapshot(), FILE_B);
    Assert.assertTrue("Should commit same new manifest", new File(newManifest).exists());
    Assert.assertTrue("Should commit the same new manifest",
        metadata.currentSnapshot().manifests().contains(newManifest));
  }

  private void validateSnapshot(Snapshot old, Snapshot snap, DataFile... newFiles) {
    List<String> oldManifests = old != null ? old.manifests() : ImmutableList.of();

    // copy the manifests to a modifiable list and remove the existing manifests
    List<String> newManifests = Lists.newArrayList(snap.manifests());
    for (String oldManifest : oldManifests) {
      Assert.assertTrue("New snapshot should contain old manifests",
          newManifests.remove(oldManifest));
    }

    Assert.assertEquals("Should create 1 new manifest and reuse old manifests",
        1, newManifests.size());
    String manifest = newManifests.get(0);

    long id = snap.snapshotId();
    Iterator<String> newPaths = paths(newFiles).iterator();

    for (ManifestEntry entry : ManifestReader.read(Files.localInput(manifest)).entries()) {
      DataFile file = entry.file();
      Assert.assertEquals("Path should match expected", newPaths.next(), file.path().toString());
      Assert.assertEquals("File's snapshot ID should match", id, entry.snapshotId());
    }

    Assert.assertFalse("Should find all files in the manifest", newPaths.hasNext());
  }

  private List<String> paths(DataFile... dataFiles) {
    List<String> paths = Lists.newArrayListWithExpectedSize(dataFiles.length);
    for (DataFile file : dataFiles) {
      paths.add(file.path().toString());
    }
    return paths;
  }
}
