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

import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.TableProperties.MANIFEST_MERGE_ENABLED;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestRewriteManifests extends TableTestBase {

  @Test
  public void testReplaceManifestsSeparate() {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .appendFile(FILE_B)
      .commit();
    long appendId = table.currentSnapshot().snapshotId();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    // cluster by path will split the manifest into two

    table.rewriteManifests()
      .clusterBy(file -> file.path())
      .commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(manifests.get(0),
                            ids(appendId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(manifests.get(1),
                            ids(appendId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testReplaceManifestsConsolidate() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    Assert.assertEquals(2, table.currentSnapshot().manifests().size());

    // cluster by constant will combine manifests into one

    table.rewriteManifests()
      .clusterBy(file -> "file")
      .commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(1, manifests.size());

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader reader = ManifestReader.read(localInput(manifests.get(0).path()))) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testReplaceManifestsWithFilter() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    table.newFastAppend()
      .appendFile(FILE_C)
      .commit();
    long appendIdC = table.currentSnapshot().snapshotId();

    Assert.assertEquals(3, table.currentSnapshot().manifests().size());

    //keep the file A manifest, combine the other two

    table.rewriteManifests()
      .clusterBy(file -> "file")
      .rewriteIf(manifest -> {
        try (ManifestReader reader = ManifestReader.read(localInput(manifest.path()))) {
          return !reader.iterator().next().path().equals(FILE_A.path());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      })
      .commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader reader = ManifestReader.read(localInput(manifests.get(0).path()))) {
      if (reader.iterator().next().path().equals(FILE_B.path())) {
        files = Arrays.asList(FILE_B, FILE_C);
        ids = Arrays.asList(appendIdB, appendIdC);
      } else {
        files = Arrays.asList(FILE_C, FILE_B);
        ids = Arrays.asList(appendIdC, appendIdB);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
    validateManifestEntries(manifests.get(1),
                            ids(appendIdA),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsMaxSize() {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .appendFile(FILE_B)
      .commit();
    long appendId = table.currentSnapshot().snapshotId();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    // cluster by constant will combine manifests into one but small target size will create one per entry
    BaseRewriteManifests rewriteManifests = spy((BaseRewriteManifests) table.rewriteManifests());
    when(rewriteManifests.getManifestTargetSizeBytes()).thenReturn(1L);
    rewriteManifests.clusterBy(file -> "file").commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(manifests.get(0),
                            ids(appendId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(manifests.get(1),
                            ids(appendId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testConcurrentRewriteManifest() throws IOException {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    // start a rewrite manifests that involves both manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // commit a rewrite manifests that only involves one manifest
    table.rewriteManifests()
      .clusterBy(file -> "file")
      .rewriteIf(manifest -> {
        try (ManifestReader reader = ManifestReader.read(localInput(manifest.path()))) {
          return !reader.iterator().next().path().equals(FILE_A.path());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      })
      .commit();

    Assert.assertEquals(2, table.currentSnapshot().manifests().size());

    // commit the rewrite manifests in progress - this should perform a full rewrite as the manifest
    // with file B is no longer part of the snapshot
    rewrite.commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(1, manifests.size());

    // get the file order correct
    List<DataFile> files;
    List<Long> ids;
    try (ManifestReader reader = ManifestReader.read(localInput(manifests.get(0).path()))) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testAppendDuringRewriteManifest() {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start the rewrite manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // append a file
    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    Assert.assertEquals(2, table.currentSnapshot().manifests().size());

    // commit the rewrite manifests in progress
    rewrite.commit();

    // the rewrite should only affect the first manifest, so we will end up with 2 manifests even though we
    // have a single cluster key, rewritten one should be the first in the list

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    validateManifestEntries(manifests.get(0),
                            ids(appendIdA),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(manifests.get(1),
                            ids(appendIdB),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testRewriteManifestDuringAppend() {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start an append
    AppendFiles append = table.newFastAppend();
    append.appendFile(FILE_B).apply();

    // rewrite the manifests - only affects the first
    table.rewriteManifests()
      .clusterBy(file -> "file")
      .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    // commit the append in progress
    append.commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    // last append should be the first in the list

    validateManifestEntries(manifests.get(0),
                            ids(appendIdB),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(manifests.get(1),
                            ids(appendIdA),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testBasicManifestReplacement() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    ManifestFile firstNewManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);
    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(3, manifests.size());

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId(), secondSnapshot.snapshotId()),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @Test
  public void testManifestReplacementConcurrentAppend() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    ManifestFile firstNewManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
    Snapshot secondSnapshot = table.currentSnapshot();

    Assert.assertEquals(2, table.currentSnapshot().manifests().size());

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(3, manifests.size());

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId(), secondSnapshot.snapshotId()),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @Test
  public void testManifestReplacementConcurrentDelete() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.updateProperties()
        .set(MANIFEST_MERGE_ENABLED, "false")
        .commit();

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend()
        .appendFile(FILE_C)
        .appendFile(FILE_D)
        .commit();
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    ManifestFile firstNewManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newDelete()
        .deleteFile(FILE_C)
        .commit();
    long thirdSnapshotId = table.currentSnapshot().snapshotId();

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(3, manifests.size());

    validateSummary(snapshot, 1, 1, 2, 0);

    validateManifestEntries(
        manifests.get(0),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(thirdSnapshotId, secondSnapshotId),
        files(FILE_C, FILE_D),
        statuses(ManifestEntry.Status.DELETED, ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testManifestReplacementConcurrentConflictingDelete() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    ManifestFile firstNewManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));
    ManifestFile secondNewManifest = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_B));

    RewriteManifests rewriteManifests = table.rewriteManifests();
    rewriteManifests.deleteManifest(firstSnapshotManifest);
    rewriteManifests.addManifest(firstNewManifest);
    rewriteManifests.addManifest(secondNewManifest);

    table.newDelete()
        .deleteFile(FILE_A)
        .commit();

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "Manifest is missing",
        rewriteManifests::commit);
  }

  @Test
  public void testManifestReplacementCombinedWithRewrite() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();

    table.newFastAppend()
        .appendFile(FILE_D)
        .commit();

    Assert.assertEquals(4, Iterables.size(table.snapshots()));

    ManifestFile newManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));

    table.rewriteManifests()
        .deleteManifest(firstSnapshotManifest)
        .addManifest(newManifest)
        .clusterBy(dataFile -> "const-value")
        .rewriteIf(manifest -> {
          try (ManifestReader reader = ManifestReader.read(localInput(manifest.path()))) {
            return !reader.iterator().next().path().equals(FILE_B.path());
          } catch (IOException x) {
            throw new RuntimeIOException(x);
          }
        })
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(3, manifests.size());

    validateSummary(snapshot, 3, 1, 2, 2);

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(2),
        ids(secondSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testManifestReplacementCombinedWithRewriteConcurrentDelete() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.updateProperties()
        .set(MANIFEST_MERGE_ENABLED, "false")
        .commit();

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Snapshot firstSnapshot = table.currentSnapshot();
    List<ManifestFile> firstSnapshotManifests = firstSnapshot.manifests();
    Assert.assertEquals(1, firstSnapshotManifests.size());
    ManifestFile firstSnapshotManifest = firstSnapshotManifests.get(0);

    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    Snapshot secondSnapshot = table.currentSnapshot();

    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();

    Assert.assertEquals(3, Iterables.size(table.snapshots()));

    ManifestFile newManifest = writeManifest(
        "manifest-file-1.avro",
        manifestEntry(ManifestEntry.Status.EXISTING, firstSnapshot.snapshotId(), FILE_A));

    RewriteManifests rewriteManifests = table.rewriteManifests()
        .deleteManifest(firstSnapshotManifest)
        .addManifest(newManifest)
        .clusterBy(dataFile -> "const-value");

    rewriteManifests.apply();

    table.newDelete()
        .deleteFile(FILE_C)
        .commit();

    rewriteManifests.commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(2, manifests.size());

    validateSummary(snapshot, 3, 0, 2, 1);

    validateManifestEntries(
        manifests.get(0),
        ids(secondSnapshot.snapshotId()),
        files(FILE_B),
        statuses(ManifestEntry.Status.EXISTING));

    validateManifestEntries(
        manifests.get(1),
        ids(firstSnapshot.snapshotId()),
        files(FILE_A),
        statuses(ManifestEntry.Status.EXISTING));
  }

  @Test
  public void testInvalidUsage() throws IOException {
    Assert.assertNull("Table should be empty", table.currentSnapshot());

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    Snapshot snapshot = table.currentSnapshot();
    List<ManifestFile> manifests = snapshot.manifests();
    Assert.assertEquals(1, manifests.size());
    ManifestFile manifest = manifests.get(0);

    ManifestFile invalidAddedFileManifest = writeManifest(
        "manifest-file-2.avro",
        manifestEntry(ManifestEntry.Status.ADDED, snapshot.snapshotId(), FILE_A));

    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest: Invalid manifest",
        () -> table.rewriteManifests()
            .deleteManifest(manifest)
            .addManifest(invalidAddedFileManifest)
            .commit());

    ManifestFile invalidDeletedFileManifest = writeManifest(
        "manifest-file-3.avro",
        manifestEntry(ManifestEntry.Status.DELETED, snapshot.snapshotId(), FILE_A));

    AssertHelpers.assertThrows("Should reject commit",
        IllegalArgumentException.class, "Cannot append manifest: Invalid manifest",
        () -> table.rewriteManifests()
            .deleteManifest(manifest)
            .addManifest(invalidDeletedFileManifest)
            .commit());

    AssertHelpers.assertThrows("Should reject commit",
        ValidationException.class, "must have the same number of active files",
        () -> table.rewriteManifests()
            .deleteManifest(manifest)
            .commit());
  }

  private void validateSummary(Snapshot snapshot, int replaced, int kept, int created, int entryCount) {
    Map<String, String> summary = snapshot.summary();
    Assert.assertEquals(
        "Replaced manifest count should match",
        replaced, Integer.parseInt(summary.get("manifests-replaced")));
    Assert.assertEquals(
        "Kept manifest count should match",
        kept, Integer.parseInt(summary.get("manifests-kept")));
    Assert.assertEquals(
        "Created manifest count should match",
        created, Integer.parseInt(summary.get("manifests-created")));
    Assert.assertEquals(
        "Entry count should match",
        entryCount, Integer.parseInt(summary.get("entries-processed")));
  }
}
