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

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.util.PathUtil;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.Files.localInput;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestRewriteManifests extends TableTestBase {

  @Test
  public void testReplaceManifestsSeparate() {
    Table table = load();
    table.newFastAppend()
      .appendFile(fileA)
      .appendFile(fileB)
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
                            files(fileA),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
    validateManifestEntries(manifests.get(1),
                            ids(appendId),
                            files(fileB),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
  }

  @Test
  public void testReplaceManifestsConsolidate() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(fileA)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend()
      .appendFile(fileB)
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
    try (ManifestReader reader =
             ManifestReader.read(localInput(PathUtil.getAbsolutePath(table.location(), manifests.get(0).path())))) {
      if (reader.iterator().next().path().equals(fileA.path())) {
        files = Arrays.asList(fileA, fileB);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(fileB, fileA);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING),
                            table.location());
  }

  @Test
  public void testReplaceManifestsWithFilter() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(fileA)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    table.newFastAppend()
      .appendFile(fileB)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    table.newFastAppend()
      .appendFile(fileC)
      .commit();
    long appendIdC = table.currentSnapshot().snapshotId();

    Assert.assertEquals(3, table.currentSnapshot().manifests().size());

    //keep the file A manifest, combine the other two

    table.rewriteManifests()
      .clusterBy(file -> "file")
      .rewriteIf(manifest -> {
        try (ManifestReader reader =
               ManifestReader.read(localInput(PathUtil.getAbsolutePath(table.location(), manifest.path())))) {
          return !reader.iterator().next().path().equals(fileA.path());
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
    try (ManifestReader reader =
           ManifestReader.read(localInput(PathUtil.getAbsolutePath(table.location(), manifests.get(0).path())))) {
      if (reader.iterator().next().path().equals(fileB.path())) {
        files = Arrays.asList(fileB, fileC);
        ids = Arrays.asList(appendIdB, appendIdC);
      } else {
        files = Arrays.asList(fileC, fileB);
        ids = Arrays.asList(appendIdC, appendIdB);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING),
                            table.location());
    validateManifestEntries(manifests.get(1),
                            ids(appendIdA),
                            files(fileA),
                            statuses(ManifestEntry.Status.ADDED),
                            table.location());
  }

  @Test
  public void testReplaceManifestsMaxSize() {
    Table table = load();
    table.newFastAppend()
      .appendFile(fileA)
      .appendFile(fileB)
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
                            files(fileA),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
    validateManifestEntries(manifests.get(1),
                            ids(appendId),
                            files(fileB),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
  }

  @Test
  public void testConcurrentRewriteManifest() throws IOException {
    Table table = load();
    table.newFastAppend()
      .appendFile(fileA)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();
    table.newFastAppend()
      .appendFile(fileB)
      .commit();
    long appendIdB = table.currentSnapshot().snapshotId();

    // start a rewrite manifests that involves both manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // commit a rewrite manifests that only involves one manifest
    table.rewriteManifests()
      .clusterBy(file -> "file")
      .rewriteIf(manifest -> {
        try (ManifestReader reader =
               ManifestReader.read(localInput(PathUtil.getAbsolutePath(table.location(), manifest.path())))) {
          return !reader.iterator().next().path().equals(fileA.path());
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
    try (ManifestReader reader =
           ManifestReader.read(localInput(PathUtil.getAbsolutePath(table.location(), manifests.get(0).path())))) {
      if (reader.iterator().next().path().equals(fileA.path())) {
        files = Arrays.asList(fileA, fileB);
        ids = Arrays.asList(appendIdA, appendIdB);
      } else {
        files = Arrays.asList(fileB, fileA);
        ids = Arrays.asList(appendIdB, appendIdA);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids.iterator(),
                            files.iterator(),
                            statuses(ManifestEntry.Status.EXISTING, ManifestEntry.Status.EXISTING),
                            table.location());
  }

  @Test
  public void testAppendDuringRewriteManifest() {
    Table table = load();
    table.newFastAppend()
      .appendFile(fileA)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start the rewrite manifests
    RewriteManifests rewrite = table.rewriteManifests();
    rewrite.clusterBy(file -> "file").apply();

    // append a file
    table.newFastAppend()
      .appendFile(fileB)
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
                            files(fileA),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
    validateManifestEntries(manifests.get(1),
                            ids(appendIdB),
                            files(fileB),
                            statuses(ManifestEntry.Status.ADDED),
                            table.location());
  }

  @Test
  public void testRewriteManifestDuringAppend() {
    Table table = load();
    table.newFastAppend()
      .appendFile(fileA)
      .commit();
    long appendIdA = table.currentSnapshot().snapshotId();

    // start an append
    AppendFiles append = table.newFastAppend();
    append.appendFile(fileB).apply();

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
                            files(fileB),
                            statuses(ManifestEntry.Status.ADDED),
                            table.location());
    validateManifestEntries(manifests.get(1),
                            ids(appendIdA),
                            files(fileA),
                            statuses(ManifestEntry.Status.EXISTING),
                            table.location());
  }
}
