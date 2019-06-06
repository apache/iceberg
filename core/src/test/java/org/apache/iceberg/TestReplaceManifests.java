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
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.Files.localInput;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestReplaceManifests extends TableTestBase {

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
    validateManifestEntries(table.currentSnapshot().manifests().get(1),
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
    ReplaceManifests rewriteManifests = spy((ReplaceManifests) table.rewriteManifests());
    when(rewriteManifests.getManifestTargetSizeBytes()).thenReturn(1L);
    rewriteManifests.clusterBy(file -> "file").commit();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(manifests.get(0),
                            ids(appendId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.EXISTING));
    validateManifestEntries(table.currentSnapshot().manifests().get(1),
                            ids(appendId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.EXISTING));
  }
}
