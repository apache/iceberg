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

public class TestReplaceManifests extends TableTestBase {

  @Test
  public void testReplaceManifestsSeparate() {
    Table table = load();
    table.newFastAppend()
      .appendFile(FILE_A)
      .appendFile(FILE_B)
      .commit();

    Assert.assertEquals(1, table.currentSnapshot().manifests().size());

    // cluster by path will split the manifest into two

    table.newRewriteManifests()
      .clusterBy(file -> file.path())
      .commit();

    long rewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());
    manifests.sort(Comparator.comparing(ManifestFile::path));

    validateManifestEntries(manifests.get(0),
                            ids(rewriteId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(table.currentSnapshot().manifests().get(1),
                            ids(rewriteId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsConsolidate() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();

    Assert.assertEquals(2, table.currentSnapshot().manifests().size());

    // cluster by constant will combine manifests into one

    table.newRewriteManifests()
      .clusterBy(file -> "file")
      .commit();

    long rewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(1, manifests.size());

    // get the file order correct
    List<DataFile> files;
    try (ManifestReader reader = ManifestReader.read(localInput(manifests.get(0).path()))) {
      if (reader.iterator().next().path().equals(FILE_A.path())) {
        files = Arrays.asList(FILE_A, FILE_B);
      } else {
        files = Arrays.asList(FILE_B, FILE_A);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids(rewriteId, rewriteId),
                            files.iterator(),
                            statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsWithFilter() throws IOException {
    Table table = load();

    table.newFastAppend()
      .appendFile(FILE_A)
      .commit();
    long firstRewriteId = table.currentSnapshot().snapshotId();

    table.newFastAppend()
      .appendFile(FILE_B)
      .commit();
    table.newFastAppend()
      .appendFile(FILE_C)
      .commit();

    Assert.assertEquals(3, table.currentSnapshot().manifests().size());

    //keep the file A manifest, combine the other two

    table.newRewriteManifests()
      .clusterBy(file -> "file")
      .filter(manifest -> {
        try (ManifestReader reader = ManifestReader.read(localInput(manifest.path()))) {
          return !reader.iterator().next().path().equals(FILE_A.path());
        } catch (IOException x) {
          throw new RuntimeIOException(x);
        }
      })
      .commit();

    long secondRewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    // get the file order correct
    List<DataFile> files;
    try (ManifestReader reader = ManifestReader.read(localInput(manifests.get(1).path()))) {
      if (reader.iterator().next().path().equals(FILE_B.path())) {
        files = Arrays.asList(FILE_B, FILE_C);
      } else {
        files = Arrays.asList(FILE_C, FILE_B);
      }
    }

    validateManifestEntries(manifests.get(0),
                            ids(firstRewriteId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(manifests.get(1),
                            ids(secondRewriteId, secondRewriteId),
                            files.iterator(),
                            statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

}
