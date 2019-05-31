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

import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class TestReplaceManifests extends TableTestBase {

  @Test
  public void testReplaceManifestsEmptyTable() {
    Table table = load();
    table.newRewriteManifests()
      .appendFile(FILE_A, "file")
      .commit();

    long rewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(1, manifests.size());

    validateManifestEntries(manifests.get(0),
                            ids(rewriteId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsMultipleKeys() {
    Table table = load();

    // add 2 files using a different key
    table.newRewriteManifests()
      .appendFile(FILE_A, "fileA")
      .appendFile(FILE_B, "fileB")
      .commit();

    long rewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    validateManifestEntries(manifests.get(1),
                            ids(rewriteId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(table.currentSnapshot().manifests().get(0),
                            ids(rewriteId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsOverwrite() {
    Table table = load();

    // setup table with file A
    table.newRewriteManifests()
      .appendFile(FILE_A, "file")
      .commit();

    // add files B and C, using the same key
    // also drop file A
    table.newRewriteManifests()
      .appendFile(FILE_B, "file")
      .appendFile(FILE_C, "file")
      .commit();

    long rewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(1, manifests.size());

    validateManifestEntries(manifests.get(0),
                            ids(rewriteId, rewriteId),
                            files(FILE_B, FILE_C),
                            statuses(ManifestEntry.Status.ADDED, ManifestEntry.Status.ADDED));
  }

  @Test
  public void testReplaceManifestsKeepExisting() {
    Table table = load();

    // setup table with file A
    table.newRewriteManifests()
      .appendFile(FILE_A, "file")
      .commit();

    long firstRewriteId = table.currentSnapshot().snapshotId();
    ManifestFile firstManifest = table.currentSnapshot().manifests().get(0);

    // add files B and C, using the same key
    // also drop file A
    table.newRewriteManifests()
      .keepManifest(firstManifest)
      .appendFile(FILE_B, "file")
      .commit();

    long secondRewriteId = table.currentSnapshot().snapshotId();

    List<ManifestFile> manifests = table.currentSnapshot().manifests();
    Assert.assertEquals(2, manifests.size());

    validateManifestEntries(manifests.get(1),
                            ids(firstRewriteId),
                            files(FILE_A),
                            statuses(ManifestEntry.Status.ADDED));
    validateManifestEntries(manifests.get(0),
                            ids(secondRewriteId),
                            files(FILE_B),
                            statuses(ManifestEntry.Status.ADDED));
  }

}
