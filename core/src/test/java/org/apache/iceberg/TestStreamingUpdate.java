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

import static org.apache.iceberg.ManifestEntry.Status.ADDED;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;

import java.io.File;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestStreamingUpdate extends TableTestBase {

  private final String branch;

  @Parameterized.Parameters(name = "formatVersion = {0}, branch = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {2, "main"},
      new Object[] {2, "testBranch"}
    };
  }

  public TestStreamingUpdate(int formatVersion, String branch) {
    super(formatVersion);
    this.branch = branch;
  }

  @Test
  public void testAddBatches() {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    StreamingUpdate streamingUpdate =
        table
            .newStreamingUpdate()
            .addFile(FILE_A)
            .addFile(FILE_A_DELETES)
            .addFile(FILE_A2)
            .newBatch()
            .newBatch() // Extra call to new batch shouldn't mess things up
            .addFile(FILE_B)
            .newBatch()
            .addFile(FILE_C)
            .addFile(FILE_C2_DELETES);

    commit(table, streamingUpdate, branch);

    TableMetadata base = readMetadata();
    Snapshot snapshot = latestSnapshot(base, branch);
    long snapshotId = snapshot.snapshotId();
    long snapshotSequenceNumber = snapshot.sequenceNumber();
    Assert.assertEquals(
        "Should create only 2 manifests (1 write 1 delete)",
        2,
        snapshot.allManifests(table.io()).size());

    ManifestFile dataManifest = snapshot.allManifests(table.io()).get(0);
    validateManifestEntries(
        dataManifest,
        ids(snapshotId, snapshotId, snapshotId, snapshotId),
        files(FILE_A, FILE_A2, FILE_B, FILE_C),
        statuses(ADDED, ADDED, ADDED, ADDED),
        dataSeqs(1L, 1L, 2L, 3L));

    ManifestFile deleteManifest = snapshot.allManifests(table.io()).get(1);
    validateDeleteManifest(
        deleteManifest,
        dataSeqs(1L, 3L),
        fileSeqs(snapshotSequenceNumber, snapshotSequenceNumber),
        ids(snapshotId, snapshotId),
        files(FILE_A_DELETES, FILE_C2_DELETES),
        statuses(ADDED, ADDED));
  }

  @Test
  public void testFailureCleanup() {

    table.ops().failCommits(5);

    StreamingUpdate streamingUpdate = table.newStreamingUpdate();
    streamingUpdate.addFile(FILE_A);
    streamingUpdate.addFile(FILE_A_DELETES);

    Snapshot pending = apply(streamingUpdate, branch);

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests(table.io()).size());
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    Assertions.assertThatThrownBy(() -> commit(table, streamingUpdate, branch))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    Assert.assertFalse("Should clean up new manifest", new File(manifest1.path()).exists());
    Assert.assertFalse("Should clean up new manifest", new File(manifest2.path()).exists());

    // As commit failed all the manifests added with streaming update should be cleaned up
    Assert.assertEquals("No manifests should remain", 0, listManifestFiles().size());
  }

  @Test
  public void testRecovery() {

    table.ops().failCommits(3);

    StreamingUpdate streamingUpdate =
        table.newStreamingUpdate().addFile(FILE_A).addFile(FILE_A_DELETES);
    Snapshot pending = apply(streamingUpdate, branch);

    Assert.assertEquals("Should produce 2 manifests", 2, pending.allManifests(table.io()).size());
    ManifestFile manifest1 = pending.allManifests(table.io()).get(0);
    ManifestFile manifest2 = pending.allManifests(table.io()).get(1);

    validateManifestEntries(manifest1, ids(pending.snapshotId()), files(FILE_A), statuses(ADDED));
    validateDeleteManifest(
        manifest2,
        dataSeqs(pending.sequenceNumber()),
        fileSeqs(pending.sequenceNumber()),
        ids(pending.snapshotId()),
        files(FILE_A_DELETES),
        statuses(ADDED));

    commit(table, streamingUpdate, branch);

    Assert.assertTrue("Should reuse the manifest for appends", new File(manifest1.path()).exists());
    Assert.assertTrue(
        "Should reuse the manifest with deletes", new File(manifest2.path()).exists());

    TableMetadata metadata = readMetadata();
    Assert.assertTrue(
        "Should commit the manifest for append",
        latestSnapshot(metadata, branch).allManifests(table.io()).contains(manifest1));
    Assert.assertTrue(
        "Should commit the manifest for delete",
        latestSnapshot(metadata, branch).allManifests(table.io()).contains(manifest2));

    // 1 for data file 1 for delete file
    Assert.assertEquals("Only 2 manifests should exist", 2, listManifestFiles().size());
  }
}
