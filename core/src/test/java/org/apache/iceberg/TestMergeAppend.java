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

import static org.apache.iceberg.relocated.com.google.common.collect.Iterators.concat;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMergeAppend extends TestBase {
  @Parameter(index = 1)
  private String branch;

  @Parameters(name = "formatVersion = {0}, branch = {1}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {1, "main"},
        new Object[] {1, "testBranch"},
        new Object[] {2, "main"},
        new Object[] {2, "testBranch"});
  }

  @TestTemplate
  public void testEmptyTableAppend() {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    Snapshot committedSnapshot =
        commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    assertThat(committedSnapshot).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    assertThat(committedSnapshot.allManifests(table.io())).hasSize(1);

    long snapshotId = committedSnapshot.snapshotId();

    validateManifest(
        committedSnapshot.allManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testEmptyTableAppendManifest() throws IOException {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot committedSnapshot = commit(table, table.newAppend().appendManifest(manifest), branch);

    assertThat(committedSnapshot).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    List<ManifestFile> manifests = committedSnapshot.allManifests(table.io());
    ManifestFile committedManifest = Iterables.getOnlyElement(manifests);
    if (formatVersion == 1) {
      assertThat(committedManifest.path()).isNotEqualTo(manifest.path());
    } else {
      assertThat(committedManifest.path()).isEqualTo(manifest.path());
    }

    long snapshotId = committedSnapshot.snapshotId();
    validateManifest(
        committedManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    assertThat(committedSnapshot.summary().get("added-data-files")).isEqualTo("2");
  }

  @TestTemplate
  public void testEmptyTableAppendFilesAndManifest() throws IOException {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot committedSnapshot =
        commit(
            table,
            table.newAppend().appendFile(FILE_C).appendFile(FILE_D).appendManifest(manifest),
            branch);

    assertThat(committedSnapshot).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());
    assertThat(committedSnapshot.allManifests(table.io())).hasSize(2);

    long snapshotId = committedSnapshot.snapshotId();

    ManifestFile committedManifest1 = committedSnapshot.allManifests(table.io()).get(0);
    ManifestFile committedManifest2 = committedSnapshot.allManifests(table.io()).get(1);

    if (formatVersion == 1) {
      assertThat(committedManifest2.path()).isNotEqualTo(manifest.path());
    } else {
      assertThat(committedManifest2.path()).isEqualTo(manifest.path());
    }

    validateManifest(
        committedManifest1,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    validateManifest(
        committedManifest2,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testAppendWithManifestScanExecutor() {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);
    AtomicInteger scanThreadsIndex = new AtomicInteger(0);
    Snapshot snapshot =
        commit(
            table,
            table
                .newAppend()
                .appendFile(FILE_A)
                .appendFile(FILE_B)
                .scanManifestsWith(
                    Executors.newFixedThreadPool(
                        1,
                        runnable -> {
                          Thread thread = new Thread(runnable);
                          thread.setName("scan-" + scanThreadsIndex.getAndIncrement());
                          thread.setDaemon(
                              true); // daemon threads will be terminated abruptly when the JVM
                          // exits
                          return thread;
                        })),
            branch);
    assertThat(scanThreadsIndex.get())
        .as("Thread should be created in provided pool")
        .isGreaterThan(0);
    assertThat(snapshot).isNotNull();
  }

  @TestTemplate
  public void testMergeWithAppendFilesAndManifest() throws IOException {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot committedSnapshot =
        commit(
            table,
            table.newAppend().appendFile(FILE_C).appendFile(FILE_D).appendManifest(manifest),
            branch);

    assertThat(committedSnapshot).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    long snapshotId = committedSnapshot.snapshotId();

    List<ManifestFile> manifests = committedSnapshot.allManifests(table.io());
    ManifestFile committedManifest = Iterables.getOnlyElement(manifests);

    assertThat(committedManifest.path()).isNotEqualTo(manifest.path());

    validateManifest(
        committedManifest,
        dataSeqs(1L, 1L, 1L, 1L),
        fileSeqs(1L, 1L, 1L, 1L),
        ids(snapshotId, snapshotId, snapshotId, snapshotId),
        files(FILE_C, FILE_D, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testMergeWithExistingManifest() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();
    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot commitBefore =
        commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    assertThat(commitBefore).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    TableMetadata base = readMetadata();
    long baseId = commitBefore.snapshotId();
    validateSnapshot(null, commitBefore, 1, FILE_A, FILE_B);

    assertThat(commitBefore.allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = commitBefore.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    Snapshot committedAfter =
        commit(table, table.newAppend().appendFile(FILE_C).appendFile(FILE_D), branch);
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, table.ops().current().lastSequenceNumber());

    assertThat(committedAfter.allManifests(table.io())).hasSize(1);
    ManifestFile newManifest = committedAfter.allManifests(table.io()).get(0);
    assertThat(newManifest).isNotEqualTo(initialManifest);

    long snapshotId = committedAfter.snapshotId();

    validateManifest(
        newManifest,
        dataSeqs(2L, 2L, 1L, 1L),
        fileSeqs(2L, 2L, 1L, 1L),
        ids(snapshotId, snapshotId, baseId, baseId),
        concat(files(FILE_C, FILE_D), files(initialManifest)),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING, Status.EXISTING));
  }

  @TestTemplate
  public void testManifestMergeMinCount() throws IOException {
    assertThat(listManifestFiles()).isEmpty();
    table
        .updateProperties()
        .set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "2")
        // Each initial v1/v2 ManifestFile is 5661/6397 bytes respectively. Merging two of the given
        // manifests make one v1/v2 ManifestFile of 5672/6408 bytes respectively, so 15000 bytes
        // limit will give us two bins with three manifest/data files.
        .set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "15000")
        .commit();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifest = writeManifestWithName("FILE_A", FILE_A);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    Snapshot snap1 =
        commit(
            table,
            table
                .newAppend()
                .appendManifest(manifest)
                .appendManifest(manifest2)
                .appendManifest(manifest3),
            branch);

    long commitId1 = snap1.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    assertThat(snap1.allManifests(table.io())).hasSize(2);
    validateManifest(
        snap1.allManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));
    validateManifest(
        snap1.allManifests(table.io()).get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(commitId1, commitId1),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    // produce new manifests as the old ones could have been compacted
    manifest = writeManifestWithName("FILE_A_S2", FILE_A);
    manifest2 = writeManifestWithName("FILE_C_S2", FILE_C);
    manifest3 = writeManifestWithName("FILE_D_S2", FILE_D);

    Snapshot snap2 =
        commit(
            table,
            table
                .newAppend()
                .appendManifest(manifest)
                .appendManifest(manifest2)
                .appendManifest(manifest3),
            branch);
    long commitId2 = snap2.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    assertThat(snap2.allManifests(table.io())).hasSize(3);
    validateManifest(
        snap2.allManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(commitId2),
        files(FILE_A),
        statuses(Status.ADDED));
    validateManifest(
        snap2.allManifests(table.io()).get(1),
        dataSeqs(2L, 2L),
        fileSeqs(2L, 2L),
        ids(commitId2, commitId2),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));
    validateManifest(
        snap2.allManifests(table.io()).get(2),
        dataSeqs(1L, 1L, 1L),
        fileSeqs(1L, 1L, 1L),
        ids(commitId1, commitId1, commitId1),
        files(FILE_A, FILE_C, FILE_D),
        statuses(Status.EXISTING, Status.EXISTING, Status.EXISTING));

    // validate that the metadata summary is correct when using appendManifest
    assertThat(snap2.summary().get("added-data-files")).isEqualTo("3");
  }

  @TestTemplate
  public void testManifestsMergeIntoOne() throws IOException {
    assertThat(listManifestFiles()).isEmpty();
    Snapshot snap1 = commit(table, table.newAppend().appendFile(FILE_A), branch);
    TableMetadata base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap1.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());
    long commitId1 = snap1.snapshotId();

    assertThat(snap1.allManifests(table.io())).hasSize(1);
    validateManifest(
        snap1.allManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));

    Snapshot snap2 = commit(table, table.newAppend().appendFile(FILE_B), branch);
    long commitId2 = snap2.snapshotId();
    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 2", 2, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    assertThat(snap2.allManifests(table.io())).hasSize(2);
    validateManifest(
        snap2.allManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(commitId2),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifest(
        snap2.allManifests(table.io()).get(1),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));

    Snapshot snap3 =
        commit(
            table,
            table
                .newAppend()
                .appendManifest(
                    writeManifest(
                        "input-m0.avro", manifestEntry(ManifestEntry.Status.ADDED, null, FILE_C))),
            branch);

    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, snap3.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 3", 3, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    assertThat(snap3.allManifests(table.io())).hasSize(3);
    long commitId3 = snap3.snapshotId();
    validateManifest(
        snap3.allManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(commitId3),
        files(FILE_C),
        statuses(Status.ADDED));
    validateManifest(
        snap3.allManifests(table.io()).get(1),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(commitId2),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifest(
        snap3.allManifests(table.io()).get(2),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(commitId1),
        files(FILE_A),
        statuses(Status.ADDED));

    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    Snapshot snap4 =
        commit(
            table,
            table
                .newAppend()
                .appendManifest(
                    writeManifest(
                        "input-m1.avro", manifestEntry(ManifestEntry.Status.ADDED, null, FILE_D))),
            branch);

    base = readMetadata();
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, snap4.sequenceNumber());
    V2Assert.assertEquals("Last sequence number should be 4", 4, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    long commitId4 = snap4.snapshotId();
    assertThat(snap4.allManifests(table.io())).hasSize(1);
    validateManifest(
        snap4.allManifests(table.io()).get(0),
        dataSeqs(4L, 3L, 2L, 1L),
        fileSeqs(4L, 3L, 2L, 1L),
        ids(commitId4, commitId3, commitId2, commitId1),
        files(FILE_D, FILE_C, FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING, Status.EXISTING, Status.EXISTING));
  }

  @TestTemplate
  public void testManifestDoNotMergeMinCount() throws IOException {
    assertThat(listManifestFiles()).isEmpty();
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    ManifestFile manifest2 = writeManifestWithName("FILE_C", FILE_C);
    ManifestFile manifest3 = writeManifestWithName("FILE_D", FILE_D);
    Snapshot committed =
        commit(
            table,
            table
                .newAppend()
                .appendManifest(manifest)
                .appendManifest(manifest2)
                .appendManifest(manifest3),
            branch);

    assertThat(committed).isNotNull();
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, table.ops().current().lastSequenceNumber());

    List<ManifestFile> manifests = committed.allManifests(table.io());
    assertThat(manifests).hasSize(3);

    ManifestFile committedManifest = manifests.get(0);
    ManifestFile committedManifest2 = manifests.get(1);
    ManifestFile committedManifest3 = manifests.get(2);

    long snapshotId = committed.snapshotId();

    if (formatVersion == 1) {
      assertThat(committedManifest.path()).isNotEqualTo(manifest.path());
      assertThat(committedManifest2.path()).isNotEqualTo(manifest2.path());
      assertThat(committedManifest3.path()).isNotEqualTo(manifest3.path());
    } else {
      assertThat(committedManifest.path()).isEqualTo(manifest.path());
      assertThat(committedManifest2.path()).isEqualTo(manifest2.path());
      assertThat(committedManifest3.path()).isEqualTo(manifest3.path());
    }

    validateManifest(
        committedManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
    validateManifest(
        committedManifest2,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snapshotId),
        files(FILE_C),
        statuses(Status.ADDED));
    validateManifest(
        committedManifest3,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(snapshotId),
        files(FILE_D),
        statuses(Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    assertThat(committed.summary().get("added-data-files")).isEqualTo("4");
  }

  @TestTemplate
  public void testMergeWithExistingManifestAfterDelete() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot snap = commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    long baseId = snap.snapshotId();
    assertThat(snap.allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = snap.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    Snapshot deleteSnapshot = commit(table, table.newDelete().deleteFile(FILE_A), branch);

    V2Assert.assertEquals(
        "Snapshot sequence number should be 2", 2, deleteSnapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    TableMetadata delete = readMetadata();
    long deleteId = latestSnapshot(table, branch).snapshotId();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);
    ManifestFile deleteManifest = deleteSnapshot.allManifests(table.io()).get(0);

    validateManifest(
        deleteManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(deleteId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));

    Snapshot committedSnapshot =
        commit(table, table.newAppend().appendFile(FILE_C).appendFile(FILE_D), branch);

    V2Assert.assertEquals(
        "Snapshot sequence number should be 3", 3, committedSnapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(committedSnapshot.allManifests(table.io())).hasSize(1);
    ManifestFile newManifest = committedSnapshot.allManifests(table.io()).get(0);
    assertThat(newManifest).isNotEqualTo(initialManifest);

    long snapshotId = committedSnapshot.snapshotId();

    // the deleted entry from the previous manifest should be removed
    validateManifestEntries(
        newManifest,
        ids(snapshotId, snapshotId, baseId),
        files(FILE_C, FILE_D, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING));
  }

  @TestTemplate
  public void testMinMergeCount() {
    // only merge when there are at least 4 manifests
    table.updateProperties().set("commit.manifest.min-count-to-merge", "4").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot snap1 = commit(table, table.newFastAppend().appendFile(FILE_A), branch);
    long idFileA = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A);

    Snapshot snap2 = commit(table, table.newFastAppend().appendFile(FILE_B), branch);
    long idFileB = snap2.snapshotId();
    validateSnapshot(snap1, snap2, 2, FILE_B);

    assertThat(snap2.allManifests(table.io())).hasSize(2);

    Snapshot snap3 = commit(table, table.newAppend().appendFile(FILE_C), branch);
    long idFileC = snap3.snapshotId();
    validateSnapshot(snap2, snap3, 3, FILE_C);

    TableMetadata base = readMetadata();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(3);
    Set<ManifestFile> unmerged =
        Sets.newHashSet(latestSnapshot(table, branch).allManifests(table.io()));

    Snapshot committed = commit(table, table.newAppend().appendFile(FILE_D), branch);
    V2Assert.assertEquals("Snapshot sequence number should be 4", 4, committed.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 4", 4, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(committed.allManifests(table.io())).hasSize(1);

    ManifestFile newManifest = committed.allManifests(table.io()).get(0);
    assertThat(unmerged).doesNotContain(newManifest);

    long lastSnapshotId = committed.snapshotId();

    validateManifest(
        newManifest,
        dataSeqs(4L, 3L, 2L, 1L),
        fileSeqs(4L, 3L, 2L, 1L),
        ids(lastSnapshotId, idFileC, idFileB, idFileA),
        files(FILE_D, FILE_C, FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING, Status.EXISTING, Status.EXISTING));
  }

  @TestTemplate
  public void testMergeSizeTargetWithExistingManifest() {
    // use a small limit on manifest size to prevent merging
    table.updateProperties().set(TableProperties.MANIFEST_TARGET_SIZE_BYTES, "10").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot snap = commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    long baseId = snap.snapshotId();
    assertThat(snap.allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = snap.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    Snapshot committed =
        commit(table, table.newAppend().appendFile(FILE_C).appendFile(FILE_D), branch);

    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, committed.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(committed.allManifests(table.io())).hasSize(2);
    ManifestFile newManifest = committed.allManifests(table.io()).get(0);
    assertThat(newManifest).isNotEqualTo(initialManifest);

    long pendingId = committed.snapshotId();
    validateManifest(
        newManifest,
        dataSeqs(2L, 2L),
        fileSeqs(2L, 2L),
        ids(pendingId, pendingId),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    validateManifest(
        committed.allManifests(table.io()).get(1),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseId, baseId),
        files(initialManifest),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testChangedPartitionSpec() {
    Snapshot snap = commit(table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    long commitId = snap.snapshotId();
    validateSnapshot(null, snap, 1, FILE_A, FILE_B);

    TableMetadata base = readMetadata();
    assertThat(snap.allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = snap.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(commitId, commitId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema()).bucket("data", 16).bucket("id", 4).build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    Snapshot snap2 = latestSnapshot(table, branch);
    V2Assert.assertEquals("Snapshot sequence number should be 1", 1, snap2.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    DataFile newFileY =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-y.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=3")
            .withRecordCount(1)
            .build();

    Snapshot lastSnapshot = commit(table, table.newAppend().appendFile(newFileY), branch);

    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, lastSnapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(lastSnapshot.allManifests(table.io())).hasSize(2);

    // new manifest comes first
    validateManifest(
        lastSnapshot.allManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(lastSnapshot.snapshotId()),
        files(newFileY),
        statuses(Status.ADDED));

    assertThat(lastSnapshot.allManifests(table.io()))
        .as("Second manifest should be the initial manifest with the old spec")
        .element(1)
        .isEqualTo(initialManifest);
  }

  @TestTemplate
  public void testChangedPartitionSpecMergeExisting() {
    Snapshot snap1 = commit(table, table.newAppend().appendFile(FILE_A), branch);

    long id1 = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A);

    // create a second compatible manifest
    Snapshot snap2 = commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    long id2 = snap2.snapshotId();
    validateSnapshot(snap1, snap2, 2, FILE_B);

    TableMetadata base = readMetadata();
    assertThat(snap2.allManifests(table.io())).hasSize(2);
    ManifestFile manifest = snap2.allManifests(table.io()).get(0);

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema()).bucket("data", 16).bucket("id", 4).build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    Snapshot snap3 = latestSnapshot(table, branch);
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap3.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    DataFile newFileY =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-y.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=2/id_bucket=3")
            .withRecordCount(1)
            .build();

    Snapshot lastSnapshot = commit(table, table.newAppend().appendFile(newFileY), branch);
    V2Assert.assertEquals("Snapshot sequence number should be 3", 3, lastSnapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 3", 3, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(lastSnapshot.allManifests(table.io())).hasSize(2);
    assertThat(lastSnapshot.allManifests(table.io())).doesNotContain(manifest);

    validateManifest(
        lastSnapshot.allManifests(table.io()).get(0),
        dataSeqs(3L),
        fileSeqs(3L),
        ids(lastSnapshot.snapshotId()),
        files(newFileY),
        statuses(Status.ADDED));
    validateManifest(
        lastSnapshot.allManifests(table.io()).get(1),
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(id2, id1),
        files(FILE_B, FILE_A),
        statuses(Status.EXISTING, Status.EXISTING));
  }

  @TestTemplate
  public void testFailure() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot snap = commit(table, table.newAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = snap.snapshotId();
    V2Assert.assertEquals("Last sequence number should be 1", 1, base.lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, base.lastSequenceNumber());

    ManifestFile initialManifest = snap.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        files(FILE_A),
        statuses(Status.ADDED));

    table.ops().failCommits(5);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = apply(append, branch);

    assertThat(pending.allManifests(table.io())).hasSize(1);
    ManifestFile newManifest = pending.allManifests(table.io()).get(0);

    assertThat(new File(newManifest.path())).exists();
    validateManifest(
        newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    assertThatThrownBy(() -> commit(table, append, branch))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);

    validateManifest(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        files(initialManifest),
        statuses(Status.ADDED));

    assertThat(new File(newManifest.path())).doesNotExist();
  }

  @TestTemplate
  public void testAppendManifestCleanup() throws IOException {
    // inject 5 failures
    TestTables.TestTableOperations ops = table.ops();
    ops.failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    AppendFiles append = table.newAppend().appendManifest(manifest);
    Snapshot pending = apply(append, branch);
    ManifestFile newManifest = pending.allManifests(table.io()).get(0);
    assertThat(new File(newManifest.path())).exists();
    if (formatVersion == 1) {
      assertThat(newManifest.path()).isNotEqualTo(manifest.path());
    } else {
      assertThat(newManifest.path()).isEqualTo(manifest.path());
    }

    assertThatThrownBy(() -> commit(table, append, branch))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
    V2Assert.assertEquals(
        "Last sequence number should be 0", 0, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    if (formatVersion == 1) {
      assertThat(new File(newManifest.path())).doesNotExist();
    } else {
      assertThat(new File(newManifest.path())).exists();
    }
  }

  @TestTemplate
  public void testRecovery() {
    // merge all manifests for this test
    table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    Snapshot current = commit(table, table.newAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = current.snapshotId();
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());
    ManifestFile initialManifest = current.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        files(FILE_A),
        statuses(Status.ADDED));

    table.ops().failCommits(3);

    AppendFiles append = table.newAppend().appendFile(FILE_B);
    Snapshot pending = apply(append, branch);

    assertThat(pending.allManifests(table.io())).hasSize(1);
    ManifestFile newManifest = pending.allManifests(table.io()).get(0);

    assertThat(new File(newManifest.path())).exists();
    validateManifest(
        newManifest,
        ids(pending.snapshotId(), baseId),
        concat(files(FILE_B), files(initialManifest)));

    V2Assert.assertEquals(
        "Snapshot sequence number should be 1", 1, latestSnapshot(table, branch).sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    Snapshot snapshot = commit(table, append, branch);
    long snapshotId = snapshot.snapshotId();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    TableMetadata metadata = readMetadata();
    assertThat(new File(newManifest.path())).exists();
    assertThat(snapshot.allManifests(table.io())).containsExactly(newManifest);

    assertThat(snapshot.allManifests(table.io())).hasSize(1);
    ManifestFile manifestFile = snapshot.allManifests(table.io()).get(0);
    validateManifest(
        manifestFile,
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(snapshotId, baseId),
        files(FILE_B, FILE_A),
        statuses(Status.ADDED, Status.EXISTING));
  }

  @TestTemplate
  public void testAppendManifestWithSnapshotIdInheritance() throws IOException {
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    Snapshot snapshot = commit(table, table.newAppend().appendManifest(manifest), branch);

    long snapshotId = snapshot.snapshotId();
    validateSnapshot(null, snapshot, 1, FILE_A, FILE_B);

    List<ManifestFile> manifests = snapshot.allManifests(table.io());
    assertThat(manifests).hasSize(1);
    ManifestFile manifestFile = snapshot.allManifests(table.io()).get(0);
    assertThat(manifestFile.path()).isEqualTo(manifest.path());
    validateManifest(
        manifestFile,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    assertThat(snapshot.summary().get("added-data-files")).isEqualTo("2");
    assertThat(snapshot.summary().get("added-records")).isEqualTo("2");
    assertThat(snapshot.summary().get("total-data-files")).isEqualTo("2");
    assertThat(snapshot.summary().get("total-records")).isEqualTo("2");
  }

  @TestTemplate
  public void testMergedAppendManifestCleanupWithSnapshotIdInheritance() throws IOException {
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();

    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    ManifestFile manifest1 = writeManifestWithName("manifest-file-1.avro", FILE_A, FILE_B);
    Snapshot snap1 = commit(table, table.newAppend().appendManifest(manifest1), branch);

    long commitId1 = snap1.snapshotId();
    validateSnapshot(null, snap1, 1, FILE_A, FILE_B);

    assertThat(snap1.allManifests(table.io())).hasSize(1);
    validateManifest(
        snap1.allManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(commitId1, commitId1),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
    assertThat(new File(manifest1.path())).exists();

    ManifestFile manifest2 = writeManifestWithName("manifest-file-2.avro", FILE_C, FILE_D);
    Snapshot snap2 = commit(table, table.newAppend().appendManifest(manifest2), branch);

    long commitId2 = snap2.snapshotId();
    V2Assert.assertEquals("Snapshot sequence number should be 2", 2, snap2.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(snap2.allManifests(table.io())).hasSize(1);
    validateManifest(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        dataSeqs(2L, 2L, 1L, 1L),
        fileSeqs(2L, 2L, 1L, 1L),
        ids(commitId2, commitId2, commitId1, commitId1),
        files(FILE_C, FILE_D, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.EXISTING, Status.EXISTING));

    assertThat(new File(manifest2.path())).doesNotExist();
  }

  @TestTemplate
  public void testAppendManifestFailureWithSnapshotIdInheritance() throws IOException {
    table.updateProperties().set(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true").commit();

    assertThat(listManifestFiles()).isEmpty();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();

    table.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "1").commit();

    table.ops().failCommits(5);

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    AppendFiles append = table.newAppend();
    append.appendManifest(manifest);

    assertThatThrownBy(() -> commit(table, append, branch))
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");

    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);
    assertThat(new File(manifest.path())).exists();
  }

  @TestTemplate
  public void testInvalidAppendManifest() throws IOException {
    assertThat(listManifestFiles()).isEmpty();

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();

    ManifestFile manifestWithExistingFiles =
        writeManifest("manifest-file-1.avro", manifestEntry(Status.EXISTING, null, FILE_A));
    assertThatThrownBy(
            () ->
                commit(table, table.newAppend().appendManifest(manifestWithExistingFiles), branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot append manifest with existing files");
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    ManifestFile manifestWithDeletedFiles =
        writeManifest("manifest-file-2.avro", manifestEntry(Status.DELETED, null, FILE_A));
    assertThatThrownBy(
            () -> commit(table, table.newAppend().appendManifest(manifestWithDeletedFiles), branch))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot append manifest with deleted files");
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);
  }

  @TestTemplate
  public void testUpdatePartitionSpecFieldIdsForV1Table() {
    TableMetadata base = readMetadata();

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema())
            .bucket("id", 16)
            .identity("data")
            .bucket("data", 4)
            .bucket("data", 16, "data_partition") // reuse field id although different target name
            .build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    List<PartitionSpec> partitionSpecs = table.ops().current().specs();
    PartitionSpec partitionSpec = partitionSpecs.get(0);
    assertThat(partitionSpec.lastAssignedFieldId()).isEqualTo(1000);

    Types.StructType structType = partitionSpec.partitionType();
    List<Types.NestedField> fields = structType.fields();
    assertThat(fields).hasSize(1);
    assertThat(fields.get(0).name()).isEqualTo("data_bucket");
    assertThat(fields.get(0).fieldId()).isEqualTo(1000);

    partitionSpec = partitionSpecs.get(1);
    assertThat(partitionSpec.lastAssignedFieldId()).isEqualTo(1003);

    structType = partitionSpec.partitionType();
    fields = structType.fields();
    assertThat(fields).hasSize(4);
    assertThat(fields.get(0).name()).isEqualTo("id_bucket");
    assertThat(fields.get(0).fieldId()).isEqualTo(1000);
    assertThat(fields.get(1).name()).isEqualTo("data");
    assertThat(fields.get(1).fieldId()).isEqualTo(1001);
    assertThat(fields.get(2).name()).isEqualTo("data_bucket");
    assertThat(fields.get(2).fieldId()).isEqualTo(1002);
    assertThat(fields.get(3).name()).isEqualTo("data_partition");
    assertThat(fields.get(3).fieldId()).isEqualTo(1003);
  }

  @TestTemplate
  public void testManifestEntryFieldIdsForChangedPartitionSpecForV1Table() {
    Snapshot snap = commit(table, table.newAppend().appendFile(FILE_A), branch);

    long commitId = snap.snapshotId();
    validateSnapshot(null, snap, 1, FILE_A);
    TableMetadata base = readMetadata();

    assertThat(snap.allManifests(table.io())).hasSize(1);
    ManifestFile initialManifest = snap.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(commitId),
        files(FILE_A),
        statuses(Status.ADDED));

    // build the new spec using the table's schema, which uses fresh IDs
    PartitionSpec newSpec =
        PartitionSpec.builderFor(base.schema()).bucket("id", 8).bucket("data", 8).build();

    // commit the new partition spec to the table manually
    table.ops().commit(base, base.updatePartitionSpec(newSpec));
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    // create a new with the table's current spec
    DataFile newFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-x.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("id_bucket=1/data_bucket=1")
            .withRecordCount(1)
            .build();

    Snapshot committedSnapshot = commit(table, table.newAppend().appendFile(newFile), branch);

    V2Assert.assertEquals(
        "Snapshot sequence number should be 2", 2, committedSnapshot.sequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, readMetadata().lastSequenceNumber());
    V1Assert.assertEquals(
        "Table should end with last-sequence-number 0", 0, readMetadata().lastSequenceNumber());

    assertThat(committedSnapshot.allManifests(table.io())).hasSize(2);

    // new manifest comes first
    validateManifest(
        committedSnapshot.allManifests(table.io()).get(0),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(committedSnapshot.snapshotId()),
        files(newFile),
        statuses(Status.ADDED));

    assertThat(committedSnapshot.allManifests(table.io()))
        .as("Second manifest should be the initial manifest with the old spec")
        .element(1)
        .isEqualTo(initialManifest);

    // field ids of manifest entries in two manifests with different specs of the same source field
    // should be different
    ManifestEntry<DataFile> entry =
        ManifestFiles.read(committedSnapshot.allManifests(table.io()).get(0), FILE_IO)
            .entries()
            .iterator()
            .next();
    Types.NestedField field =
        ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    assertThat(field.fieldId()).isEqualTo(1000);
    assertThat(field.name()).isEqualTo("id_bucket");
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(1);
    assertThat(field.fieldId()).isEqualTo(1001);
    assertThat(field.name()).isEqualTo("data_bucket");

    entry =
        ManifestFiles.read(committedSnapshot.allManifests(table.io()).get(1), FILE_IO)
            .entries()
            .iterator()
            .next();
    field = ((PartitionData) entry.file().partition()).getPartitionType().fields().get(0);
    assertThat(field.fieldId()).isEqualTo(1000);
    assertThat(field.name()).isEqualTo("data_bucket");
  }

  @TestTemplate
  public void testDefaultPartitionSummaries() {
    table.newFastAppend().appendFile(FILE_A).commit();

    Set<String> partitionSummaryKeys =
        table.currentSnapshot().summary().keySet().stream()
            .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
            .collect(Collectors.toSet());
    assertThat(partitionSummaryKeys).isEmpty();

    String summariesIncluded =
        table
            .currentSnapshot()
            .summary()
            .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    assertThat(summariesIncluded).isEqualTo("false");

    String changedPartitions =
        table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    assertThat(changedPartitions).isEqualTo("1");
  }

  @TestTemplate
  public void testIncludedPartitionSummaries() {
    table.updateProperties().set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "1").commit();

    table.newFastAppend().appendFile(FILE_A).commit();

    Set<String> partitionSummaryKeys =
        table.currentSnapshot().summary().keySet().stream()
            .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
            .collect(Collectors.toSet());
    assertThat(partitionSummaryKeys).hasSize(1);

    String summariesIncluded =
        table
            .currentSnapshot()
            .summary()
            .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    assertThat(summariesIncluded).isEqualTo("true");

    String changedPartitions =
        table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    assertThat(changedPartitions).isEqualTo("1");

    String partitionSummary =
        table
            .currentSnapshot()
            .summary()
            .get(SnapshotSummary.CHANGED_PARTITION_PREFIX + "data_bucket=0");
    assertThat(partitionSummary)
        .isEqualTo("added-data-files=1,added-records=1,added-files-size=10");
  }

  @TestTemplate
  public void testIncludedPartitionSummaryLimit() {
    table.updateProperties().set(TableProperties.WRITE_PARTITION_SUMMARY_LIMIT, "1").commit();

    table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    Set<String> partitionSummaryKeys =
        table.currentSnapshot().summary().keySet().stream()
            .filter(key -> key.startsWith(SnapshotSummary.CHANGED_PARTITION_PREFIX))
            .collect(Collectors.toSet());
    assertThat(partitionSummaryKeys).isEmpty();

    String summariesIncluded =
        table
            .currentSnapshot()
            .summary()
            .getOrDefault(SnapshotSummary.PARTITION_SUMMARY_PROP, "false");
    assertThat(summariesIncluded).isEqualTo("false");

    String changedPartitions =
        table.currentSnapshot().summary().get(SnapshotSummary.CHANGED_PARTITION_COUNT_PROP);
    assertThat(changedPartitions).isEqualTo("2");
  }
}
