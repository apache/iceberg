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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.OutputFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestAppend extends AdaptiveMetadataTestBase {
  private static final List<String> BRANCHES = List.of("main", "branch");

  @Test
  public void appendNullFile() {
    assertThatThrownBy(() -> defaultTable.newAppend().appendFile(null).commit())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid data file: null");
  }

  @Test
  public void appendManifest() throws IOException {
    OutputFile output =
        io.newOutputFile("memory:/warehouse/table/metadata/append-manifest.parquet");
    ManifestFile manifest = writeLeafManifest(defaultTable, output, FILE_A, FILE_B);
    AppendFiles append = defaultTable.newAppend().appendManifest(manifest);
    assertThatThrownBy(append::commit)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot append manifest files in v4");
  }

  @Test
  public void removeDataFile() {
    defaultTable.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    MergingSnapshotProducer<?> producer = (MergeAppend) defaultTable.newAppend();
    producer.delete(FILE_A);
    assertThatThrownBy(producer::commit)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot remove data files in v4");
  }

  @Test
  public void addDeleteFile() {
    defaultTable.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    MergingSnapshotProducer<?> producer = (MergeAppend) defaultTable.newAppend();
    producer.add(FILE_A_DV);
    assertThatThrownBy(producer::commit)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot add new delete files in v4");
  }

  @Test
  public void removeDeleteFile() {
    MergingSnapshotProducer<?> producer = (MergeAppend) defaultTable.newAppend();
    producer.delete(FILE_A_EQ_DELETES);
    assertThatThrownBy(producer::commit)
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot remove delete files in v4");
  }

  @ParameterizedTest
  @FieldSource("BRANCHES")
  public void emptyTableAppend(String branch) throws IOException {
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).isNull();
    assertThat(base.lastSequenceNumber()).isEqualTo(0);

    Snapshot committedSnapshot =
        commit(
            defaultTable, defaultTable.newAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    assertThat(committedSnapshot).isNotNull();
    assertThat(defaultTable.ops().current().lastSequenceNumber()).isEqualTo(1);

    long snapshotId = committedSnapshot.snapshotId();

    List<TrackedFile> rootFiles = readRoot(defaultTable, committedSnapshot);
    validateNoLeafManifests(rootFiles);
    validateFiles(
        rootFiles,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        locations(FILE_A, FILE_B),
        statuses(EntryStatus.ADDED, EntryStatus.ADDED));

    validateBranchFiles(defaultTable, branch, FILE_A, FILE_B);
  }

  @Test
  public void appendWithDifferentSpecs() throws IOException {
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    TableMetadata base = readMetadata();
    assertThat(base.currentSnapshot()).as("Should not have a current snapshot").isNull();
    assertThat(base.lastSequenceNumber()).as("Last sequence number should be 0").isEqualTo(0);

    defaultTable.updateSpec().addField("id").commit();
    PartitionSpec newSpec = defaultTable.spec();

    assertThat(defaultTable.specs()).as("Table should have 2 specs").hasSize(2);

    DataFile fileNewSpec =
        DataFiles.builder(newSpec)
            .withPath("memory:/warehouse/table/data/data_bucket=0/id=0/data-b.parquet")
            .withPartition(TestHelpers.Row.of(0, 0))
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    defaultTable.newAppend().appendFile(FILE_A).appendFile(fileNewSpec).commit();
    Snapshot committedSnapshot = defaultTable.currentSnapshot();

    assertThat(committedSnapshot).as("Should create a snapshot").isNotNull();
    assertThat(defaultTable.ops().current().lastSequenceNumber()).isEqualTo(1);

    long snapshotId = committedSnapshot.snapshotId();

    List<TrackedFile> rootFiles = readRoot(defaultTable, committedSnapshot);
    validateNoLeafManifests(rootFiles);
    validateFiles(
        rootFiles,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(snapshotId, snapshotId),
        locations(FILE_A, fileNewSpec),
        statuses(EntryStatus.ADDED, EntryStatus.ADDED));
  }

  @Test
  public void concurrentAppends() throws IOException {
    defaultTable.updateProperties().set("random-snapshot-ids", "true");
    AppendFiles append = defaultTable.newAppend().appendFile(FILE_A);
    Snapshot pending = append.apply();
    assertThat(defaultTable.currentSnapshot()).isNull();
    assertThat(pending).isNotNull();
    assertThat(pending.sequenceNumber()).isEqualTo(1);
    validateFiles(
        readRoot(defaultTable, pending),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(pending.snapshotId()),
        locations(FILE_A),
        statuses(EntryStatus.ADDED));

    // use a separate table instance to test recovery
    Table separateInstance = load("test");
    separateInstance.newAppend().appendFile(FILE_B).commit();
    Snapshot concurrent = separateInstance.currentSnapshot();
    assertThat(concurrent).isNotNull();
    assertThat(concurrent.sequenceNumber()).isEqualTo(1);
    validateFiles(
        readRoot(defaultTable, concurrent),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(concurrent.snapshotId()),
        locations(FILE_B),
        statuses(EntryStatus.ADDED));

    assertThat(defaultTable.currentSnapshot()).as("Original Table is unchanged").isNull();
    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(
            pending.rootManifestLocation(), concurrent.rootManifestLocation());

    append.commit();
    Snapshot latest = defaultTable.currentSnapshot();
    assertThat(latest.rootManifestLocation()).isNotEqualTo(pending.rootManifestLocation());
    assertThat(latest.snapshotId()).isEqualTo(pending.snapshotId());
    assertThat(latest.parentId()).isEqualTo(concurrent.snapshotId());
    assertThat(latest.sequenceNumber()).isEqualTo(2);
    validateFiles(
        readRoot(defaultTable, latest),
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(pending.snapshotId(), concurrent.snapshotId()),
        locations(FILE_A, FILE_B),
        statuses(EntryStatus.ADDED, EntryStatus.EXISTING));

    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(
            latest.rootManifestLocation(), concurrent.rootManifestLocation());
  }

  @Test
  public void appendToUpgradedTable() throws IOException {
    Table v3Table = create("upgraded_table", SCHEMA, SPEC, 3);
    v3Table.newAppend().appendFile(FILE_A).commit();
    Snapshot v3Snapshot = v3Table.currentSnapshot();
    long v3SnapshotId = v3Snapshot.snapshotId();

    assertThat(v3Snapshot.allManifests(io)).hasSize(1);
    ManifestFile v3Manifest = v3Snapshot.allManifests(io).get(0);

    Table upgraded = upgrade("upgraded_table", 4);
    upgraded.newAppend().appendFile(FILE_B).commit();
    Snapshot snapshot = upgraded.currentSnapshot();
    long snapshotId = snapshot.snapshotId();

    List<TrackedFile> rootFiles = readRoot(upgraded, snapshot);
    validateFiles(
        rootFiles,
        contents(FileContent.DATA, FileContent.DATA_MANIFEST),
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(snapshotId, v3SnapshotId),
        locations(FILE_B.location(), v3Manifest.path()),
        statuses(EntryStatus.ADDED, EntryStatus.EXISTING));
  }

  @Test
  public void failureCleanup() throws IOException {
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    defaultTable.newAppend().appendFile(FILE_A).commit();
    Snapshot snap = defaultTable.currentSnapshot();

    long baseId = snap.snapshotId();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(1);

    validateFiles(
        readRoot(defaultTable, snap),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        locations(FILE_A),
        statuses(EntryStatus.ADDED));

    defaultTable.ops().failCommits(5);

    AppendFiles append = defaultTable.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    String pendingRootLocation = pending.rootManifestLocation();

    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(pendingRootLocation, snap.rootManifestLocation());
    validateFiles(
        readRoot(defaultTable, pending),
        ids(pending.snapshotId(), baseId),
        locations(FILE_B, FILE_A));

    assertThatThrownBy(append::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(1);

    Snapshot latest = defaultTable.currentSnapshot();
    assertThat(latest.snapshotId()).as("Should not have a new commit").isEqualTo(snap.snapshotId());

    List<TrackedFile> files = readRoot(defaultTable, latest);
    validateNoLeafManifests(files);
    validateFiles(
        files,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        locations(FILE_A),
        statuses(EntryStatus.ADDED));

    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(snap.rootManifestLocation());
  }

  @Test
  public void recoveryAndCleanup() throws IOException {
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(0);

    defaultTable.newAppend().appendFile(FILE_A).commit();
    Snapshot snap = defaultTable.currentSnapshot();

    long baseId = snap.snapshotId();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(1);

    validateFiles(
        readRoot(defaultTable, snap),
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        locations(FILE_A),
        statuses(EntryStatus.ADDED));

    defaultTable.ops().failCommits(3);

    AppendFiles append = defaultTable.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    String pendingRootLocation = pending.rootManifestLocation();

    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(pendingRootLocation, snap.rootManifestLocation());
    validateFiles(
        readRoot(defaultTable, pending),
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(pending.snapshotId(), baseId),
        locations(FILE_B, FILE_A),
        statuses(EntryStatus.ADDED, EntryStatus.EXISTING));

    append.commit();
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(2);

    Snapshot latest = defaultTable.currentSnapshot();
    assertThat(latest.rootManifestLocation())
        .as("Pending root manifest should be rewritten")
        .isNotEqualTo(pendingRootLocation);
    assertThat(listRootManifests(defaultTable))
        .containsExactlyInAnyOrder(latest.rootManifestLocation(), snap.rootManifestLocation());

    List<TrackedFile> files = readRoot(defaultTable, latest);
    validateNoLeafManifests(files);
    validateFiles(
        files,
        dataSeqs(2L, 1L),
        fileSeqs(2L, 1L),
        ids(latest.snapshotId(), baseId),
        locations(FILE_B, FILE_A),
        statuses(EntryStatus.ADDED, EntryStatus.EXISTING));
  }
}
