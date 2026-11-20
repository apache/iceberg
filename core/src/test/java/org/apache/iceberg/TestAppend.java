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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

public class TestAppend extends AdaptiveMetadataTestBase {
  private static final List<String> BRANCHES = List.of("main", "branch");

  // TODO:
  // - Append to upgraded v3 table
  // - Fail append manifest
  // - Fail delete data file
  // - Fail delete delete file
  // - Fail add delete files, DVs

  @Test
  public void appendNullFile() {
    assertThatThrownBy(() -> defaultTable.newAppend().appendFile(null).commit())
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid data file: null");
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
            .withPath("file:/warehouse/table/data/data_bucket=0/id=0/data-b.parquet")
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

    assertThat(defaultTable.io().newInputFile(pendingRootLocation).exists()).isTrue();
    validateFiles(
        readRoot(defaultTable, pending),
        ids(pending.snapshotId(), baseId),
        locations(FILE_B, FILE_A));

    assertThatThrownBy(append::commit)
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Injected failure");
    assertThat(readMetadata().lastSequenceNumber()).isEqualTo(1);

    Snapshot latest = defaultTable.currentSnapshot();

    List<TrackedFile> files = readRoot(defaultTable, latest);
    validateNoLeafManifests(files);
    validateFiles(
        files,
        dataSeqs(1L),
        fileSeqs(1L),
        ids(baseId),
        locations(FILE_A),
        statuses(EntryStatus.ADDED));

    assertThat(defaultTable.io().newInputFile(pendingRootLocation).exists()).isFalse();
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

    defaultTable.ops().failCommits(1);

    AppendFiles append = defaultTable.newAppend().appendFile(FILE_B);
    Snapshot pending = append.apply();

    String pendingRootLocation = pending.rootManifestLocation();

    assertThat(defaultTable.io().newInputFile(pendingRootLocation).exists()).isTrue();
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
    assertThat(defaultTable.io().newInputFile(pendingRootLocation).exists()).isFalse();

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
