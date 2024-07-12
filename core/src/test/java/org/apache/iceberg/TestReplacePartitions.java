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

import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestReplacePartitions extends TestBase {

  static final DataFile FILE_E =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-e.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=0") // same partition as FILE_A
          .withRecordCount(0)
          .build();

  static final DataFile FILE_F =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-f.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=1") // same partition as FILE_B
          .withRecordCount(0)
          .build();

  static final DataFile FILE_G =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-g.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=10") // no other partition
          .withRecordCount(0)
          .build();

  static final DataFile FILE_UNPARTITIONED_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-unpartitioned-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  static final DataFile FILE_NULL_PARTITION =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-null-partition.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("data_bucket=__HIVE_DEFAULT_PARTITION__")
          .withRecordCount(0)
          .build();

  // Partition spec with VOID partition transform ("alwaysNull" in Java code.)
 static final PartitionSpec SPEC_VOID =
      PartitionSpec.builderFor(SCHEMA).alwaysNull("id").bucket("data", BUCKETS_NUMBER).build();

  static final DataFile FILE_A_VOID_PARTITION =
      DataFiles.builder(SPEC_VOID)
          .withPath("/path/to/data-a-void-partition.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_null=__HIVE_DEFAULT_PARTITION__/data_bucket=0")
          .withRecordCount(1)
          .build();

  static final DataFile FILE_B_VOID_PARTITION =
      DataFiles.builder(SPEC_VOID)
          .withPath("/path/to/data-b-void-partition.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("id_null=__HIVE_DEFAULT_PARTITION__/data_bucket=1")
          .withRecordCount(10)
          .build();

  static final DeleteFile FILE_UNPARTITIONED_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-unpartitioned-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

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
  public void testReplaceOnePartition() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_E), branch);

    long replaceId = latestSnapshot(readMetadata(), branch).snapshotId();
    assertThat(replaceId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(replaceId),
        files(FILE_E),
        statuses(Status.ADDED));

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(replaceId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceAndMergeOnePartition() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_E), branch);

    long replaceId = latestSnapshot(table, branch).snapshotId();
    assertThat(replaceId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(replaceId, replaceId, baseId),
        files(FILE_E, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testReplaceWithUnpartitionedTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(0);

    commit(table, unpartitioned.newAppend().appendFile(FILE_A), branch);
    // make sure the data was successfully added
    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(1);
    validateSnapshot(
        null, latestSnapshot(TestTables.readMetadata("unpartitioned"), branch), FILE_A);

    ReplacePartitions replacePartitions = unpartitioned.newReplacePartitions().addFile(FILE_B);
    commit(table, replacePartitions, branch);

    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(2);
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = latestSnapshot(replaceMetadata, branch).snapshotId();

    assertThat(latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io())).hasSize(2);

    validateManifestEntries(
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).get(0),
        ids(replaceId),
        files(FILE_B),
        statuses(Status.ADDED));

    validateManifestEntries(
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).get(1),
        ids(replaceId),
        files(FILE_A),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testReplaceAndMergeWithUnpartitionedTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    // ensure the overwrite results in a merge
    unpartitioned.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(1);

    AppendFiles appendFiles = unpartitioned.newAppend().appendFile(FILE_A);
    commit(table, appendFiles, branch);

    // make sure the data was successfully added
    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(2);
    validateSnapshot(
        null, latestSnapshot(TestTables.readMetadata("unpartitioned"), branch), FILE_A);

    ReplacePartitions replacePartitions = unpartitioned.newReplacePartitions().addFile(FILE_B);
    commit(table, replacePartitions, branch);

    assertThat(TestTables.metadataVersion("unpartitioned")).isEqualTo(3);
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = latestSnapshot(replaceMetadata, branch).snapshotId();

    assertThat(latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io())).hasSize(1);

    validateManifestEntries(
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).get(0),
        ids(replaceId, replaceId),
        files(FILE_B, FILE_A),
        statuses(Status.ADDED, Status.DELETED));
  }

  @TestTemplate
  public void testValidationFailure() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    ReplacePartitions replace =
        table.newReplacePartitions().addFile(FILE_F).addFile(FILE_G).validateAppendOnly();

    assertThatThrownBy(() -> commit(table, replace, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit file that conflicts with existing partition");

    assertThat(latestSnapshot(readMetadata(), branch).snapshotId()).isEqualTo(baseId);
  }

  @TestTemplate
  public void testValidationSuccess() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_G).validateAppendOnly(), branch);

    long replaceId = latestSnapshot(readMetadata(), branch).snapshotId();
    assertThat(replaceId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(replaceId),
        files(FILE_G),
        statuses(Status.ADDED));

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(baseId, baseId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
  }

  @TestTemplate
  public void testValidationNotInvoked() {
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();

    // Two concurrent ReplacePartitions with No Validation Enabled
    commit(
        table,
        table
            .newReplacePartitions()
            .addFile(FILE_E)
            .validateFromSnapshot(latestSnapshot(base, branch).snapshotId()),
        branch);
    commit(
        table,
        table
            .newReplacePartitions()
            .addFile(FILE_A) // Replaces FILE_E which becomes Deleted
            .addFile(FILE_B)
            .validateFromSnapshot(latestSnapshot(base, branch).snapshotId()),
        branch);

    long replaceId = latestSnapshot(readMetadata(), branch).snapshotId();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(replaceId, replaceId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(replaceId),
        files(FILE_E),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testValidateWithDefaultSnapshotId() {
    commit(table, table.newReplacePartitions().addFile(FILE_A), branch);

    // Concurrent Replace Partitions should fail with ValidationException
    ReplacePartitions replace = table.newReplacePartitions();
    assertThatThrownBy(
            () ->
                commit(
                    table,
                    replace
                        .addFile(FILE_A)
                        .addFile(FILE_B)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching partitions "
                + "[data_bucket=0, data_bucket=1]: [/path/to/data-a.parquet]");
  }

  @TestTemplate
  public void testValidateWithNullPartition() {
    commit(table, table.newReplacePartitions().addFile(FILE_NULL_PARTITION), branch);

    // Concurrent Replace Partitions should fail with ValidationException
    ReplacePartitions replace = table.newReplacePartitions();
    assertThatThrownBy(
            () ->
                commit(
                    table,
                    replace
                        .addFile(FILE_NULL_PARTITION)
                        .addFile(FILE_B)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching partitions "
                + "[data_bucket=null, data_bucket=1]: [/path/to/data-null-partition.parquet]");
  }

  @TestTemplate
  public void testValidateWithVoidTransform() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Table tableVoid = TestTables.create(tableDir, "tablevoid", SCHEMA, SPEC_VOID, formatVersion);
    commit(tableVoid, tableVoid.newReplacePartitions().addFile(FILE_A_VOID_PARTITION), branch);

    // Concurrent Replace Partitions should fail with ValidationException
    ReplacePartitions replace = tableVoid.newReplacePartitions();
    assertThatThrownBy(
            () ->
                commit(
                    tableVoid,
                    replace
                        .addFile(FILE_A_VOID_PARTITION)
                        .addFile(FILE_B_VOID_PARTITION)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching partitions "
                + "[id_null=null, data_bucket=1, id_null=null, data_bucket=0]: "
                + "[/path/to/data-a-void-partition.parquet]");
  }

  @TestTemplate
  public void testConcurrentReplaceConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Replace Partitions should fail with ValidationException
    commit(table, table.newReplacePartitions().addFile(FILE_A), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newReplacePartitions()
                        .validateFromSnapshot(baseId)
                        .addFile(FILE_A)
                        .addFile(FILE_B)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching partitions "
                + "[data_bucket=0, data_bucket=1]: [/path/to/data-a.parquet]");
  }

  @TestTemplate
  public void testConcurrentReplaceNoConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long id1 = latestSnapshot(base, branch).snapshotId();

    // Concurrent Replace Partitions should not fail if concerning different partitions
    commit(table, table.newReplacePartitions().addFile(FILE_A), branch);
    long id2 = latestSnapshot(readMetadata(), branch).snapshotId();

    commit(
        table,
        table
            .newReplacePartitions()
            .validateFromSnapshot(id1)
            .validateNoConflictingData()
            .validateNoConflictingDeletes()
            .addFile(FILE_B),
        branch);

    long id3 = latestSnapshot(readMetadata(), branch).snapshotId();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(id3),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(id2),
        files(FILE_A),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testConcurrentReplaceConflictNonPartitioned() {
    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newReplacePartitions().addFile(FILE_UNPARTITIONED_A), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    unpartitioned
                        .newReplacePartitions()
                        .validateFromSnapshot(replaceBaseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_UNPARTITIONED_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching true: "
                + "[/path/to/data-unpartitioned-a.parquet]");
  }

  @TestTemplate
  public void testAppendReplaceConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Append and ReplacePartition should fail with ValidationException
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newReplacePartitions()
                        .validateFromSnapshot(baseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_A)
                        .addFile(FILE_B),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching partitions "
                + "[data_bucket=0, data_bucket=1]: [/path/to/data-b.parquet]");
  }

  @TestTemplate
  public void testAppendReplaceNoConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long id1 = latestSnapshot(base, branch).snapshotId();

    // Concurrent Append and ReplacePartition should not conflict if concerning different partitions
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    long id2 = latestSnapshot(readMetadata(), branch).snapshotId();

    commit(
        table,
        table
            .newReplacePartitions()
            .validateFromSnapshot(id1)
            .validateNoConflictingData()
            .validateNoConflictingDeletes()
            .addFile(FILE_A),
        branch);

    long id3 = latestSnapshot(readMetadata(), branch).snapshotId();
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(3);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(id3),
        files(FILE_A),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(id2),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(2),
        ids(id3),
        files(FILE_A),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testAppendReplaceConflictNonPartitioned() {
    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Append and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    unpartitioned
                        .newReplacePartitions()
                        .validateFromSnapshot(replaceBaseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_UNPARTITIONED_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting files that can contain records matching true: "
                + "[/path/to/data-unpartitioned-a.parquet]");
  }

  @TestTemplate
  public void testDeleteReplaceConflict() {
    assumeThat(formatVersion).isEqualTo(2);
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Delete and ReplacePartition should fail with ValidationException
    commit(
        table, table.newRowDelta().addDeletes(FILE_A_DELETES).validateFromSnapshot(baseId), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newReplacePartitions()
                        .validateFromSnapshot(baseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found new conflicting delete files that can apply to records matching "
                + "[data_bucket=0]: [/path/to/data-a-deletes.parquet]");
  }

  @TestTemplate
  public void testDeleteReplaceConflictNonPartitioned() {
    assumeThat(formatVersion).isEqualTo(2);

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Delete and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newRowDelta().addDeletes(FILE_UNPARTITIONED_A_DELETES), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    unpartitioned
                        .newReplacePartitions()
                        .validateFromSnapshot(replaceBaseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_UNPARTITIONED_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found new conflicting delete files that can apply to records matching true: "
                + "[/path/to/data-unpartitioned-a-deletes.parquet]");
  }

  @TestTemplate
  public void testDeleteReplaceNoConflict() {
    assumeThat(formatVersion).isEqualTo(2);
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);
    long id1 = latestSnapshot(readMetadata(), branch).snapshotId();

    // Concurrent Delta and ReplacePartition should not conflict if concerning different partitions
    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(FILE_A_DELETES)
            .validateFromSnapshot(id1)
            .validateNoConflictingDataFiles()
            .validateNoConflictingDeleteFiles()
            .validateFromSnapshot(id1),
        branch);

    long id2 = latestSnapshot(readMetadata(), branch).snapshotId();

    commit(
        table,
        table
            .newReplacePartitions()
            .validateNoConflictingData()
            .validateNoConflictingDeletes()
            .validateFromSnapshot(id1)
            .addFile(FILE_B),
        branch);

    long id3 = latestSnapshot(readMetadata(), branch).snapshotId();

    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(3);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(id3),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(id1),
        files(FILE_A),
        statuses(Status.ADDED));
    validateDeleteManifest(
        latestSnapshot(table, branch).allManifests(table.io()).get(2),
        dataSeqs(2L),
        fileSeqs(2L),
        ids(id2),
        files(FILE_A_DELETES),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testOverwriteReplaceConflict() {
    assumeThat(formatVersion).isEqualTo(2);
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Overwrite and ReplacePartition should fail with ValidationException
    commit(table, table.newOverwrite().deleteFile(FILE_A), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newReplacePartitions()
                        .validateFromSnapshot(baseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting deleted files that can apply to records matching "
                + "[data_bucket=0]: [/path/to/data-a.parquet]");
  }

  @TestTemplate
  public void testOverwriteReplaceNoConflict() {
    assumeThat(formatVersion).isEqualTo(2);
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Overwrite and ReplacePartition should not fail with if concerning different
    // partitions
    commit(table, table.newOverwrite().deleteFile(FILE_A), branch);

    commit(
        table,
        table
            .newReplacePartitions()
            .validateNoConflictingData()
            .validateNoConflictingDeletes()
            .validateFromSnapshot(baseId)
            .addFile(FILE_B),
        branch);

    long finalId = latestSnapshot(readMetadata(), branch).snapshotId();

    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(finalId),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(finalId),
        files(FILE_B),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testOverwriteReplaceConflictNonPartitioned() {
    assumeThat(formatVersion).isEqualTo(2);

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Overwrite and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newOverwrite().deleteFile(FILE_UNPARTITIONED_A), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    unpartitioned
                        .newReplacePartitions()
                        .validateFromSnapshot(replaceBaseId)
                        .validateNoConflictingData()
                        .validateNoConflictingDeletes()
                        .addFile(FILE_UNPARTITIONED_A),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Found conflicting deleted files that can contain records matching true: "
                + "[/path/to/data-unpartitioned-a.parquet]");
  }

  @TestTemplate
  public void testValidateOnlyDeletes() {
    commit(table, table.newAppend().appendFile(FILE_A), branch);
    long baseId = latestSnapshot(readMetadata(), branch).snapshotId();

    // Snapshot Isolation mode: appends do not conflict with replace
    commit(table, table.newAppend().appendFile(FILE_B), branch);

    commit(
        table,
        table
            .newReplacePartitions()
            .validateFromSnapshot(baseId)
            .validateNoConflictingDeletes()
            .addFile(FILE_B),
        branch);
    long finalId = latestSnapshot(readMetadata(), branch).snapshotId();

    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(3);
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(finalId),
        files(FILE_B),
        statuses(Status.ADDED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(finalId),
        files(FILE_B),
        statuses(Status.DELETED));
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(2),
        ids(baseId),
        files(FILE_A),
        statuses(Status.ADDED));
  }

  @TestTemplate
  public void testEmptyPartitionPathWithUnpartitionedTable() {
    DataFiles.builder(PartitionSpec.unpartitioned()).withPartitionPath("");
  }
}
