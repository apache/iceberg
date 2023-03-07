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

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestReplacePartitions extends TableTestBase {

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

  static final DeleteFile FILE_UNPARTITIONED_A_DELETES =
      FileMetadata.deleteFileBuilder(PartitionSpec.unpartitioned())
          .ofPositionDeletes()
          .withPath("/path/to/data-unpartitioned-a-deletes.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(1)
          .build();

  private final String branch;

  @Parameterized.Parameters(name = "formatVersion = {0}, branch = {1}")
  public static Object[] parameters() {
    return new Object[][] {
      new Object[] {1, "main"},
      new Object[] {1, "testBranch"},
      new Object[] {2, "main"},
      new Object[] {2, "testBranch"}
    };
  }

  public TestReplacePartitions(int formatVersion, String branch) {
    super(formatVersion);
    this.branch = branch;
  }

  @Test
  public void testReplaceOnePartition() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_E), branch);

    long replaceId = latestSnapshot(readMetadata(), branch).snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals(
        "Table should have 2 manifests",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());

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

  @Test
  public void testReplaceAndMergeOnePartition() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_E), branch);

    long replaceId = latestSnapshot(table, branch).snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals(
        "Table should have 1 manifest",
        1,
        latestSnapshot(table, branch).allManifests(table.io()).size());

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(replaceId, replaceId, baseId),
        files(FILE_E, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testReplaceWithUnpartitionedTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    Assert.assertEquals(
        "Table version should be 0", 0, (long) TestTables.metadataVersion("unpartitioned"));

    commit(table, unpartitioned.newAppend().appendFile(FILE_A), branch);
    // make sure the data was successfully added
    Assert.assertEquals(
        "Table version should be 1", 1, (long) TestTables.metadataVersion("unpartitioned"));
    validateSnapshot(
        null, latestSnapshot(TestTables.readMetadata("unpartitioned"), branch), FILE_A);

    ReplacePartitions replacePartitions = unpartitioned.newReplacePartitions().addFile(FILE_B);
    commit(table, replacePartitions, branch);

    Assert.assertEquals(
        "Table version should be 2", 2, (long) TestTables.metadataVersion("unpartitioned"));
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = latestSnapshot(replaceMetadata, branch).snapshotId();

    Assert.assertEquals(
        "Table should have 2 manifests",
        2,
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).size());

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

  @Test
  public void testReplaceAndMergeWithUnpartitionedTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    // ensure the overwrite results in a merge
    unpartitioned.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    Assert.assertEquals(
        "Table version should be 1", 1, (long) TestTables.metadataVersion("unpartitioned"));

    AppendFiles appendFiles = unpartitioned.newAppend().appendFile(FILE_A);
    commit(table, appendFiles, branch);

    // make sure the data was successfully added
    Assert.assertEquals(
        "Table version should be 2", 2, (long) TestTables.metadataVersion("unpartitioned"));
    validateSnapshot(
        null, latestSnapshot(TestTables.readMetadata("unpartitioned"), branch), FILE_A);

    ReplacePartitions replacePartitions = unpartitioned.newReplacePartitions().addFile(FILE_B);
    commit(table, replacePartitions, branch);

    Assert.assertEquals(
        "Table version should be 3", 3, (long) TestTables.metadataVersion("unpartitioned"));
    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceId = latestSnapshot(replaceMetadata, branch).snapshotId();

    Assert.assertEquals(
        "Table should have 1 manifest",
        1,
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).size());

    validateManifestEntries(
        latestSnapshot(replaceMetadata, branch).allManifests(unpartitioned.io()).get(0),
        ids(replaceId, replaceId),
        files(FILE_B, FILE_A),
        statuses(Status.ADDED, Status.DELETED));
  }

  @Test
  public void testValidationFailure() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    ReplacePartitions replace =
        table.newReplacePartitions().addFile(FILE_F).addFile(FILE_G).validateAppendOnly();

    AssertHelpers.assertThrows(
        "Should reject commit with file not matching delete expression",
        ValidationException.class,
        "Cannot commit file that conflicts with existing partition",
        () -> commit(table, replace, branch));

    Assert.assertEquals(
        "Should not create a new snapshot",
        baseId,
        latestSnapshot(readMetadata(), branch).snapshotId());
  }

  @Test
  public void testValidationSuccess() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newReplacePartitions().addFile(FILE_G).validateAppendOnly(), branch);

    long replaceId = latestSnapshot(readMetadata(), branch).snapshotId();
    Assert.assertNotEquals("Should create a new snapshot", baseId, replaceId);
    Assert.assertEquals(
        "Table should have 2 manifests",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());

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

  @Test
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
    Assert.assertEquals(
        "Table should have 2 manifest",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testValidateWithDefaultSnapshotId() {
    commit(table, table.newReplacePartitions().addFile(FILE_A), branch);

    // Concurrent Replace Partitions should fail with ValidationException
    ReplacePartitions replace = table.newReplacePartitions();
    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting files that can contain records matching partitions "
            + "[data_bucket=0, data_bucket=1]: [/path/to/data-a.parquet]",
        () ->
            commit(
                table,
                replace
                    .addFile(FILE_A)
                    .addFile(FILE_B)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes(),
                branch));
  }

  @Test
  public void testConcurrentReplaceConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A).appendFile(FILE_B), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Replace Partitions should fail with ValidationException
    commit(table, table.newReplacePartitions().addFile(FILE_A), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting files that can contain records matching partitions "
            + "[data_bucket=0, data_bucket=1]: [/path/to/data-a.parquet]",
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
                branch));
  }

  @Test
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
    Assert.assertEquals(
        "Table should have 2 manifests",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testConcurrentReplaceConflictNonPartitioned() {
    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newReplacePartitions().addFile(FILE_UNPARTITIONED_A), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting files that can contain records matching true: "
            + "[/path/to/data-unpartitioned-a.parquet]",
        () ->
            commit(
                table,
                unpartitioned
                    .newReplacePartitions()
                    .validateFromSnapshot(replaceBaseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_UNPARTITIONED_A),
                branch));
  }

  @Test
  public void testAppendReplaceConflict() {
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Append and ReplacePartition should fail with ValidationException
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting files that can contain records matching partitions "
            + "[data_bucket=0, data_bucket=1]: [/path/to/data-b.parquet]",
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
                branch));
  }

  @Test
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
    Assert.assertEquals(
        "Table should have 3 manifests",
        3,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testAppendReplaceConflictNonPartitioned() {
    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Append and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting files that can contain records matching true: "
            + "[/path/to/data-unpartitioned-a.parquet]",
        () ->
            commit(
                table,
                unpartitioned
                    .newReplacePartitions()
                    .validateFromSnapshot(replaceBaseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_UNPARTITIONED_A),
                branch));
  }

  @Test
  public void testDeleteReplaceConflict() {
    Assume.assumeTrue(formatVersion == 2);
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Delete and ReplacePartition should fail with ValidationException
    commit(
        table, table.newRowDelta().addDeletes(FILE_A_DELETES).validateFromSnapshot(baseId), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found new conflicting delete files that can apply to records matching "
            + "[data_bucket=0]: [/path/to/data-a-deletes.parquet]",
        () ->
            commit(
                table,
                table
                    .newReplacePartitions()
                    .validateFromSnapshot(baseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_A),
                branch));
  }

  @Test
  public void testDeleteReplaceConflictNonPartitioned() {
    Assume.assumeTrue(formatVersion == 2);

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);
    commit(table, unpartitioned.newAppend().appendFile(FILE_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Delete and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newRowDelta().addDeletes(FILE_UNPARTITIONED_A_DELETES), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found new conflicting delete files that can apply to records matching true: "
            + "[/path/to/data-unpartitioned-a-deletes.parquet]",
        () ->
            commit(
                table,
                unpartitioned
                    .newReplacePartitions()
                    .validateFromSnapshot(replaceBaseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_UNPARTITIONED_A),
                branch));
  }

  @Test
  public void testDeleteReplaceNoConflict() {
    Assume.assumeTrue(formatVersion == 2);
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

    Assert.assertEquals(
        "Table should have 3 manifest",
        3,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testOverwriteReplaceConflict() {
    Assume.assumeTrue(formatVersion == 2);
    commit(table, table.newFastAppend().appendFile(FILE_A), branch);

    TableMetadata base = readMetadata();
    long baseId = latestSnapshot(base, branch).snapshotId();

    // Concurrent Overwrite and ReplacePartition should fail with ValidationException
    commit(table, table.newOverwrite().deleteFile(FILE_A), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting deleted files that can apply to records matching "
            + "[data_bucket=0]: [/path/to/data-a.parquet]",
        () ->
            commit(
                table,
                table
                    .newReplacePartitions()
                    .validateFromSnapshot(baseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_A),
                branch));
  }

  @Test
  public void testOverwriteReplaceNoConflict() {
    Assume.assumeTrue(formatVersion == 2);
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

    Assert.assertEquals(
        "Table should have 2 manifest",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testOverwriteReplaceConflictNonPartitioned() {
    Assume.assumeTrue(formatVersion == 2);

    Table unpartitioned =
        TestTables.create(
            tableDir, "unpartitioned", SCHEMA, PartitionSpec.unpartitioned(), formatVersion);

    commit(table, unpartitioned.newAppend().appendFile(FILE_UNPARTITIONED_A), branch);

    TableMetadata replaceMetadata = TestTables.readMetadata("unpartitioned");
    long replaceBaseId = latestSnapshot(replaceMetadata, branch).snapshotId();

    // Concurrent Overwrite and ReplacePartitions should fail with ValidationException
    commit(table, unpartitioned.newOverwrite().deleteFile(FILE_UNPARTITIONED_A), branch);

    AssertHelpers.assertThrows(
        "Should reject commit with file matching partitions replaced",
        ValidationException.class,
        "Found conflicting deleted files that can contain records matching true: "
            + "[/path/to/data-unpartitioned-a.parquet]",
        () ->
            commit(
                table,
                unpartitioned
                    .newReplacePartitions()
                    .validateFromSnapshot(replaceBaseId)
                    .validateNoConflictingData()
                    .validateNoConflictingDeletes()
                    .addFile(FILE_UNPARTITIONED_A),
                branch));
  }

  @Test
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

    Assert.assertEquals(
        "Table should have 3 manifest",
        3,
        latestSnapshot(table, branch).allManifests(table.io()).size());
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

  @Test
  public void testEmptyPartitionPathWithUnpartitionedTable() {
    DataFiles.builder(PartitionSpec.unpartitioned()).withPartitionPath("");
  }
}
