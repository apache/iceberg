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

import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.relocated.com.google.common.collect.Iterators.concat;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestOverwrite extends TestBase {
  private static final Schema DATE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_BY_DATE =
      PartitionSpec.builderFor(DATE_SCHEMA).identity("date").build();

  private static final String TABLE_NAME = "overwrite_table";

  private static final DataFile FILE_0_TO_4 =
      DataFiles.builder(PARTITION_BY_DATE)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("date=2018-06-08")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 3L), // value count
                  ImmutableMap.of(1, 0L, 2, 2L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                  ))
          .build();

  private static final DataFile FILE_5_TO_9 =
      DataFiles.builder(PARTITION_BY_DATE)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("date=2018-06-09")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 3L), // value count
                  ImmutableMap.of(1, 0L, 2, 2L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
                  ))
          .build();

  private static final DataFile FILE_10_TO_14 =
      DataFiles.builder(PARTITION_BY_DATE)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(0)
          .withPartitionPath("date=2018-06-09")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 3L), // value count
                  ImmutableMap.of(1, 0L, 2, 2L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(14L)) // upper bounds
                  ))
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

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  private Table table = null;

  @BeforeEach
  public void createTestTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    this.table =
        TestTables.create(tableDir, TABLE_NAME, DATE_SCHEMA, PARTITION_BY_DATE, formatVersion);

    commit(table, table.newAppend().appendFile(FILE_0_TO_4).appendFile(FILE_5_TO_9), branch);
  }

  @TestTemplate
  public void deleteDataFilesProducesDeleteOperation() {
    commit(table, table.newOverwrite().deleteFile(FILE_A).deleteFile(FILE_B), branch);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void addAndDeleteDataFilesProducesOverwriteOperation() {
    commit(table, table.newOverwrite().addFile(FILE_10_TO_14).deleteFile(FILE_B), branch);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.OVERWRITE);
  }

  @TestTemplate
  public void overwriteByRowFilterProducesDeleteOperation() {
    commit(table, table.newOverwrite().overwriteByRowFilter(equal("date", "2018-06-08")), branch);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.DELETE);
  }

  @TestTemplate
  public void addAndOverwriteByRowFilterProducesOverwriteOperation() {
    commit(
        table,
        table
            .newOverwrite()
            .addFile(FILE_10_TO_14)
            .overwriteByRowFilter(equal("date", "2018-06-08")),
        branch);

    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.OVERWRITE);
  }

  @TestTemplate
  public void addFilesProducesAppendOperation() {
    commit(table, table.newOverwrite().addFile(FILE_10_TO_14).addFile(FILE_5_TO_9), branch);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.APPEND);
  }

  @TestTemplate
  public void testOverwriteWithoutAppend() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newOverwrite().overwriteByRowFilter(equal("date", "2018-06-08")), branch);

    long overwriteId = latestSnapshot(table, branch).snapshotId();

    assertThat(overwriteId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.DELETE);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(overwriteId, baseId),
        files(FILE_0_TO_4, FILE_5_TO_9),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testOverwriteFailsDelete() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(and(equal("date", "2018-06-09"), lessThan("id", 9)));

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot delete file where some, but not all, rows match filter");

    assertThat(latestSnapshot(base, branch).snapshotId()).isEqualTo(baseId);
  }

  @TestTemplate
  public void testOverwriteWithAppendOutsideOfDelete() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    Snapshot latestSnapshot = latestSnapshot(base, branch);
    long baseId = latestSnapshot == null ? -1 : latestSnapshot.snapshotId();

    commit(
        table,
        table
            .newOverwrite()
            .overwriteByRowFilter(equal("date", "2018-06-08"))
            .addFile(FILE_10_TO_14), // in 2018-06-09, NOT in 2018-06-08
        branch);

    long overwriteId = latestSnapshot(table, branch).snapshotId();

    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(overwriteId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(2);

    // manifest is not merged because it is less than the minimum
    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(overwriteId),
        files(FILE_10_TO_14),
        statuses(Status.ADDED));

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(1),
        ids(overwriteId, baseId),
        files(FILE_0_TO_4, FILE_5_TO_9),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testOverwriteWithMergedAppendOutsideOfDelete() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    commit(
        table,
        table
            .newOverwrite()
            .overwriteByRowFilter(equal("date", "2018-06-08"))
            .addFile(FILE_10_TO_14),
        branch); // in 2018-06-09, NOT in 2018-06-08

    long overwriteId = latestSnapshot(table, branch).snapshotId();

    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.OVERWRITE);
    assertThat(overwriteId).isNotEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).allManifests(table.io())).hasSize(1);

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(overwriteId, overwriteId, baseId),
        files(FILE_10_TO_14, FILE_0_TO_4, FILE_5_TO_9),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testValidatedOverwriteWithAppendOutsideOfDelete() {
    // ensure the overwrite results in a merge
    table.updateProperties().set(TableProperties.MANIFEST_MIN_MERGE_COUNT, "1").commit();

    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(equal("date", "2018-06-08"))
            .addFile(FILE_10_TO_14) // in 2018-06-09, NOT in 2018-06-08
            .validateAddedFilesMatchOverwriteFilter();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot append file with rows that do not match filter");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.APPEND);
  }

  @TestTemplate
  public void testValidatedOverwriteWithAppendOutsideOfDeleteMetrics() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(and(equal("date", "2018-06-09"), lessThan("id", 10)))
            .addFile(FILE_10_TO_14) // in 2018-06-09 matches, but IDs are outside range
            .validateAddedFilesMatchOverwriteFilter();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot append file with rows that do not match filter");

    assertThat(latestSnapshot(base, branch).snapshotId()).isEqualTo(baseId);
    assertThat(latestSnapshot(table, branch).operation()).isEqualTo(DataOperations.APPEND);
  }

  @TestTemplate
  public void testValidatedOverwriteWithAppendSuccess() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(and(equal("date", "2018-06-09"), lessThan("id", 20)))
            .addFile(FILE_10_TO_14) // in 2018-06-09 matches and IDs are inside range
            .validateAddedFilesMatchOverwriteFilter();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot append file with rows that do not match filter");

    assertThat(latestSnapshot(base, branch).snapshotId()).isEqualTo(baseId);
  }

  @Test
  public void testOverwriteWithEmptyTableAppendManifest() throws IOException {
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", latestSnapshot(base, branch));
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    commit(super.table, super.table.newOverwrite().appendManifest(manifest), branch);

    Snapshot committedSnapshot = latestSnapshot(super.table, branch);
    Assert.assertNotNull("Should create a snapshot", latestSnapshot(super.table, branch));
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, super.table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, super.table.ops().current().lastSequenceNumber());
    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        committedSnapshot.allManifests(super.table.io()).size());

    long overwriteId = committedSnapshot.snapshotId();
    validateManifest(
        committedSnapshot.allManifests(table.io()).get(0),
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(overwriteId, overwriteId),
        files(FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED));

    // validate that the metadata summary is correct when using appendManifest
    Assert.assertEquals(
        "Summary metadata should include 2 added files",
        "2",
        committedSnapshot.summary().get("added-data-files"));
  }

  @Test
  public void testOverwriteWithAddFileAndAppendManifest() throws IOException {
    // merge all manifests for this test
    super.table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    TableMetadata base = readMetadata();
    Assert.assertNull("Should not have a current snapshot", latestSnapshot(base, branch));
    Assert.assertEquals("Last sequence number should be 0", 0, base.lastSequenceNumber());

    ManifestFile manifest = writeManifest(FILE_A, FILE_B);

    commit(
        super.table,
        super.table.newOverwrite().addFile(FILE_C).addFile(FILE_D).appendManifest(manifest),
        branch);

    Snapshot committedSnapshot = latestSnapshot(super.table, branch);
    Assert.assertNotNull("Should create a snapshot", latestSnapshot(super.table, branch));
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, super.table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, super.table.ops().current().lastSequenceNumber());

    long snapshotId = committedSnapshot.snapshotId();

    Assert.assertEquals(
        "Should create 1 merged manifest",
        1,
        committedSnapshot.allManifests(super.table.io()).size());

    validateManifest(
        committedSnapshot.allManifests(super.table.io()).get(0),
        dataSeqs(1L, 1L, 1L, 1L),
        fileSeqs(1L, 1L, 1L, 1L),
        ids(snapshotId, snapshotId, snapshotId, snapshotId),
        files(FILE_C, FILE_D, FILE_A, FILE_B),
        statuses(Status.ADDED, Status.ADDED, Status.ADDED, Status.ADDED));
  }

  @Test
  public void testOverwriteWithDeleteFileAndAppendManifest() throws IOException {
    // merge all manifests for this test
    super.table.updateProperties().set("commit.manifest.min-count-to-merge", "1").commit();

    Assert.assertNull("Should not have a current snapshot", latestSnapshot(readMetadata(), branch));
    Assert.assertEquals("Table should start empty", 0, listManifestFiles().size());

    commit(
        super.table,
        super.table.newOverwrite().addFile(FILE_C).addFile(FILE_D),
        branch); // Added data file , FILE_C and FILE_D

    Assert.assertNotNull("Should create a snapshot", latestSnapshot(super.table, branch));
    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, super.table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 1", 1, super.table.ops().current().lastSequenceNumber());

    Snapshot commitBefore = latestSnapshot(super.table, branch);
    long baseId = commitBefore.snapshotId();
    validateSnapshot(null, commitBefore, 1, FILE_C, FILE_D);

    Assert.assertEquals(
        "Should create 1 manifest for initial write",
        1,
        commitBefore.allManifests(table.io()).size());
    ManifestFile initialManifest = commitBefore.allManifests(table.io()).get(0);
    validateManifest(
        initialManifest,
        dataSeqs(1L, 1L),
        fileSeqs(1L, 1L),
        ids(baseId, baseId),
        files(FILE_C, FILE_D),
        statuses(Status.ADDED, Status.ADDED));

    ManifestFile manifest = writeManifest(FILE_A, FILE_B); // Added Data File FILE_A, FILE_B
    commit(
        super.table,
        super.table
            .newOverwrite()
            .appendManifest(manifest)
            .deleteFile(FILE_C), // Delete Data File FILE_C
        branch);

    V1Assert.assertEquals(
        "Last sequence number should be 0", 0, super.table.ops().current().lastSequenceNumber());
    V2Assert.assertEquals(
        "Last sequence number should be 2", 2, super.table.ops().current().lastSequenceNumber());

    Snapshot committedAfter = latestSnapshot(super.table, branch);
    Assert.assertEquals(
        "Should contain 1 merged manifest for second write",
        1,
        committedAfter.allManifests(super.table.io()).size());
    ManifestFile newManifest = committedAfter.allManifests(super.table.io()).get(0);
    Assert.assertNotEquals(
        "Should not contain manifest from initial write", initialManifest, newManifest);

    long snapshotId = committedAfter.snapshotId();

    validateManifest(
        newManifest,
        dataSeqs(2L, 2L, 1L, 1L),
        fileSeqs(2L, 2L, 1L, 1L),
        ids(snapshotId, snapshotId, snapshotId, baseId),
        concat(files(FILE_A, FILE_B), files(initialManifest)), // FILE_A, FILE_B, FILE_C, FILE_D
        statuses(Status.ADDED, Status.ADDED, Status.DELETED, Status.EXISTING)); // FILE_C - DELETED
  }
}
