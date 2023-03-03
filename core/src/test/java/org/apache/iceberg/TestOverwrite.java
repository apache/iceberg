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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestOverwrite extends TableTestBase {
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
                  ImmutableMap.of(1, longToBuffer(5L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(9L)) // upper bounds
                  ))
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

  public TestOverwrite(int formatVersion, String branch) {
    super(formatVersion);
    this.branch = branch;
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }

  private Table table = null;

  @Before
  public void createTestTable() throws IOException {
    File tableDir = temp.newFolder();
    Assert.assertTrue(tableDir.delete());

    this.table =
        TestTables.create(tableDir, TABLE_NAME, DATE_SCHEMA, PARTITION_BY_DATE, formatVersion);

    commit(table, table.newAppend().appendFile(FILE_0_TO_4).appendFile(FILE_5_TO_9), branch);
  }

  @Test
  public void testOverwriteWithoutAppend() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId = latestSnapshot(base, branch).snapshotId();

    commit(table, table.newOverwrite().overwriteByRowFilter(equal("date", "2018-06-08")), branch);

    long overwriteId = latestSnapshot(table, branch).snapshotId();

    Assert.assertNotEquals("Should create a new snapshot", baseId, overwriteId);
    Assert.assertEquals(
        "Table should have one manifest",
        1,
        latestSnapshot(table, branch).allManifests(table.io()).size());

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(overwriteId, baseId),
        files(FILE_0_TO_4, FILE_5_TO_9),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @Test
  public void testOverwriteFailsDelete() {
    TableMetadata base = TestTables.readMetadata(TABLE_NAME);
    long baseId =
        latestSnapshot(base, branch) == null ? -1 : latestSnapshot(base, branch).snapshotId();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(and(equal("date", "2018-06-09"), lessThan("id", 9)));

    AssertHelpers.assertThrows(
        "Should reject commit with file not matching delete expression",
        ValidationException.class,
        "Cannot delete file where some, but not all, rows match filter",
        () -> commit(table, overwrite, branch));

    Assert.assertEquals(
        "Should not create a new snapshot", baseId, latestSnapshot(base, branch).snapshotId());
  }

  @Test
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

    Assert.assertNotEquals("Should create a new snapshot", baseId, overwriteId);
    Assert.assertEquals(
        "Table should have 2 manifests",
        2,
        latestSnapshot(table, branch).allManifests(table.io()).size());

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

  @Test
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

    Assert.assertNotEquals("Should create a new snapshot", baseId, overwriteId);
    Assert.assertEquals(
        "Table should have one merged manifest",
        1,
        latestSnapshot(table, branch).allManifests(table.io()).size());

    validateManifestEntries(
        latestSnapshot(table, branch).allManifests(table.io()).get(0),
        ids(overwriteId, overwriteId, baseId),
        files(FILE_10_TO_14, FILE_0_TO_4, FILE_5_TO_9),
        statuses(Status.ADDED, Status.DELETED, Status.EXISTING));
  }

  @Test
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

    AssertHelpers.assertThrows(
        "Should reject commit with file not matching delete expression",
        ValidationException.class,
        "Cannot append file with rows that do not match filter",
        () -> commit(table, overwrite, branch));

    Assert.assertEquals(
        "Should not create a new snapshot", baseId, latestSnapshot(table, branch).snapshotId());
  }

  @Test
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

    AssertHelpers.assertThrows(
        "Should reject commit with file not matching delete expression",
        ValidationException.class,
        "Cannot append file with rows that do not match filter",
        () -> commit(table, overwrite, branch));

    Assert.assertEquals(
        "Should not create a new snapshot", baseId, latestSnapshot(base, branch).snapshotId());
  }

  @Test
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

    AssertHelpers.assertThrows(
        "Should reject commit with file not matching delete expression",
        ValidationException.class,
        "Cannot append file with rows that do not match filter",
        () -> commit(table, overwrite, branch));

    Assert.assertEquals(
        "Should not create a new snapshot", baseId, latestSnapshot(base, branch).snapshotId());
  }
}
