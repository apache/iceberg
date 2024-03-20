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

import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.iceberg.util.SnapshotUtil.latestSnapshot;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestOverwriteWithValidation extends TestBase {

  private static final String TABLE_NAME = "overwrite_table";

  private static final Schema DATE_SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "data", Types.StringType.get()),
          required(3, "date", Types.StringType.get()));

  private static final PartitionSpec PARTITION_SPEC =
      PartitionSpec.builderFor(DATE_SCHEMA).identity("date").build();

  private static final DataFile FILE_DAY_1 =
      DataFiles.builder(PARTITION_SPEC)
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

  private static final DeleteFile FILE_DAY_1_POS_DELETES =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-1-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-08")
          .withRecordCount(1)
          .build();

  private static final DataFile FILE_DAY_2 =
      DataFiles.builder(PARTITION_SPEC)
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

  private static final DeleteFile FILE_DAY_2_EQ_DELETES =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-2-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-09")
          .withRecordCount(1)
          .build();

  private static final DeleteFile FILE_DAY_2_POS_DELETES =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofPositionDeletes()
          .withPath("/path/to/data-2-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-09")
          .withRecordCount(1)
          .build();

  private static final DataFile FILE_DAY_2_MODIFIED =
      DataFiles.builder(PARTITION_SPEC)
          .withPath("/path/to/data-3.parquet")
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

  private static final DataFile FILE_DAY_2_ANOTHER_RANGE =
      DataFiles.builder(PARTITION_SPEC)
          .withPath("/path/to/data-3.parquet")
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

  private static final DeleteFile FILE_DAY_2_ANOTHER_RANGE_EQ_DELETES =
      FileMetadata.deleteFileBuilder(PARTITION_SPEC)
          .ofEqualityDeletes()
          .withPath("/path/to/data-3-eq-deletes.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("date=2018-06-09")
          .withRecordCount(1)
          .withMetrics(
              new Metrics(
                  1L,
                  null, // no column sizes
                  ImmutableMap.of(1, 1L, 2, 1L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  null,
                  ImmutableMap.of(1, longToBuffer(10L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(10L)) // upper bounds
                  ))
          .build();

  private static final Expression EXPRESSION_DAY_2 = equal("date", "2018-06-09");

  private static final Expression EXPRESSION_DAY_2_ID_RANGE =
      and(greaterThanOrEqual("id", 5L), lessThanOrEqual("id", 9L));

  private static final Expression EXPRESSION_DAY_2_ANOTHER_ID_RANGE = greaterThanOrEqual("id", 10L);

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
  public void before() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();
    this.table =
        TestTables.create(tableDir, TABLE_NAME, DATE_SCHEMA, PARTITION_SPEC, formatVersion);
  }

  @TestTemplate
  public void testOverwriteEmptyTableNotValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newOverwrite().addFile(FILE_DAY_2_MODIFIED), branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteEmptyTableStrictValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(
        table,
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(alwaysTrue())
            .validateNoConflictingData(),
        branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteEmptyTableValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(
        table,
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData(),
        branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteTableNotValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    commit(table, table.newOverwrite().deleteFile(FILE_DAY_2).addFile(FILE_DAY_2_MODIFIED), branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteTableStrictValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    commit(
        table,
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(alwaysTrue())
            .validateNoConflictingData(),
        branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteTableValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    commit(
        table,
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData(),
        branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteCompatibleAdditionNotValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);

    validateSnapshot(null, latestSnapshot(table, branch), FILE_DAY_2);

    OverwriteFiles overwrite =
        table.newOverwrite().deleteFile(FILE_DAY_2).addFile(FILE_DAY_2_MODIFIED);

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteCompatibleAdditionStrictValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(alwaysTrue())
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteCompatibleAdditionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteCompatibleDeletionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newDelete().deleteFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteIncompatibleAdditionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteIncompatibleDeletionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newDelete().deleteFile(FILE_DAY_2), branch);
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Missing required files to delete:");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteCompatibleRewriteAllowed() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(
        table,
        table.newRewrite().rewriteFiles(ImmutableSet.of(FILE_DAY_2), ImmutableSet.of(FILE_DAY_2)),
        branch);
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    commit(table, overwrite, branch);

    assertThat(latestSnapshot(table, branch).snapshotId()).isNotEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteCompatibleExpirationAdditionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch); // id 1

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch); // id 2

    table.expireSnapshots().expireSnapshotId(1L).commit();

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteCompatibleExpirationDeletionValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch); // id 1

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    validateSnapshot(null, baseSnapshot, FILE_DAY_1, FILE_DAY_2);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newDelete().deleteFile(FILE_DAY_1), branch); // id 2

    table.expireSnapshots().expireSnapshotId(1L).commit();

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteIncompatibleExpirationValidated() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch); // id 1

    Snapshot baseSnapshot = latestSnapshot(table, branch);
    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch); // id 2

    commit(table, table.newDelete().deleteFile(FILE_DAY_1), branch); // id 3

    table.expireSnapshots().expireSnapshotId(2L).commit();
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot determine history");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteIncompatibleBaseExpirationEmptyTableValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch); // id 1

    commit(table, table.newDelete().deleteFile(FILE_DAY_1), branch); // id 2

    table.expireSnapshots().expireSnapshotId(1L).commit();
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot determine history");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testOverwriteAnotherRangeValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(EXPRESSION_DAY_2_ID_RANGE)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testOverwriteAnotherRangeWithinPartitionValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    Expression conflictDetectionFilter = and(EXPRESSION_DAY_2, EXPRESSION_DAY_2_ID_RANGE);
    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(conflictDetectionFilter)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_2_ANOTHER_RANGE), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_2_ANOTHER_RANGE, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testTransactionCompatibleAdditionValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);

    Snapshot baseSnapshot = latestSnapshot(table, branch);

    Transaction txn = table.newTransaction();
    OverwriteFiles overwrite =
        txn.newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(baseSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);
    txn.commitTransaction();

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testTransactionIncompatibleAdditionValidated() {
    assertThat(latestSnapshot(table, branch)).isNull();

    Transaction txn = table.newTransaction();

    commit(table, txn.newAppend().appendFile(FILE_DAY_1), branch);

    OverwriteFiles overwrite =
        txn.newOverwrite()
            .addFile(FILE_DAY_2_MODIFIED)
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData();

    commit(table, table.newAppend().appendFile(FILE_DAY_2), branch);
    long committedSnapshotId = latestSnapshot(table, branch).snapshotId();

    commit(table, overwrite, branch);

    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");

    assertThat(latestSnapshot(table, branch).snapshotId()).isEqualTo(committedSnapshotId);
  }

  @TestTemplate
  public void testConcurrentConflictingPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_2_POS_DELETES), branch);

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, found new delete for replaced data file");
  }

  @TestTemplate
  public void testConcurrentConflictingPositionDeletesOverwriteByFilter() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_2_POS_DELETES), branch);

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found new conflicting delete");
  }

  @TestTemplate
  public void testConcurrentConflictingDataFileDeleteOverwriteByFilter() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newOverwrite().deleteFile(FILE_DAY_2), branch);

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting deleted files");
  }

  @TestTemplate
  public void testConcurrentNonConflictingDataFileDeleteOverwriteByFilter() {
    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newOverwrite().deleteFile(FILE_DAY_1), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_2_MODIFIED);
  }

  @TestTemplate
  public void testConcurrentNonConflictingPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_1_POS_DELETES), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
    validateBranchDeleteFiles(table, branch, FILE_DAY_1_POS_DELETES);
  }

  @TestTemplate
  public void testConcurrentNonConflictingPositionDeletesOverwriteByFilter() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_1_POS_DELETES), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
    validateBranchDeleteFiles(table, branch, FILE_DAY_1_POS_DELETES);
  }

  @TestTemplate
  public void testConcurrentConflictingEqualityDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_2_EQ_DELETES), branch);

    assertThatThrownBy(() -> commit(table, overwrite, branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot commit, found new delete for replaced data file");
  }

  @TestTemplate
  public void testConcurrentNonConflictingEqualityDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(
        table,
        table.newAppend().appendFile(FILE_DAY_2).appendFile(FILE_DAY_2_ANOTHER_RANGE),
        branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .deleteFile(FILE_DAY_2)
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .conflictDetectionFilter(EXPRESSION_DAY_2_ID_RANGE)
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_2_ANOTHER_RANGE_EQ_DELETES), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_2_ANOTHER_RANGE, FILE_DAY_2_MODIFIED);
    validateBranchDeleteFiles(table, branch, FILE_DAY_2_ANOTHER_RANGE_EQ_DELETES);
  }

  @TestTemplate
  public void testOverwriteByFilterInheritsConflictDetectionFilter() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    Snapshot firstSnapshot = latestSnapshot(table, branch);

    OverwriteFiles overwrite =
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2)
            .validateAddedFilesMatchOverwriteFilter()
            .addFile(FILE_DAY_2_MODIFIED)
            .validateFromSnapshot(firstSnapshot.snapshotId())
            .validateNoConflictingData()
            .validateNoConflictingDeletes();

    commit(table, table.newRowDelta().addDeletes(FILE_DAY_1_POS_DELETES), branch);

    commit(table, overwrite, branch);

    validateBranchFiles(table, branch, FILE_DAY_1, FILE_DAY_2_MODIFIED);
    validateBranchDeleteFiles(table, branch, FILE_DAY_1_POS_DELETES);
  }

  @TestTemplate
  public void testOverwriteCaseSensitivity() {
    commit(table, table.newAppend().appendFile(FILE_DAY_1).appendFile(FILE_DAY_2), branch);

    commit(table, table.newAppend().appendFile(FILE_DAY_1), branch);

    Expression rowFilter = equal("dAtE", "2018-06-09");

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newOverwrite()
                        .addFile(FILE_DAY_2_MODIFIED)
                        .conflictDetectionFilter(rowFilter)
                        .validateNoConflictingData(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtE'");

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newOverwrite()
                        .caseSensitive(true)
                        .addFile(FILE_DAY_2_MODIFIED)
                        .conflictDetectionFilter(rowFilter)
                        .validateNoConflictingData(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'dAtE'");

    // binding should succeed and trigger the validation
    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newOverwrite()
                        .caseSensitive(false)
                        .addFile(FILE_DAY_2_MODIFIED)
                        .conflictDetectionFilter(rowFilter)
                        .validateNoConflictingData(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Found conflicting files");
  }

  @TestTemplate
  public void testMetadataOnlyDeleteWithPositionDeletes() {
    assumeThat(formatVersion).isEqualTo(2);

    assertThat(latestSnapshot(table, branch)).isNull();

    commit(
        table,
        table.newAppend().appendFile(FILE_DAY_2).appendFile(FILE_DAY_2_ANOTHER_RANGE),
        branch);

    commit(
        table,
        table
            .newRowDelta()
            .addDeletes(FILE_DAY_2_POS_DELETES)
            .addDeletes(FILE_DAY_2_ANOTHER_RANGE_EQ_DELETES),
        branch);

    commit(
        table,
        table
            .newOverwrite()
            .overwriteByRowFilter(EXPRESSION_DAY_2_ANOTHER_ID_RANGE)
            .addFile(FILE_DAY_2_MODIFIED),
        branch);

    validateBranchFiles(table, branch, FILE_DAY_2, FILE_DAY_2_MODIFIED);
    validateBranchDeleteFiles(table, branch, FILE_DAY_2_POS_DELETES);
  }
}
