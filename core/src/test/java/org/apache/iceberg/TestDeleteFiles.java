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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ManifestEntry.Status;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestDeleteFiles extends TestBase {

  private static final DataFile DATA_FILE_BUCKET_0_IDS_0_2 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-1.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 5L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  null, // no nan value counts
                  ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(2L)) // upper bounds
                  ))
          .build();

  private static final DataFile DATA_FILE_BUCKET_0_IDS_8_10 =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-2.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withMetrics(
              new Metrics(
                  5L,
                  null, // no column sizes
                  ImmutableMap.of(1, 5L, 2, 5L), // value count
                  ImmutableMap.of(1, 0L, 2, 0L), // null count
                  null, // no nan value counts
                  ImmutableMap.of(1, longToBuffer(8L)), // lower bounds
                  ImmutableMap.of(1, longToBuffer(10L)) // upper bounds
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

  @TestTemplate
  public void testMultipleDeletes() {
    commit(
        table, table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C), branch);
    Snapshot append = latestSnapshot(readMetadata(), branch);
    assertThat(version()).isEqualTo(1);
    validateSnapshot(null, append, FILE_A, FILE_B, FILE_C);

    commit(table, table.newDelete().deleteFile(FILE_A), branch);
    Snapshot delete1 = latestSnapshot(readMetadata(), branch);

    assertThat(version()).isEqualTo(2);
    assertThat(delete1.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        delete1.allManifests(table.io()).get(0),
        ids(delete1.snapshotId(), append.snapshotId(), append.snapshotId()),
        files(FILE_A, FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING, Status.EXISTING));

    Snapshot delete2 = commit(table, table.newDelete().deleteFile(FILE_B), branch);
    assertThat(version()).isEqualTo(3);
    assertThat(delete2.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        delete2.allManifests(FILE_IO).get(0),
        ids(delete2.snapshotId(), append.snapshotId()),
        files(FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING));
  }

  @TestTemplate
  public void testAlreadyDeletedFilesAreIgnoredDuringDeletesByRowFilter() {
    PartitionSpec spec = table.spec();

    DataFile firstDataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-2.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withMetrics(
                new Metrics(
                    5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 5L), // value count
                    ImmutableMap.of(1, 0L, 2, 0L), // null count
                    null, // no nan value counts
                    ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(10L)) // upper bounds
                    ))
            .build();

    DataFile secondDataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-1.parquet")
            .withFileSizeInBytes(10)
            .withPartitionPath("data_bucket=0")
            .withMetrics(
                new Metrics(
                    5L,
                    null, // no column sizes
                    ImmutableMap.of(1, 5L, 2, 5L), // value count
                    ImmutableMap.of(1, 0L, 2, 0L), // null count
                    null, // no nan value counts
                    ImmutableMap.of(1, longToBuffer(0L)), // lower bounds
                    ImmutableMap.of(1, longToBuffer(4L)) // upper bounds
                    ))
            .build();

    // add both data files
    Snapshot initialSnapshot =
        commit(
            table,
            table.newFastAppend().appendFile(firstDataFile).appendFile(secondDataFile),
            branch);

    assertThat(initialSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(firstDataFile, secondDataFile),
        statuses(Status.ADDED, Status.ADDED));

    // delete the first data file
    Snapshot deleteSnapshot = commit(table, table.newDelete().deleteFile(firstDataFile), branch);
    assertThat(deleteSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(deleteSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(firstDataFile, secondDataFile),
        statuses(Status.DELETED, Status.EXISTING));

    // delete the second data file using a row filter
    // the commit should succeed as there is only one live data file
    Snapshot finalSnapshot =
        commit(table, table.newDelete().deleteFromRowFilter(Expressions.lessThan("id", 7)), branch);

    assertThat(finalSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        finalSnapshot.allManifests(FILE_IO).get(0),
        ids(finalSnapshot.snapshotId()),
        files(secondDataFile),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testDeleteSomeFilesByRowFilterWithoutPartitionPredicates() {
    // add both data files
    Snapshot initialSnapshot =
        commit(
            table,
            table
                .newFastAppend()
                .appendFile(DATA_FILE_BUCKET_0_IDS_0_2)
                .appendFile(DATA_FILE_BUCKET_0_IDS_8_10),
            branch);

    assertThat(initialSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.ADDED, Status.ADDED));

    // delete the second one using a metrics filter (no partition filter)
    Snapshot deleteSnapshot =
        commit(
            table, table.newDelete().deleteFromRowFilter(Expressions.greaterThan("id", 5)), branch);

    assertThat(deleteSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.EXISTING, Status.DELETED));
  }

  @TestTemplate
  public void testDeleteSomeFilesByRowFilterWithCombinedPredicates() {
    // add both data files
    Snapshot initialSnapshot =
        commit(
            table,
            table
                .newFastAppend()
                .appendFile(DATA_FILE_BUCKET_0_IDS_0_2)
                .appendFile(DATA_FILE_BUCKET_0_IDS_8_10),
            branch);

    assertThat(initialSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        initialSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.ADDED, Status.ADDED));

    // delete the second one using a filter that relies on metrics and partition data
    Expression partPredicate = Expressions.equal(Expressions.bucket("data", 16), 0);
    Expression rowPredicate = Expressions.greaterThan("id", 5);
    Expression predicate = Expressions.and(partPredicate, rowPredicate);
    Snapshot deleteSnapshot =
        commit(table, table.newDelete().deleteFromRowFilter(predicate), branch);
    assertThat(deleteSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(initialSnapshot.snapshotId(), deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2, DATA_FILE_BUCKET_0_IDS_8_10),
        statuses(Status.EXISTING, Status.DELETED));
  }

  @TestTemplate
  public void testCannotDeleteFileWhereNotAllRowsMatchPartitionFilter() {
    assumeThat(formatVersion).isEqualTo(2);

    table
        .updateSpec()
        .removeField(Expressions.bucket("data", 16))
        .addField(Expressions.truncate("data", 2))
        .commit();

    PartitionSpec spec = table.spec();

    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("/path/to/data-1.parquet")
            .withRecordCount(10)
            .withFileSizeInBytes(10)
            .withPartitionPath("data_trunc_2=aa")
            .build();

    commit(table, table.newFastAppend().appendFile(dataFile), branch);

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table.newDelete().deleteFromRowFilter(Expressions.equal("data", "aa")),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot delete file where some, but not all, rows match filter");
  }

  @TestTemplate
  public void testDeleteCaseSensitivity() {
    commit(table, table.newFastAppend().appendFile(DATA_FILE_BUCKET_0_IDS_0_2), branch);

    Expression rowFilter = Expressions.lessThan("iD", 5);

    assertThatThrownBy(
            () -> commit(table, table.newDelete().deleteFromRowFilter(rowFilter), branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'iD'");

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table.newDelete().deleteFromRowFilter(rowFilter).caseSensitive(true),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Cannot find field 'iD'");

    Snapshot deleteSnapshot =
        commit(
            table, table.newDelete().deleteFromRowFilter(rowFilter).caseSensitive(false), branch);

    assertThat(deleteSnapshot.allManifests(FILE_IO)).hasSize(1);
    validateManifestEntries(
        deleteSnapshot.allManifests(FILE_IO).get(0),
        ids(deleteSnapshot.snapshotId()),
        files(DATA_FILE_BUCKET_0_IDS_0_2),
        statuses(Status.DELETED));
  }

  @TestTemplate
  public void testDeleteFilesOnIndependentBranches() {
    String testBranch = "testBranch";
    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();
    Snapshot initialSnapshot = table.currentSnapshot();
    // Delete A on test branch
    table.newDelete().deleteFile(FILE_A).toBranch(testBranch).commit();
    Snapshot testBranchTip = table.snapshot(testBranch);

    // Delete B and C on main
    table.newDelete().deleteFile(FILE_B).deleteFile(FILE_C).commit();
    Snapshot delete2 = table.currentSnapshot();

    // Verify B and C on testBranch
    validateManifestEntries(
        Iterables.getOnlyElement(testBranchTip.allManifests(FILE_IO)),
        ids(testBranchTip.snapshotId(), initialSnapshot.snapshotId(), initialSnapshot.snapshotId()),
        files(FILE_A, FILE_B, FILE_C),
        statuses(Status.DELETED, Status.EXISTING, Status.EXISTING));

    // Verify A on main
    validateManifestEntries(
        Iterables.getOnlyElement(delete2.allManifests(FILE_IO)),
        ids(initialSnapshot.snapshotId(), delete2.snapshotId(), delete2.snapshotId()),
        files(FILE_A, FILE_B, FILE_C),
        statuses(Status.EXISTING, Status.DELETED, Status.DELETED));
  }

  @TestTemplate
  public void testDeleteWithCollision() {
    Schema schema = new Schema(Types.NestedField.of(0, false, "x", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("x").build();
    Table collisionTable =
        TestTables.create(tableDir, "hashcollision", schema, spec, formatVersion);

    PartitionData partitionOne = new PartitionData(spec.partitionType());
    partitionOne.set(0, "Aa");
    PartitionData partitionTwo = new PartitionData(spec.partitionType());
    partitionTwo.set(0, "BB");

    assertThat(StructLikeWrapper.forType(spec.partitionType()).set(partitionTwo).hashCode())
        .isEqualTo(StructLikeWrapper.forType(spec.partitionType()).set(partitionOne).hashCode());

    DataFile testFileOne =
        DataFiles.builder(spec)
            .withPartition(partitionOne)
            .withPath("/g1.parquet")
            .withFileSizeInBytes(100)
            .withRecordCount(1)
            .build();

    DataFile testFileTwo =
        DataFiles.builder(spec)
            .withPartition(partitionTwo)
            .withRecordCount(1)
            .withFileSizeInBytes(100)
            .withPath("/g2.parquet")
            .build();

    collisionTable.newFastAppend().appendFile(testFileOne).appendFile(testFileTwo).commit();

    List<StructLike> beforeDeletePartitions =
        Lists.newArrayList(collisionTable.newScan().planFiles().iterator()).stream()
            .map(s -> ((PartitionData) s.partition()).copy())
            .collect(Collectors.toList());

    assertThat(beforeDeletePartitions).containsExactly(partitionOne, partitionTwo);

    collisionTable.newDelete().deleteFromRowFilter(Expressions.equal("x", "BB")).commit();

    List<StructLike> afterDeletePartitions =
        Lists.newArrayList(collisionTable.newScan().planFiles().iterator()).stream()
            .map(s -> ((PartitionData) s.partition()).copy())
            .collect(Collectors.toList());

    assertThat(afterDeletePartitions).containsExactly(partitionOne);
  }

  @TestTemplate
  public void testDeleteValidateFileExistence() {
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);
    Snapshot delete =
        commit(table, table.newDelete().deleteFile(FILE_B).validateFilesExist(), branch);
    validateManifestEntries(
        Iterables.getOnlyElement(delete.allManifests(FILE_IO)),
        ids(delete.snapshotId()),
        files(FILE_B),
        statuses(Status.DELETED));

    assertThatThrownBy(
            () -> commit(table, table.newDelete().deleteFile(FILE_B).validateFilesExist(), branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/data-b.parquet");

    assertThatThrownBy(
            () ->
                commit(
                    table,
                    table
                        .newDelete()
                        .deleteFile("/path/to/non-existing.parquet")
                        .validateFilesExist(),
                    branch))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Missing required files to delete: /path/to/non-existing.parquet");
  }

  @TestTemplate
  public void testDeleteFilesNoValidation() {
    commit(table, table.newFastAppend().appendFile(FILE_B), branch);
    Snapshot delete1 = commit(table, table.newDelete().deleteFile(FILE_B), branch);
    validateManifestEntries(
        Iterables.getOnlyElement(delete1.allManifests(FILE_IO)),
        ids(delete1.snapshotId()),
        files(FILE_B),
        statuses(Status.DELETED));

    Snapshot delete2 = commit(table, table.newDelete().deleteFile(FILE_B), branch);
    assertThat(delete2.allManifests(FILE_IO)).isEmpty();
    assertThat(delete2.removedDataFiles(FILE_IO)).isEmpty();
  }

  private static ByteBuffer longToBuffer(long value) {
    return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(0, value);
  }
}
