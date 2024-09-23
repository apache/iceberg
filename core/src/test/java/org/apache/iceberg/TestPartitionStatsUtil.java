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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestPartitionStatsUtil {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @TempDir public File temp;

  @Test
  public void testPartitionStatsOnEmptyTable() throws Exception {
    Table testTable = TestTables.create(tempDir("empty_table"), "empty_table", SCHEMA, SPEC, 2);
    assertThatThrownBy(
            () -> PartitionStatsUtil.computeStats(testTable, testTable.currentSnapshot()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("snapshot cannot be null");
  }

  @Test
  public void testPartitionStatsOnUnPartitionedTable() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("unpartitioned_table"),
            "unpartitioned_table",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            2);

    List<DataFile> files = prepareDataFiles(testTable);
    AppendFiles appendFiles = testTable.newAppend();
    files.forEach(appendFiles::appendFile);
    appendFiles.commit();

    assertThatThrownBy(
            () -> PartitionStatsUtil.computeStats(testTable, testTable.currentSnapshot()))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Computing partition stats for an unpartitioned table");
  }

  @Test
  public void testPartitionStats() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("partition_stats_compute"), "partition_stats_compute", SCHEMA, SPEC, 2);

    List<DataFile> files = prepareDataFiles(testTable);
    for (int i = 0; i < 3; i++) {
      // insert same set of records thrice to have a new manifest files
      AppendFiles appendFiles = testTable.newAppend();
      files.forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    Snapshot snapshot1 = testTable.currentSnapshot();
    Types.StructType partitionType = Partitioning.partitionType(testTable);
    computeAndValidatePartitionStats(
        testTable,
        Tuple.tuple(
            partitionData(partitionType, "foo", "A"),
            0,
            3 * files.get(0).recordCount(),
            3,
            3 * files.get(0).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "foo", "B"),
            0,
            3 * files.get(1).recordCount(),
            3,
            3 * files.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "A"),
            0,
            3 * files.get(2).recordCount(),
            3,
            3 * files.get(2).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "B"),
            0,
            3 * files.get(3).recordCount(),
            3,
            3 * files.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));

    DeleteFile deleteFile =
        FileGenerationUtil.generatePositionDeleteFile(testTable, TestHelpers.Row.of("foo", "A"));
    testTable.newRowDelta().addDeletes(deleteFile).commit();
    Snapshot snapshot2 = testTable.currentSnapshot();

    DeleteFile eqDelete =
        FileGenerationUtil.generateEqualityDeleteFile(testTable, TestHelpers.Row.of("bar", "B"));
    testTable.newRowDelta().addDeletes(eqDelete).commit();
    Snapshot snapshot3 = testTable.currentSnapshot();

    computeAndValidatePartitionStats(
        testTable,
        Tuple.tuple(
            partitionData(partitionType, "foo", "A"),
            0,
            3 * files.get(0).recordCount(),
            3,
            3 * files.get(0).fileSizeInBytes(),
            deleteFile.recordCount(), // position delete file count
            1, // one position delete file
            0L,
            0,
            0L,
            snapshot2.timestampMillis(), // new snapshot from pos delete commit
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "foo", "B"),
            0,
            3 * files.get(1).recordCount(),
            3,
            3 * files.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "A"),
            0,
            3 * files.get(2).recordCount(),
            3,
            3 * files.get(2).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "B"),
            0,
            3 * files.get(3).recordCount(),
            3,
            3 * files.get(3).fileSizeInBytes(),
            0L,
            0,
            eqDelete.recordCount(),
            1, // one equality delete file
            0L,
            snapshot3.timestampMillis(), // new snapshot from equality delete commit
            snapshot3.snapshotId()));
  }

  @Test
  public void testPartitionStatsWithSchemaEvolution() throws Exception {
    final PartitionSpec specBefore = PartitionSpec.builderFor(SCHEMA).identity("c2").build();

    Table testTable =
        TestTables.create(
            tempDir("partition_stats_schema_evolve"),
            "partition_stats_schema_evolve",
            SCHEMA,
            specBefore,
            SortOrder.unsorted(),
            2);

    List<DataFile> dataFiles = prepareDataFilesOnePart(testTable);
    for (int i = 0; i < 2; i++) {
      AppendFiles appendFiles = testTable.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();
    }
    Snapshot snapshot1 = testTable.currentSnapshot();
    Types.StructType partitionType = Partitioning.partitionType(testTable);

    computeAndValidatePartitionStats(
        testTable,
        Tuple.tuple(
            partitionData(partitionType, "foo"),
            0,
            2 * dataFiles.get(0).recordCount(),
            2,
            2 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar"),
            0,
            2 * dataFiles.get(1).recordCount(),
            2,
            2 * dataFiles.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));

    // Evolve the partition spec to include c3
    testTable.updateSpec().addField("c3").commit();
    List<DataFile> filesWithNewSpec = prepareDataFiles(testTable);
    filesWithNewSpec.add(
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("bar", null)));
    partitionType = Partitioning.partitionType(testTable);

    AppendFiles appendFiles = testTable.newAppend();
    filesWithNewSpec.forEach(appendFiles::appendFile);
    appendFiles.commit();
    Snapshot snapshot2 = testTable.currentSnapshot();

    computeAndValidatePartitionStats(
        testTable,
        Tuple.tuple(
            partitionData(partitionType, "foo", null), // unified tuple
            0, // old spec id as the record is unmodified
            2 * dataFiles.get(0).recordCount(),
            2,
            2 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", null),
            1, // new spec id as same partition inserted after evolution
            2 * dataFiles.get(1).recordCount() + filesWithNewSpec.get(4).recordCount(),
            3,
            2 * dataFiles.get(1).fileSizeInBytes() + filesWithNewSpec.get(4).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "foo", "A"),
            1, // new spec id
            filesWithNewSpec.get(0).recordCount(),
            1,
            filesWithNewSpec.get(0).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot2.timestampMillis(), // new snapshot
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "foo", "B"),
            1,
            filesWithNewSpec.get(1).recordCount(),
            1,
            filesWithNewSpec.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "A"),
            1,
            filesWithNewSpec.get(2).recordCount(),
            1,
            filesWithNewSpec.get(2).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionData(partitionType, "bar", "B"),
            1,
            filesWithNewSpec.get(3).recordCount(),
            1,
            filesWithNewSpec.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            0L,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()));
  }

  private static PartitionData partitionData(Types.StructType partitionType, String c2, String c3) {
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, c2);
    partitionData.set(1, c3);
    return partitionData;
  }

  private static PartitionData partitionData(Types.StructType partitionType, String c2) {
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, c2);
    return partitionData;
  }

  private static List<DataFile> prepareDataFiles(Table table) {
    List<DataFile> dataFiles = Lists.newArrayList();
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("foo", "A")));
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("foo", "B")));
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("bar", "A")));
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("bar", "B")));

    return dataFiles;
  }

  private static List<DataFile> prepareDataFilesOnePart(Table table) {
    List<DataFile> dataFiles = Lists.newArrayList();
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("foo")));
    dataFiles.add(FileGenerationUtil.generateDataFile(table, TestHelpers.Row.of("bar")));

    return dataFiles;
  }

  private static void computeAndValidatePartitionStats(Table testTable, Tuple... expectedValues) {
    // compute and commit partition stats file
    Collection<PartitionStats> result =
        PartitionStatsUtil.computeStats(testTable, testTable.currentSnapshot());

    assertThat(result)
        .extracting(
            PartitionStats::partition,
            PartitionStats::specId,
            PartitionStats::dataRecordCount,
            PartitionStats::dataFileCount,
            PartitionStats::totalDataFileSizeInBytes,
            PartitionStats::positionDeleteRecordCount,
            PartitionStats::positionDeleteFileCount,
            PartitionStats::equalityDeleteRecordCount,
            PartitionStats::equalityDeleteFileCount,
            PartitionStats::totalRecordCount,
            PartitionStats::lastUpdatedAt,
            PartitionStats::lastUpdatedSnapshotId)
        .containsExactlyInAnyOrder(expectedValues);
  }

  private File tempDir(String folderName) throws IOException {
    return java.nio.file.Files.createTempDirectory(temp.toPath(), folderName).toFile();
  }
}
