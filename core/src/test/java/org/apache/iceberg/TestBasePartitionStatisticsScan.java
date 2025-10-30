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

import static org.apache.iceberg.PartitionStatsHandler.PARTITION_FIELD_ID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

public abstract class TestBasePartitionStatisticsScan extends PartitionStatisticsTestBase {

  public abstract FileFormat format();

  private final Map<String, String> fileFormatProperty =
      ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format().name());

  @Test
  public void emptyTable() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_empty_table"), "scan_empty_table", SCHEMA, SPEC, 2, fileFormatProperty);

    assertThat(Lists.newArrayList(testTable.newPartitionStatisticsScan().scan())).isEmpty();
  }

  @Test
  public void invalidSnapshotId() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_invalid_snapshot"),
            "scan_invalid_snapshot",
            SCHEMA,
            SPEC,
            2,
            fileFormatProperty);

    assertThatThrownBy(() -> testTable.newPartitionStatisticsScan().useSnapshot(1234L).scan())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find snapshot with ID 1234");
  }

  @Test
  public void noStatsForSnapshot() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_no_stats"), "scan_no_stats", SCHEMA, SPEC, 2, fileFormatProperty);

    DataFile dataFile =
        DataFiles.builder(SPEC)
            .withPath("some_path")
            .withFileSizeInBytes(15)
            .withFormat(format())
            .withRecordCount(1)
            .build();
    testTable.newAppend().appendFile(dataFile).commit();
    long snapshotId = testTable.currentSnapshot().snapshotId();

    assertThat(testTable.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()).isEmpty();
  }

  @Test
  public void testReadingStatsWithInvalidSchema() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table testTable =
        TestTables.create(
            tempDir("scan_with_old_schema"),
            "scan_with_old_schema",
            SCHEMA,
            spec,
            2,
            fileFormatProperty);
    Types.StructType partitionType = Partitioning.partitionType(testTable);
    Schema oldSchema = invalidOldSchema(partitionType);

    // Add a dummy file to the table to have a snapshot
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("some_path")
            .withFileSizeInBytes(15)
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(1)
            .build();
    testTable.newAppend().appendFile(dataFile).commit();
    long snapshotId = testTable.currentSnapshot().snapshotId();

    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(
            PartitionStatsHandler.writePartitionStatsFile(
                testTable,
                snapshotId,
                oldSchema,
                Collections.singletonList(randomStats(partitionType))))
        .commit();

    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {

      if (format() == FileFormat.PARQUET) {
        assertThatThrownBy(() -> Lists.newArrayList(recordIterator))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Not a primitive type: struct");
      } else if (format() == FileFormat.AVRO) {
        assertThatThrownBy(() -> Lists.newArrayList(recordIterator))
            .isInstanceOf(ClassCastException.class)
            .hasMessageContaining("Integer cannot be cast to class org.apache.iceberg.StructLike");
      }
    }
  }

  @Test
  public void testV2toV3SchemaEvolution() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_with_schema_evolution"),
            "scan_with_schema_evolution",
            SCHEMA,
            SPEC,
            2,
            fileFormatProperty);

    // write stats file using v2 schema
    DataFile dataFile =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    testTable.newAppend().appendFile(dataFile).commit();

    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(
            PartitionStatsHandler.computeAndWriteStatsFile(
                testTable, testTable.currentSnapshot().snapshotId()))
        .commit();

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);

    // read with v2 schema
    List<PartitionStatistics> partitionStatsV2;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().scan()) {
      partitionStatsV2 = Lists.newArrayList(recordIterator);
    }

    testTable.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    // read with v3 schema
    List<PartitionStatistics> partitionStatsV3;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().scan()) {
      partitionStatsV3 = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStatsV2).hasSameSizeAs(partitionStatsV3);
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < partitionStatsV2.size(); i++) {
      assertThat(isEqual(comparator, partitionStatsV2.get(i), partitionStatsV3.get(i))).isTrue();
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void testScanPartitionStatsForCurrentSnapshot() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_partition_stats"),
            "scan_partition_stats",
            SCHEMA,
            SPEC,
            2,
            fileFormatProperty);

    DataFile dataFile1 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    DataFile dataFile2 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B"));
    DataFile dataFile3 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("bar", "A"));
    DataFile dataFile4 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("bar", "B"));

    for (int i = 0; i < 3; i++) {
      // insert same set of seven records thrice to have a new manifest files
      testTable
          .newAppend()
          .appendFile(dataFile1)
          .appendFile(dataFile2)
          .appendFile(dataFile3)
          .appendFile(dataFile4)
          .commit();
    }

    Snapshot snapshot1 = testTable.currentSnapshot();
    Schema recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable), 2);

    Types.StructType partitionType =
        recordSchema.findField(PARTITION_FIELD_ID).type().asStructType();
    computeAndValidatePartitionStats(
        testTable,
        testTable.currentSnapshot().snapshotId(),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            3 * dataFile1.recordCount(),
            3,
            3 * dataFile1.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            null),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            3 * dataFile2.recordCount(),
            3,
            3 * dataFile2.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            null),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3 * dataFile3.recordCount(),
            3,
            3 * dataFile3.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            null),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            3 * dataFile4.recordCount(),
            3,
            3 * dataFile4.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            null));

    DeleteFile posDelete =
        FileGenerationUtil.generatePositionDeleteFile(testTable, TestHelpers.Row.of("bar", "A"));
    testTable.newRowDelta().addDeletes(posDelete).commit();
    // snapshot2 is unused in the result as same partition was updated by snapshot4

    DeleteFile eqDelete =
        FileGenerationUtil.generateEqualityDeleteFile(testTable, TestHelpers.Row.of("foo", "A"));
    testTable.newRowDelta().addDeletes(eqDelete).commit();
    Snapshot snapshot3 = testTable.currentSnapshot();

    testTable.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();
    DeleteFile dv = FileGenerationUtil.generateDV(testTable, dataFile3);
    testTable.newRowDelta().addDeletes(dv).commit();
    Snapshot snapshot4 = testTable.currentSnapshot();

    computeAndValidatePartitionStats(
        testTable,
        testTable.currentSnapshot().snapshotId(),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            3 * dataFile1.recordCount(),
            3,
            3 * dataFile1.fileSizeInBytes(),
            0L,
            0,
            eqDelete.recordCount(),
            1,
            null,
            snapshot3.timestampMillis(),
            snapshot3.snapshotId(),
            0),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            3 * dataFile2.recordCount(),
            3,
            3 * dataFile2.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            0),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3 * dataFile3.recordCount(),
            3,
            3 * dataFile3.fileSizeInBytes(),
            posDelete.recordCount() + dv.recordCount(),
            1,
            0L,
            0,
            null,
            snapshot4.timestampMillis(),
            snapshot4.snapshotId(),
            1), // dv count
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            3 * dataFile4.recordCount(),
            3,
            3 * dataFile4.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            0));
  }

  @Test
  public void testScanPartitionStatsForOlderSnapshot() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_older_snapshot"),
            "scan_older_snapshot",
            SCHEMA,
            SPEC,
            2,
            fileFormatProperty);

    DataFile dataFile1 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    DataFile dataFile2 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B"));

    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    Snapshot firstSnapshot = testTable.currentSnapshot();

    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    Schema recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable), 2);

    Types.StructType partitionType =
        recordSchema.findField(PARTITION_FIELD_ID).type().asStructType();

    computeAndValidatePartitionStats(
        testTable,
        firstSnapshot.snapshotId(),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            dataFile1.recordCount(),
            1,
            dataFile1.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            firstSnapshot.timestampMillis(),
            firstSnapshot.snapshotId(),
            null),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            dataFile2.recordCount(),
            1,
            dataFile2.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            firstSnapshot.timestampMillis(),
            firstSnapshot.snapshotId(),
            null));
  }

  private static void computeAndValidatePartitionStats(
      Table testTable, long snapshotId, Tuple... expectedValues) throws IOException {
    PartitionStatisticsFile result =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotId);
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();
    assertThat(result.snapshotId()).isEqualTo(snapshotId);

    PartitionStatisticsScan statScan = testTable.newPartitionStatisticsScan();
    if (testTable.currentSnapshot().snapshotId() != snapshotId) {
      statScan.useSnapshot(snapshotId);
    }

    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator = statScan.scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats)
        .extracting(
            PartitionStatistics::partition,
            PartitionStatistics::specId,
            PartitionStatistics::dataRecordCount,
            PartitionStatistics::dataFileCount,
            PartitionStatistics::totalDataFileSizeInBytes,
            PartitionStatistics::positionDeleteRecordCount,
            PartitionStatistics::positionDeleteFileCount,
            PartitionStatistics::equalityDeleteRecordCount,
            PartitionStatistics::equalityDeleteFileCount,
            PartitionStatistics::totalRecords,
            PartitionStatistics::lastUpdatedAt,
            PartitionStatistics::lastUpdatedSnapshotId,
            PartitionStatistics::dvCount)
        .containsExactlyInAnyOrder(expectedValues);
  }
}
