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

import static org.apache.iceberg.PartitionStatistics.EMPTY_PARTITION_FIELD;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;

public abstract class PartitionStatisticsScanTestBase extends PartitionStatisticsTestBase {

  public abstract FileFormat format();

  private final Map<String, String> fileFormatProperty =
      ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format().name());

  @Test
  public void testEmptyTable() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_empty_table"), "scan_empty_table", SCHEMA, SPEC, 2, fileFormatProperty);

    assertThat(Lists.newArrayList(testTable.newPartitionStatisticsScan().scan())).isEmpty();
  }

  @Test
  public void testInvalidSnapshotId() throws Exception {
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
  public void testNoStatsForSnapshot() throws Exception {
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
    Schema recordSchema = PartitionStatistics.schema(Partitioning.partitionType(testTable), 2);

    Types.StructType partitionType =
        recordSchema.findField(EMPTY_PARTITION_FIELD.fieldId()).type().asStructType();
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

    Schema recordSchema = PartitionStatistics.schema(Partitioning.partitionType(testTable), 2);

    Types.StructType partitionType =
        recordSchema.findField(EMPTY_PARTITION_FIELD.fieldId()).type().asStructType();

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

  @Test
  public void testProjectStatFields() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_project_subset"),
            "scan_project_subset",
            SCHEMA,
            SPEC,
            3,
            fileFormatProperty);

    DataFile dataFile1 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    DataFile dataFile2 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B"));
    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long snapshotId = testTable.currentSnapshot().snapshotId();
    PartitionStatisticsFile result =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotId);
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();

    Schema projection =
        new Schema(
            PartitionStatistics.EMPTY_PARTITION_FIELD,
            PartitionStatistics.DATA_RECORD_COUNT,
            PartitionStatistics.DATA_FILE_COUNT,
            PartitionStatistics.LAST_UPDATED_SNAPSHOT_ID);

    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().project(projection).scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(partitionStats)
        .extracting(
            PartitionStatistics::partition,
            PartitionStatistics::dataRecordCount,
            PartitionStatistics::dataFileCount,
            PartitionStatistics::lastUpdatedSnapshotId)
        .containsExactlyInAnyOrder(
            Tuple.tuple(
                partitionRecord(partitionType, "foo", "A"), dataFile1.recordCount(), 1, snapshotId),
            Tuple.tuple(
                partitionRecord(partitionType, "foo", "B"),
                dataFile2.recordCount(),
                1,
                snapshotId));

    assertThat(partitionStats)
        .allSatisfy(
            stats -> {
              assertThat(stats.specId()).isNull();
              assertThat(stats.totalDataFileSizeInBytes()).isNull();
              assertThat(stats.positionDeleteRecordCount()).isNull();
              assertThat(stats.positionDeleteFileCount()).isNull();
              assertThat(stats.equalityDeleteRecordCount()).isNull();
              assertThat(stats.equalityDeleteFileCount()).isNull();
              assertThat(stats.totalRecords()).isNull();
              assertThat(stats.lastUpdatedAt()).isNull();
              assertThat(stats.dvCount()).isNull();
            });
  }

  @Test
  public void testProjectNullSchemaIsRejected() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_project_null"), "scan_project_null", SCHEMA, SPEC, 2, fileFormatProperty);

    assertThatThrownBy(() -> testTable.newPartitionStatisticsScan().project(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid projection schema");
  }

  @Test
  public void testProjectIgnoresUnknownField() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_project_unknown"),
            "scan_project_unknown",
            SCHEMA,
            SPEC,
            3,
            fileFormatProperty);

    DataFile dataFile1 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    DataFile dataFile2 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B"));
    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long snapshotId = testTable.currentSnapshot().snapshotId();
    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(
            PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotId))
        .commit();

    Schema projection =
        new Schema(
            PartitionStatistics.EMPTY_PARTITION_FIELD,
            PartitionStatistics.DATA_RECORD_COUNT,
            Types.NestedField.optional(9999, "not_a_stats_field", Types.StringType.get()));

    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().project(projection).scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    // only the valid projected fields are populated
    assertThat(partitionStats)
        .extracting(PartitionStatistics::partition, PartitionStatistics::dataRecordCount)
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), dataFile1.recordCount()),
            Tuple.tuple(partitionRecord(partitionType, "foo", "B"), dataFile2.recordCount()));

    assertThat(partitionStats)
        .allSatisfy(
            stats -> {
              assertThat(stats.specId()).isNull();
              assertThat(stats.dataFileCount()).isNull();
              assertThat(stats.totalDataFileSizeInBytes()).isNull();
              assertThat(stats.positionDeleteRecordCount()).isNull();
              assertThat(stats.positionDeleteFileCount()).isNull();
              assertThat(stats.equalityDeleteRecordCount()).isNull();
              assertThat(stats.equalityDeleteFileCount()).isNull();
              assertThat(stats.totalRecords()).isNull();
              assertThat(stats.lastUpdatedAt()).isNull();
              assertThat(stats.lastUpdatedSnapshotId()).isNull();
              assertThat(stats.dvCount()).isNull();
            });
  }

  @Test
  public void testNullFilter() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("scan_filter_null"), "scan_filter_null", SCHEMA, SPEC, 2, fileFormatProperty);

    assertThatThrownBy(() -> testTable.newPartitionStatisticsScan().filter(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid filter");
  }

  @Test
  public void testAlwaysTrueFilter() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_always_true");

    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().filter(Expressions.alwaysTrue()).scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats).hasSize(2);
  }

  @Test
  public void testAlwaysFalseFilter() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_always_false");

    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().filter(Expressions.alwaysFalse()).scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats).isEmpty();
  }

  @Test
  public void testFilterOnPartitionColumn() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_partition_col");

    List<PartitionStatistics> results;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable
            .newPartitionStatisticsScan()
            .filter(
                Expressions.and(
                    Expressions.equal("partition.c2", "foo"),
                    Expressions.equal("partition.c3", "A")))
            .scan()) {
      results = Lists.newArrayList(recordIterator);
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(results)
        .extracting(PartitionStatistics::partition)
        .containsExactlyInAnyOrder(partitionRecord(partitionType, "foo", "A"));
  }

  @Test
  public void testFilterOnStatsField() throws Exception {
    Table testTable = tableWithUnevenPartitions("scan_filter_stats_field");

    List<PartitionStatistics> results;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable
            .newPartitionStatisticsScan()
            .filter(Expressions.greaterThan("data_file_count", 2))
            .scan()) {
      results = Lists.newArrayList(recordIterator);
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(results)
        .extracting(PartitionStatistics::partition, PartitionStatistics::dataFileCount)
        .containsExactlyInAnyOrder(Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 3));
  }

  @Test
  public void testFilterOnUnknownColumnFails() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_unknown_col");

    assertThatThrownBy(
            () ->
                testTable
                    .newPartitionStatisticsScan()
                    .filter(Expressions.equal("does_not_exist", 1))
                    .scan())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'does_not_exist'");
  }

  @Test
  public void testFilterOnUnknownPartitionSubFieldFails() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_unknown_partition");

    assertThatThrownBy(
            () ->
                testTable
                    .newPartitionStatisticsScan()
                    .filter(Expressions.equal("partition.no_such_col", "foo"))
                    .scan())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'partition.no_such_col'");
  }

  @Test
  public void testFilterDvCountOnV2Stats() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_v2_dvcount");

    assertThatThrownBy(
            () ->
                testTable
                    .newPartitionStatisticsScan()
                    .filter(Expressions.isNull("dv_count"))
                    .scan())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'dv_count'");
  }

  @Test
  public void testFilterColumnIncludedInProjection() throws Exception {
    Table testTable = tableWithUnevenPartitions("scan_filter_in_projection");

    Schema projection =
        new Schema(
            PartitionStatistics.EMPTY_PARTITION_FIELD,
            PartitionStatistics.DATA_FILE_COUNT,
            PartitionStatistics.DATA_RECORD_COUNT);

    List<PartitionStatistics> results;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable
            .newPartitionStatisticsScan()
            .filter(Expressions.greaterThan("data_file_count", 2L))
            .project(projection)
            .scan()) {
      results = Lists.newArrayList(recordIterator);
    }

    long expectedDataRecordCount = 0L;
    try (CloseableIterable<FileScanTask> tasks = testTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if ("A".equals(task.file().partition().get(1, String.class))) {
          expectedDataRecordCount += task.file().recordCount();
        }
      }
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(results)
        .extracting(
            PartitionStatistics::partition,
            PartitionStatistics::dataFileCount,
            PartitionStatistics::dataRecordCount)
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 3, expectedDataRecordCount));

    assertThat(results)
        .allSatisfy(
            stats -> {
              assertThat(stats.specId()).isNull();
              assertThat(stats.totalDataFileSizeInBytes()).isNull();
              assertThat(stats.positionDeleteRecordCount()).isNull();
              assertThat(stats.positionDeleteFileCount()).isNull();
              assertThat(stats.equalityDeleteRecordCount()).isNull();
              assertThat(stats.equalityDeleteFileCount()).isNull();
              assertThat(stats.totalRecords()).isNull();
              assertThat(stats.lastUpdatedAt()).isNull();
              assertThat(stats.lastUpdatedSnapshotId()).isNull();
              assertThat(stats.dvCount()).isNull();
            });
  }

  @Test
  public void testFilterColumnNotInProjection() throws Exception {
    Table testTable = tableWithUnevenPartitions("scan_filter_outside_projection");

    Schema projection =
        new Schema(
            PartitionStatistics.EMPTY_PARTITION_FIELD, PartitionStatistics.DATA_RECORD_COUNT);

    List<PartitionStatistics> results;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable
            .newPartitionStatisticsScan()
            .filter(Expressions.greaterThan("data_file_count", 2L))
            .project(projection)
            .scan()) {
      results = Lists.newArrayList(recordIterator);
    }

    long expectedDataRecordCount = 0L;
    try (CloseableIterable<FileScanTask> tasks = testTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        if ("A".equals(task.file().partition().get(1, String.class))) {
          expectedDataRecordCount += task.file().recordCount();
        }
      }
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(results)
        .extracting(
            PartitionStatistics::partition,
            PartitionStatistics::dataFileCount,
            PartitionStatistics::dataRecordCount)
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 3, expectedDataRecordCount));

    assertThat(results)
        .allSatisfy(
            stats -> {
              assertThat(stats.specId()).isNull();
              assertThat(stats.totalDataFileSizeInBytes()).isNull();
              assertThat(stats.positionDeleteRecordCount()).isNull();
              assertThat(stats.positionDeleteFileCount()).isNull();
              assertThat(stats.equalityDeleteRecordCount()).isNull();
              assertThat(stats.equalityDeleteFileCount()).isNull();
              assertThat(stats.totalRecords()).isNull();
              assertThat(stats.lastUpdatedAt()).isNull();
              assertThat(stats.lastUpdatedSnapshotId()).isNull();
              assertThat(stats.dvCount()).isNull();
            });
  }

  @Test
  public void testCaseSensitiveFilterFailsOnUppercaseRef() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_case_sensitive");

    assertThatThrownBy(
            () ->
                testTable
                    .newPartitionStatisticsScan()
                    .filter(Expressions.greaterThan("DATA_RECORD_COUNT", 0L))
                    .scan())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Cannot find field 'DATA_RECORD_COUNT'");
  }

  @Test
  public void testCaseInsensitiveFilterMatchesUppercaseRef() throws Exception {
    Table testTable = tableWithTwoPartitions("scan_filter_case_insensitive");

    List<PartitionStatistics> results;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable
            .newPartitionStatisticsScan()
            .filter(Expressions.equal("partition.C3", "B"))
            .caseSensitive(false)
            .scan()) {
      results = Lists.newArrayList(recordIterator);
    }

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    assertThat(results)
        .extracting(PartitionStatistics::partition)
        .containsExactlyInAnyOrder(partitionRecord(partitionType, "foo", "B"));
  }

  private Table tableWithTwoPartitions(String name) throws IOException {
    Table testTable = TestTables.create(tempDir(name), name, SCHEMA, SPEC, 2, fileFormatProperty);

    DataFile dataFile1 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    DataFile dataFile2 =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B"));
    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    long snapshotId = testTable.currentSnapshot().snapshotId();
    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(
            PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotId))
        .commit();
    return testTable;
  }

  private Table tableWithUnevenPartitions(String name) throws IOException {
    Table testTable = TestTables.create(tempDir(name), name, SCHEMA, SPEC, 2, fileFormatProperty);

    for (int i = 0; i < 3; i++) {
      testTable
          .newAppend()
          .appendFile(
              FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A")))
          .commit();
    }
    testTable
        .newAppend()
        .appendFile(FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "B")))
        .commit();

    long snapshotId = testTable.currentSnapshot().snapshotId();
    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(
            PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotId))
        .commit();
    return testTable;
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
