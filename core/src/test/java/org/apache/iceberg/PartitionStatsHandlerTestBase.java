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
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class PartitionStatsHandlerTestBase extends PartitionStatisticsTestBase {

  public abstract FileFormat format();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return TestHelpers.V2_AND_ABOVE;
  }

  @Parameter protected int formatVersion;

  private final Map<String, String> fileFormatProperty =
      ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format().name());

  private static final PartitionData PARTITION =
      new PartitionData(
          Types.StructType.of(Types.NestedField.required(1, "foo", Types.IntegerType.get())));

  @Test
  public void testPartitionStatsOnEmptyTable() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("empty_table"), "empty_table", SCHEMA, SPEC, 2, fileFormatProperty);
    assertThat(PartitionStatsHandler.computeAndWriteStatsFile(testTable)).isNull();
  }

  @Test
  public void testPartitionStatsOnEmptyBranch() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("empty_branch"), "empty_branch", SCHEMA, SPEC, 2, fileFormatProperty);
    testTable.manageSnapshots().createBranch("b1").commit();
    long branchSnapshot = testTable.refs().get("b1").snapshotId();
    assertThat(PartitionStatsHandler.computeAndWriteStatsFile(testTable, branchSnapshot)).isNull();
  }

  @Test
  public void testPartitionStatsOnInvalidSnapshot() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("invalid_snapshot"), "invalid_snapshot", SCHEMA, SPEC, 2, fileFormatProperty);
    assertThatThrownBy(() -> PartitionStatsHandler.computeAndWriteStatsFile(testTable, 42L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Snapshot not found: 42");
  }

  @Test
  public void testPartitionStatsOnUnPartitionedTable() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("unpartitioned_table"),
            "unpartitioned_table",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            2,
            fileFormatProperty);

    DataFile dataFile = FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of());
    testTable.newAppend().appendFile(dataFile).commit();

    assertThatThrownBy(() -> PartitionStatsHandler.computeAndWriteStatsFile(testTable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table must be partitioned");
  }

  @TestTemplate
  public void testAllDatatypePartitionWriting() throws Exception {
    Schema schema =
        new Schema(
            required(100, "id", Types.LongType.get()),
            optional(101, "data", Types.StringType.get()),
            required(102, "b", Types.BooleanType.get()),
            optional(103, "i", Types.IntegerType.get()),
            required(104, "l", Types.LongType.get()),
            optional(105, "f", Types.FloatType.get()),
            required(106, "d", Types.DoubleType.get()),
            optional(107, "date", Types.DateType.get()),
            required(108, "ts", Types.TimestampType.withoutZone()),
            required(110, "s", Types.StringType.get()),
            required(111, "uuid", Types.UUIDType.get()),
            required(112, "fixed", Types.FixedType.ofLength(7)),
            optional(113, "bytes", Types.BinaryType.get()),
            required(114, "dec_9_0", Types.DecimalType.of(9, 0)),
            required(115, "dec_11_2", Types.DecimalType.of(11, 2)),
            required(116, "dec_38_10", Types.DecimalType.of(38, 10)), // maximum precision
            required(117, "time", Types.TimeType.get()));

    PartitionSpec spec =
        PartitionSpec.builderFor(schema)
            .identity("b")
            .identity("i")
            .identity("l")
            .identity("f")
            .identity("d")
            .identity("date")
            .identity("ts")
            .identity("s")
            .identity("uuid")
            .identity("fixed")
            .identity("bytes")
            .identity("dec_9_0")
            .identity("dec_11_2")
            .identity("dec_38_10")
            .identity("time")
            .build();

    Table testTable =
        TestTables.create(
            tempDir("test_all_type_" + formatVersion),
            "test_all_type_" + formatVersion,
            schema,
            spec,
            formatVersion,
            fileFormatProperty);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatistics.schema(partitionSchema, formatVersion);

    PartitionData partitionData =
        new PartitionData(
            dataSchema.findField(EMPTY_PARTITION_FIELD.fieldId()).type().asStructType());
    partitionData.set(0, true);
    partitionData.set(1, 42);
    partitionData.set(2, 42L);
    partitionData.set(3, 3.14f);
    partitionData.set(4, 3.141592653589793);
    partitionData.set(5, Literal.of("2022-01-01").to(Types.DateType.get()).value());
    partitionData.set(
        6, Literal.of("2017-12-01T10:12:55.038194").to(Types.TimestampType.withoutZone()).value());
    partitionData.set(7, "string");
    partitionData.set(8, UUID.randomUUID());
    partitionData.set(9, ByteBuffer.wrap(new byte[] {0, 1, 2, 3, 4, 5, 6}));
    partitionData.set(10, ByteBuffer.wrap(new byte[] {1, 2, 3}));
    partitionData.set(11, new BigDecimal("123456789"));
    partitionData.set(12, new BigDecimal("1234567.89"));
    partitionData.set(13, new BigDecimal("12345678901234567890.1234567890"));
    partitionData.set(14, Literal.of("10:10:10").to(Types.TimeType.get()).value());

    PartitionStatistics partitionStats = randomStats(partitionData);
    List<PartitionStatistics> expected = Collections.singletonList(partitionStats);

    // Add a dummy file to the table to have a snapshot
    DataFile dataFile =
        DataFiles.builder(spec)
            .withPath("some_path")
            .withPartition(partitionData)
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
                testTable, snapshotId, dataSchema, expected))
        .commit();

    List<PartitionStatistics> written;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
      written = Lists.newArrayList(recordIterator);
    }

    assertThat(written).hasSize(expected.size());
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < written.size(); i++) {
      assertThat(isEqual(comparator, written.get(i), expected.get(i))).isTrue();
    }
  }

  @TestTemplate
  public void testOptionalFieldsWriting() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table testTable =
        TestTables.create(
            tempDir("test_partition_stats_optional_" + formatVersion),
            "test_partition_stats_optional_" + formatVersion,
            SCHEMA,
            spec,
            formatVersion,
            fileFormatProperty);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatistics.schema(partitionSchema, formatVersion);

    ImmutableList.Builder<PartitionStatistics> partitionListBuilder = ImmutableList.builder();
    for (int i = 0; i < 5; i++) {
      PartitionStatistics stats =
          randomStats(dataSchema.findField(EMPTY_PARTITION_FIELD.fieldId()).type().asStructType());
      stats.set(PartitionStatistics.POSITION_DELETE_RECORD_COUNT_POSITION, null);
      stats.set(PartitionStatistics.POSITION_DELETE_FILE_COUNT_POSITION, null);
      stats.set(PartitionStatistics.EQUALITY_DELETE_RECORD_COUNT_POSITION, null);
      stats.set(PartitionStatistics.EQUALITY_DELETE_FILE_COUNT_POSITION, null);
      stats.set(PartitionStatistics.TOTAL_RECORD_COUNT_POSITION, null);
      stats.set(PartitionStatistics.LAST_UPDATED_AT_POSITION, null);
      stats.set(PartitionStatistics.LAST_UPDATED_SNAPSHOT_ID_POSITION, null);
      stats.set(PartitionStatistics.DV_COUNT_POSITION, null);

      partitionListBuilder.add(stats);
    }

    List<PartitionStatistics> expected = partitionListBuilder.build();

    assertThat(expected.get(0))
        .extracting(
            PartitionStatistics::positionDeleteRecordCount,
            PartitionStatistics::positionDeleteFileCount,
            PartitionStatistics::equalityDeleteRecordCount,
            PartitionStatistics::equalityDeleteFileCount,
            PartitionStatistics::totalRecords,
            PartitionStatistics::lastUpdatedAt,
            PartitionStatistics::lastUpdatedSnapshotId,
            PartitionStatistics::dvCount)
        .isEqualTo(
            Arrays.asList(
                0L, 0, 0L, 0, null, null, null, 0)); // null counters must be initialized to zero.

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
                testTable, snapshotId, dataSchema, expected))
        .commit();

    List<PartitionStatistics> written;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().useSnapshot(snapshotId).scan()) {
      written = Lists.newArrayList(recordIterator);
    }

    assertThat(written).hasSize(expected.size());
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < written.size(); i++) {
      assertThat(isEqual(comparator, written.get(i), expected.get(i))).isTrue();
    }
  }

  @Test
  public void testCopyOnWriteDelete() throws Exception {
    Table testTable =
        TestTables.create(tempDir("my_test"), "my_test", SCHEMA, SPEC, 2, fileFormatProperty);

    DataFile dataFile1 =
        DataFiles.builder(SPEC)
            .withPath("/df1.parquet")
            .withPartitionPath("c2=a/c3=a")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    DataFile dataFile2 =
        DataFiles.builder(SPEC)
            .withPath("/df2.parquet")
            .withPartitionPath("c2=b/c3=b")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();

    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(PartitionStatsHandler.computeAndWriteStatsFile(testTable))
        .commit();

    assertThat(testTable.newPartitionStatisticsScan().scan())
        .allMatch(s -> (s.dataRecordCount() != 0 && s.dataFileCount() != 0));

    testTable.newDelete().deleteFile(dataFile1).commit();
    testTable.newDelete().deleteFile(dataFile2).commit();

    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(PartitionStatsHandler.computeAndWriteStatsFile(testTable))
        .commit();

    // stats must be decremented to zero as all the files removed from table.
    assertThat(testTable.newPartitionStatisticsScan().scan())
        .allMatch(s -> (s.dataRecordCount() == 0 && s.dataFileCount() == 0));
  }

  @Test
  public void testLatestStatsFile() throws Exception {
    Table testTable =
        TestTables.create(tempDir("stats_file"), "stats_file", SCHEMA, SPEC, 2, fileFormatProperty);

    DataFile dataFile =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    testTable.newAppend().appendFile(dataFile).commit();

    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(
            testTable, testTable.currentSnapshot().snapshotId());
    testTable.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();

    PartitionStatisticsFile latestStatsFile =
        PartitionStatsHandler.latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);

    // another commit but without stats file
    testTable.newAppend().appendFile(dataFile).commit();
    // should point to last stats file
    latestStatsFile =
        PartitionStatsHandler.latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);

    // compute stats
    statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(
            testTable, testTable.currentSnapshot().snapshotId());
    testTable.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();
    latestStatsFile =
        PartitionStatsHandler.latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);
  }

  @Test
  public void testLatestStatsFileWithBranch() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("stats_file_branch"), "stats_file_branch", SCHEMA, SPEC, 2, fileFormatProperty);
    DataFile dataFile =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));

    /*
                                             * [statsMainB]
          ---- snapshotA  ------ snapshotMainB
                        \
                         \
                          \
                           snapshotBranchB(branch:b1)
    */

    testTable.newAppend().appendFile(dataFile).commit();
    long snapshotAId = testTable.currentSnapshot().snapshotId();

    testTable.newAppend().appendFile(dataFile).commit();
    long snapshotMainBId = testTable.currentSnapshot().snapshotId();

    String branchName = "b1";
    testTable.manageSnapshots().createBranch(branchName, snapshotAId).commit();
    testTable.newAppend().appendFile(dataFile).commit();
    long snapshotBranchBId = testTable.snapshot(branchName).snapshotId();

    PartitionStatisticsFile statsMainB =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable, snapshotMainBId);
    testTable.updatePartitionStatistics().setPartitionStatistics(statsMainB).commit();

    // should find latest stats for snapshotMainB
    assertThat(PartitionStatsHandler.latestStatsFile(testTable, snapshotMainBId))
        .isEqualTo(statsMainB);

    // should not find latest stats for snapshotBranchB
    assertThat(PartitionStatsHandler.latestStatsFile(testTable, snapshotBranchBId)).isNull();
  }

  @Test
  public void testFullComputeFallbackWithInvalidStats() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table testTable =
        TestTables.create(
            tempDir("invalid_schema"), "invalid_schema", SCHEMA, spec, 2, fileFormatProperty);
    DataFile dataFile = FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of(42));
    testTable.newAppend().appendFile(dataFile).commit();

    Types.StructType partitionType = Partitioning.partitionType(testTable);

    PartitionStatisticsFile invalidStatisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(
            testTable,
            testTable.currentSnapshot().snapshotId(),
            invalidOldSchema(partitionType),
            Collections.singletonList(randomStats(partitionType)));
    testTable.updatePartitionStatistics().setPartitionStatistics(invalidStatisticsFile).commit();

    testTable.newAppend().appendFile(dataFile).commit();
    testTable
        .updatePartitionStatistics()
        .setPartitionStatistics(PartitionStatsHandler.computeAndWriteStatsFile(testTable))
        .commit();

    // read the partition entries from the stats file
    List<PartitionStatistics> partitionStats;
    try (CloseableIterable<PartitionStatistics> recordIterator =
        testTable.newPartitionStatisticsScan().scan()) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats).hasSize(1);
    // should include stats from both the appends.
    assertThat(partitionStats.get(0).dataFileCount()).isEqualTo(2);
  }

  @Test
  public void testAppendWithAllValues() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, 15L, 1625077800000L, 12345L);
    BasePartitionStatistics stats2 =
        createStats(200L, 7, 500L, 1L, 100, 0L, 50, 7L, 1625077900000L, 12346L);

    PartitionStatsHandler.appendStats(stats1, stats2);

    validateStats(stats1, 300L, 22, 1500L, 3L, 600, 1L, 250, 22L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendWithThisNullOptionalField() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, null, null, null);
    BasePartitionStatistics stats2 =
        createStats(100L, 7, 500L, 1L, 100, 0L, 50, 7L, 1625077900000L, 12346L);

    PartitionStatsHandler.appendStats(stats1, stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, 7L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendWithBothNullOptionalFields() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, null, null, null);
    BasePartitionStatistics stats2 = createStats(100L, 7, 500L, 1L, 100, 0L, 50, null, null, null);

    PartitionStatsHandler.appendStats(stats1, stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, null, null, null);
  }

  @Test
  public void testAppendWithOtherNullOptionalFields() {
    BasePartitionStatistics stats1 =
        createStats(100L, 15, 1000L, 2L, 500, 1L, 200, 15L, 1625077900000L, 12346L);
    BasePartitionStatistics stats2 = createStats(100L, 7, 500L, 1L, 100, 0L, 50, null, null, null);

    PartitionStatsHandler.appendStats(stats1, stats2);

    validateStats(stats1, 200L, 22, 1500L, 3L, 600, 1L, 250, 15L, 1625077900000L, 12346L);
  }

  @Test
  public void testAppendEmptyStats() {
    BasePartitionStatistics stats1 = new BasePartitionStatistics(PARTITION, 1);
    BasePartitionStatistics stats2 = new BasePartitionStatistics(PARTITION, 1);

    PartitionStatsHandler.appendStats(stats1, stats2);

    validateStats(stats1, 0L, 0, 0L, 0L, 0, 0L, 0, null, null, null);
  }

  @Test
  public void testAppendWithDifferentSpec() {
    BasePartitionStatistics stats1 = new BasePartitionStatistics(PARTITION, 1);
    BasePartitionStatistics stats2 = new BasePartitionStatistics(PARTITION, 2);

    assertThatThrownBy(() -> PartitionStatsHandler.appendStats(stats1, stats2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Spec IDs must match");
  }

  private BasePartitionStatistics createStats(
      long dataRecordCount,
      int dataFileCount,
      long totalDataFileSizeInBytes,
      long positionDeleteRecordCount,
      int positionDeleteFileCount,
      long equalityDeleteRecordCount,
      int equalityDeleteFileCount,
      Long totalRecordCount,
      Long lastUpdatedAt,
      Long lastUpdatedSnapshotId) {

    BasePartitionStatistics stats = new BasePartitionStatistics(PARTITION, 1);
    stats.set(2, dataRecordCount);
    stats.set(3, dataFileCount);
    stats.set(4, totalDataFileSizeInBytes);
    stats.set(5, positionDeleteRecordCount);
    stats.set(6, positionDeleteFileCount);
    stats.set(7, equalityDeleteRecordCount);
    stats.set(8, equalityDeleteFileCount);
    stats.set(9, totalRecordCount);
    stats.set(10, lastUpdatedAt);
    stats.set(11, lastUpdatedSnapshotId);

    return stats;
  }

  private void validateStats(PartitionStatistics stats, Object... expectedValues) {
    // Spec id and partition data should be unchanged
    assertThat(stats.get(0, PartitionData.class)).isEqualTo(PARTITION);
    assertThat(stats.get(1, Integer.class)).isEqualTo(1);

    for (int i = 0; i < expectedValues.length; i++) {
      if (expectedValues[i] == null) {
        assertThat(stats.get(i + 2, Object.class)).isNull();
      } else {
        assertThat(stats.get(i + 2, Object.class)).isEqualTo(expectedValues[i]);
      }
    }
  }
}
