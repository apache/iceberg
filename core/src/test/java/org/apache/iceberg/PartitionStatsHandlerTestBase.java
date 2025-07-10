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

import static org.apache.iceberg.PartitionStatsHandler.DATA_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.DATA_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.EQUALITY_DELETE_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.EQUALITY_DELETE_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.LAST_UPDATED_AT;
import static org.apache.iceberg.PartitionStatsHandler.LAST_UPDATED_SNAPSHOT_ID;
import static org.apache.iceberg.PartitionStatsHandler.PARTITION_FIELD_ID;
import static org.apache.iceberg.PartitionStatsHandler.PARTITION_FIELD_NAME;
import static org.apache.iceberg.PartitionStatsHandler.POSITION_DELETE_FILE_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.POSITION_DELETE_RECORD_COUNT;
import static org.apache.iceberg.PartitionStatsHandler.SPEC_ID;
import static org.apache.iceberg.PartitionStatsHandler.TOTAL_DATA_FILE_SIZE_IN_BYTES;
import static org.apache.iceberg.PartitionStatsHandler.TOTAL_RECORD_COUNT;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class PartitionStatsHandlerTestBase {

  public abstract FileFormat format();

  @Parameters(name = "formatVersion = {0}")
  protected static List<Integer> formatVersions() {
    return TestHelpers.V2_AND_ABOVE;
  }

  @Parameter protected int formatVersion;

  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @TempDir public File temp;

  private static final Random RANDOM = ThreadLocalRandom.current();

  private final Map<String, String> fileFormatProperty =
      ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format().name());

  // position in StructLike
  private static final int DATA_RECORD_COUNT_POSITION = 2;
  private static final int DATA_FILE_COUNT_POSITION = 3;
  private static final int TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION = 4;
  private static final int POSITION_DELETE_RECORD_COUNT_POSITION = 5;
  private static final int POSITION_DELETE_FILE_COUNT_POSITION = 6;
  private static final int EQUALITY_DELETE_RECORD_COUNT_POSITION = 7;
  private static final int EQUALITY_DELETE_FILE_COUNT_POSITION = 8;
  private static final int TOTAL_RECORD_COUNT_POSITION = 9;
  private static final int LAST_UPDATED_AT_POSITION = 10;
  private static final int LAST_UPDATED_SNAPSHOT_ID_POSITION = 11;
  private static final int DV_COUNT_POSITION = 12;

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
    Schema dataSchema = PartitionStatsHandler.schema(partitionSchema, formatVersion);

    PartitionData partitionData =
        new PartitionData(dataSchema.findField(PARTITION_FIELD_ID).type().asStructType());
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

    PartitionStats partitionStats = new PartitionStats(partitionData, RANDOM.nextInt(10));
    partitionStats.set(DATA_RECORD_COUNT_POSITION, RANDOM.nextLong());
    partitionStats.set(DATA_FILE_COUNT_POSITION, RANDOM.nextInt());
    partitionStats.set(TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 1024L * RANDOM.nextInt(20));
    List<PartitionStats> expected = Collections.singletonList(partitionStats);
    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(testTable, 42L, dataSchema, expected);

    List<PartitionStats> written;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            dataSchema, testTable.io().newInputFile(statisticsFile.path()))) {
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
    Schema dataSchema = PartitionStatsHandler.schema(partitionSchema, formatVersion);

    ImmutableList.Builder<PartitionStats> partitionListBuilder = ImmutableList.builder();
    for (int i = 0; i < 5; i++) {
      PartitionData partitionData =
          new PartitionData(dataSchema.findField(PARTITION_FIELD_ID).type().asStructType());
      partitionData.set(0, RANDOM.nextInt());

      PartitionStats stats = new PartitionStats(partitionData, RANDOM.nextInt(10));
      stats.set(DATA_RECORD_COUNT_POSITION, RANDOM.nextLong());
      stats.set(DATA_FILE_COUNT_POSITION, RANDOM.nextInt());
      stats.set(TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 1024L * RANDOM.nextInt(20));
      stats.set(POSITION_DELETE_RECORD_COUNT_POSITION, null);
      stats.set(POSITION_DELETE_FILE_COUNT_POSITION, null);
      stats.set(EQUALITY_DELETE_RECORD_COUNT_POSITION, null);
      stats.set(EQUALITY_DELETE_FILE_COUNT_POSITION, null);
      stats.set(TOTAL_RECORD_COUNT_POSITION, null);
      stats.set(LAST_UPDATED_AT_POSITION, null);
      stats.set(LAST_UPDATED_SNAPSHOT_ID_POSITION, null);
      stats.set(DV_COUNT_POSITION, null);

      partitionListBuilder.add(stats);
    }

    List<PartitionStats> expected = partitionListBuilder.build();

    assertThat(expected.get(0))
        .extracting(
            PartitionStats::positionDeleteRecordCount,
            PartitionStats::positionDeleteFileCount,
            PartitionStats::equalityDeleteRecordCount,
            PartitionStats::equalityDeleteFileCount,
            PartitionStats::totalRecords,
            PartitionStats::lastUpdatedAt,
            PartitionStats::lastUpdatedSnapshotId,
            PartitionStats::dvCount)
        .isEqualTo(
            Arrays.asList(
                0L, 0, 0L, 0, null, null, null, 0)); // null counters must be initialized to zero.

    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(testTable, 42L, dataSchema, expected);

    List<PartitionStats> written;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            dataSchema, testTable.io().newInputFile(statisticsFile.path()))) {
      written = Lists.newArrayList(recordIterator);
    }

    assertThat(written).hasSize(expected.size());
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < written.size(); i++) {
      assertThat(isEqual(comparator, written.get(i), expected.get(i))).isTrue();
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void testPartitionStats() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("partition_stats_compute"),
            "partition_stats_compute",
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
        recordSchema,
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
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId(),
            0),
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

    recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable), 3);

    computeAndValidatePartitionStats(
        testTable,
        recordSchema,
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

    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable);
    testTable.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();

    assertThat(
            PartitionStatsHandler.readPartitionStatsFile(
                PartitionStatsHandler.schema(Partitioning.partitionType(testTable), 2),
                testTable.io().newInputFile(statisticsFile.path())))
        .allMatch(s -> (s.dataRecordCount() != 0 && s.dataFileCount() != 0));

    testTable.newDelete().deleteFile(dataFile1).commit();
    testTable.newDelete().deleteFile(dataFile2).commit();

    PartitionStatisticsFile statisticsFileNew =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable);

    // stats must be decremented to zero as all the files removed from table.
    assertThat(
            PartitionStatsHandler.readPartitionStatsFile(
                PartitionStatsHandler.schema(Partitioning.partitionType(testTable), 2),
                testTable.io().newInputFile(statisticsFileNew.path())))
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
  public void testReadingStatsWithInvalidSchema() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table testTable =
        TestTables.create(tempDir("old_schema"), "old_schema", SCHEMA, spec, 2, fileFormatProperty);
    Types.StructType partitionType = Partitioning.partitionType(testTable);
    Schema newSchema = PartitionStatsHandler.schema(partitionType);
    Schema oldSchema = invalidOldSchema(partitionType);

    PartitionStatisticsFile invalidStatisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(
            testTable, 42L, oldSchema, Collections.singletonList(randomStats(partitionType)));

    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            newSchema, testTable.io().newInputFile(invalidStatisticsFile.path()))) {

      if (format() == FileFormat.PARQUET) {
        assertThatThrownBy(() -> Lists.newArrayList(recordIterator))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("Not a primitive type: struct");
      } else if (format() == FileFormat.AVRO) {
        assertThatThrownBy(() -> Lists.newArrayList(recordIterator))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("Not an instance of org.apache.iceberg.StructLike");
      }
    }
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
    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(testTable);

    // read the partition entries from the stats file
    List<PartitionStats> partitionStats;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            PartitionStatsHandler.schema(partitionType),
            testTable.io().newInputFile(statisticsFile.path()))) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats).hasSize(1);
    // should include stats from both the appends.
    assertThat(partitionStats.get(0).dataFileCount()).isEqualTo(2);
  }

  @Test
  public void testV2toV3SchemaEvolution() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("schema_evolution"), "schema_evolution", SCHEMA, SPEC, 2, fileFormatProperty);

    // write stats file using v2 schema
    DataFile dataFile =
        FileGenerationUtil.generateDataFile(testTable, TestHelpers.Row.of("foo", "A"));
    testTable.newAppend().appendFile(dataFile).commit();
    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(
            testTable, testTable.currentSnapshot().snapshotId());

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);

    // read with v2 schema
    Schema v2Schema = PartitionStatsHandler.schema(partitionSchema, 2);
    List<PartitionStats> partitionStatsV2;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            v2Schema, testTable.io().newInputFile(statisticsFile.path()))) {
      partitionStatsV2 = Lists.newArrayList(recordIterator);
    }

    // read with v3 schema
    Schema v3Schema = PartitionStatsHandler.schema(partitionSchema, 3);
    List<PartitionStats> partitionStatsV3;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            v3Schema, testTable.io().newInputFile(statisticsFile.path()))) {
      partitionStatsV3 = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStatsV2).hasSameSizeAs(partitionStatsV3);
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < partitionStatsV2.size(); i++) {
      assertThat(isEqual(comparator, partitionStatsV2.get(i), partitionStatsV3.get(i))).isTrue();
    }
  }

  private static StructLike partitionRecord(
      Types.StructType partitionType, String val1, String val2) {
    GenericRecord record = GenericRecord.create(partitionType);
    record.set(0, val1);
    record.set(1, val2);
    return record;
  }

  private static void computeAndValidatePartitionStats(
      Table testTable, Schema recordSchema, Tuple... expectedValues) throws IOException {
    // compute and commit partition stats file
    Snapshot currentSnapshot = testTable.currentSnapshot();
    PartitionStatisticsFile result = PartitionStatsHandler.computeAndWriteStatsFile(testTable);
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();
    assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // read the partition entries from the stats file
    List<PartitionStats> partitionStats;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            recordSchema, testTable.io().newInputFile(result.path()))) {
      partitionStats = Lists.newArrayList(recordIterator);
    }

    assertThat(partitionStats)
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
            PartitionStats::totalRecords,
            PartitionStats::lastUpdatedAt,
            PartitionStats::lastUpdatedSnapshotId,
            PartitionStats::dvCount)
        .containsExactlyInAnyOrder(expectedValues);
  }

  private File tempDir(String folderName) throws IOException {
    return java.nio.file.Files.createTempDirectory(temp.toPath(), folderName).toFile();
  }

  private Schema invalidOldSchema(Types.StructType unifiedPartitionType) {
    // field ids starts from 0 instead of 1
    return new Schema(
        Types.NestedField.required(0, PARTITION_FIELD_NAME, unifiedPartitionType),
        Types.NestedField.required(1, SPEC_ID.name(), Types.IntegerType.get()),
        Types.NestedField.required(2, DATA_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.required(3, DATA_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.required(4, TOTAL_DATA_FILE_SIZE_IN_BYTES.name(), Types.LongType.get()),
        Types.NestedField.optional(5, POSITION_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(6, POSITION_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(7, EQUALITY_DELETE_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(8, EQUALITY_DELETE_FILE_COUNT.name(), Types.IntegerType.get()),
        Types.NestedField.optional(9, TOTAL_RECORD_COUNT.name(), Types.LongType.get()),
        Types.NestedField.optional(10, LAST_UPDATED_AT.name(), Types.LongType.get()),
        Types.NestedField.optional(11, LAST_UPDATED_SNAPSHOT_ID.name(), Types.LongType.get()));
  }

  private PartitionStats randomStats(Types.StructType partitionType) {
    PartitionData partitionData = new PartitionData(partitionType);
    partitionData.set(0, RANDOM.nextInt());

    PartitionStats stats = new PartitionStats(partitionData, RANDOM.nextInt(10));
    stats.set(DATA_RECORD_COUNT_POSITION, RANDOM.nextLong());
    stats.set(DATA_FILE_COUNT_POSITION, RANDOM.nextInt());
    stats.set(TOTAL_DATA_FILE_SIZE_IN_BYTES_POSITION, 1024L * RANDOM.nextInt(20));
    return stats;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static boolean isEqual(
      Comparator<StructLike> partitionComparator, PartitionStats stats1, PartitionStats stats2) {
    if (stats1 == stats2) {
      return true;
    } else if (stats1 == null || stats2 == null) {
      return false;
    }

    return partitionComparator.compare(stats1.partition(), stats2.partition()) == 0
        && stats1.specId() == stats2.specId()
        && stats1.dataRecordCount() == stats2.dataRecordCount()
        && stats1.dataFileCount() == stats2.dataFileCount()
        && stats1.totalDataFileSizeInBytes() == stats2.totalDataFileSizeInBytes()
        && stats1.positionDeleteRecordCount() == stats2.positionDeleteRecordCount()
        && stats1.positionDeleteFileCount() == stats2.positionDeleteFileCount()
        && stats1.equalityDeleteRecordCount() == stats2.equalityDeleteRecordCount()
        && stats1.equalityDeleteFileCount() == stats2.equalityDeleteFileCount()
        && Objects.equals(stats1.totalRecords(), stats2.totalRecords())
        && Objects.equals(stats1.lastUpdatedAt(), stats2.lastUpdatedAt())
        && Objects.equals(stats1.lastUpdatedSnapshotId(), stats2.lastUpdatedSnapshotId());
  }
}
