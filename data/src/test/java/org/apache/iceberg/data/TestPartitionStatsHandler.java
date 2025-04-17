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
package org.apache.iceberg.data;

import static org.apache.iceberg.data.PartitionStatsHandler.DATA_FILE_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.DATA_RECORD_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.EQUALITY_DELETE_FILE_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.EQUALITY_DELETE_RECORD_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.LAST_UPDATED_AT;
import static org.apache.iceberg.data.PartitionStatsHandler.LAST_UPDATED_SNAPSHOT_ID;
import static org.apache.iceberg.data.PartitionStatsHandler.PARTITION_FIELD_ID;
import static org.apache.iceberg.data.PartitionStatsHandler.POSITION_DELETE_FILE_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.POSITION_DELETE_RECORD_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.TOTAL_DATA_FILE_SIZE_IN_BYTES;
import static org.apache.iceberg.data.PartitionStatsHandler.TOTAL_RECORD_COUNT;
import static org.apache.iceberg.data.PartitionStatsHandler.latestStatsFile;
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
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assumptions;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestPartitionStatsHandler {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @TempDir public File temp;

  private static final Random RANDOM = ThreadLocalRandom.current();

  @Parameters(name = "fileFormat = {0}")
  public static List<Object> parameters() {
    return Arrays.asList(FileFormat.PARQUET, FileFormat.ORC, FileFormat.AVRO);
  }

  @Parameter private FileFormat format;

  @Test
  public void testPartitionStatsOnEmptyTable() throws Exception {
    Table testTable = TestTables.create(tempDir("empty_table"), "empty_table", SCHEMA, SPEC, 2);
    assertThat(PartitionStatsHandler.computeAndWriteStatsFile(testTable)).isNull();
  }

  @Test
  public void testPartitionStatsOnEmptyBranch() throws Exception {
    Table testTable = TestTables.create(tempDir("empty_branch"), "empty_branch", SCHEMA, SPEC, 2);
    testTable.manageSnapshots().createBranch("b1").commit();
    long branchSnapshot = testTable.refs().get("b1").snapshotId();
    assertThat(PartitionStatsHandler.computeAndWriteStatsFile(testTable, branchSnapshot)).isNull();
  }

  @Test
  public void testPartitionStatsOnInvalidSnapshot() throws Exception {
    Table testTable =
        TestTables.create(tempDir("invalid_snapshot"), "invalid_snapshot", SCHEMA, SPEC, 2);
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
            2);

    List<Record> records = prepareRecords(testTable.schema());
    DataFile dataFile = FileHelpers.writeDataFile(testTable, outputFile(), records);
    testTable.newAppend().appendFile(dataFile).commit();

    assertThatThrownBy(() -> PartitionStatsHandler.computeAndWriteStatsFile(testTable))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table must be partitioned");
  }

  @Test
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
            tempDir("test_all_type"), "test_all_type", schema, spec, SortOrder.unsorted(), 2);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatsHandler.schema(partitionSchema);

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
    partitionStats.set(DATA_RECORD_COUNT.fieldId(), RANDOM.nextLong());
    partitionStats.set(DATA_FILE_COUNT.fieldId(), RANDOM.nextInt());
    partitionStats.set(TOTAL_DATA_FILE_SIZE_IN_BYTES.fieldId(), 1024L * RANDOM.nextInt(20));
    List<PartitionStats> expected = Collections.singletonList(partitionStats);
    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(testTable, 42L, dataSchema, expected);

    List<PartitionStats> written;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            dataSchema, Files.localInput(statisticsFile.path()))) {
      written = Lists.newArrayList(recordIterator);
    }

    assertThat(written).hasSize(expected.size());
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < written.size(); i++) {
      assertThat(isEqual(comparator, written.get(i), expected.get(i))).isTrue();
    }
  }

  @Test
  public void testOptionalFieldsWriting() throws Exception {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("c1").build();
    Table testTable =
        TestTables.create(
            tempDir("test_partition_stats_optional"),
            "test_partition_stats_optional",
            SCHEMA,
            spec,
            SortOrder.unsorted(),
            2);

    Types.StructType partitionSchema = Partitioning.partitionType(testTable);
    Schema dataSchema = PartitionStatsHandler.schema(partitionSchema);

    ImmutableList.Builder<PartitionStats> partitionListBuilder = ImmutableList.builder();
    for (int i = 0; i < 5; i++) {
      PartitionData partitionData =
          new PartitionData(dataSchema.findField(PARTITION_FIELD_ID).type().asStructType());
      partitionData.set(0, RANDOM.nextInt());

      PartitionStats stats = new PartitionStats(partitionData, RANDOM.nextInt(10));
      stats.set(PARTITION_FIELD_ID, partitionData);
      stats.set(DATA_RECORD_COUNT.fieldId(), RANDOM.nextLong());
      stats.set(DATA_FILE_COUNT.fieldId(), RANDOM.nextInt());
      stats.set(TOTAL_DATA_FILE_SIZE_IN_BYTES.fieldId(), 1024L * RANDOM.nextInt(20));
      stats.set(POSITION_DELETE_RECORD_COUNT.fieldId(), null);
      stats.set(POSITION_DELETE_FILE_COUNT.fieldId(), null);
      stats.set(EQUALITY_DELETE_RECORD_COUNT.fieldId(), null);
      stats.set(EQUALITY_DELETE_FILE_COUNT.fieldId(), null);
      stats.set(TOTAL_RECORD_COUNT.fieldId(), null);
      stats.set(LAST_UPDATED_AT.fieldId(), null);
      stats.set(LAST_UPDATED_SNAPSHOT_ID.fieldId(), null);

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
            PartitionStats::lastUpdatedSnapshotId)
        .isEqualTo(
            Arrays.asList(
                0L, 0, 0L, 0, null, null, null)); // null counters must be initialized to zero.

    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.writePartitionStatsFile(testTable, 42L, dataSchema, expected);

    List<PartitionStats> written;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            dataSchema, Files.localInput(statisticsFile.path()))) {
      written = Lists.newArrayList(recordIterator);
    }

    assertThat(written).hasSize(expected.size());
    Comparator<StructLike> comparator = Comparators.forType(partitionSchema);
    for (int i = 0; i < written.size(); i++) {
      assertThat(isEqual(comparator, written.get(i), expected.get(i))).isTrue();
    }
  }

  @TestTemplate // Tests for all the table formats (PARQUET, ORC, AVRO)
  public void testPartitionStats() throws Exception {
    Assumptions.assumeThat(format)
        .as("ORC internal readers and writers are not supported")
        .isNotEqualTo(FileFormat.ORC);

    Table testTable =
        TestTables.create(
            tempDir("partition_stats_" + format.name()),
            "partition_stats_compute_" + format.name(),
            SCHEMA,
            SPEC,
            2,
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, format.name()));

    List<Record> records = prepareRecords(testTable.schema());
    List<DataFile> dataFiles = dataFiles(testTable, records);

    Snapshot snapshot1 = appendMultipleAndValidate(testTable, dataFiles);

    commitDeletesAndValidate(testTable, dataFiles, snapshot1);
  }

  private static Snapshot appendMultipleAndValidate(Table testTable, List<DataFile> dataFiles)
      throws IOException {
    for (int i = 0; i < 3; i++) {
      // insert same set of seven records thrice to have a new manifest files
      AppendFiles appendFiles = testTable.newAppend();
      dataFiles.forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    Snapshot snapshot = testTable.currentSnapshot();

    Schema recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema.findField(PARTITION_FIELD_ID).type().asStructType();
    computeAndValidatePartitionStats(
        false, // incremental
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            9L,
            3,
            3 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            3L,
            3,
            3 * dataFiles.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3L,
            3,
            3 * dataFiles.get(2).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            6L,
            3,
            3 * dataFiles.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()));

    return snapshot;
  }

  private void commitDeletesAndValidate(
      Table testTable, List<DataFile> dataFiles, Snapshot snapshot1) throws IOException {
    DeleteFile posDeletes = commitPositionDeletes(testTable, dataFiles.get(0));
    Snapshot snapshot2 = testTable.currentSnapshot();

    DeleteFile eqDeletes = commitEqualityDeletes(testTable);
    Snapshot snapshot3 = testTable.currentSnapshot();

    Schema recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema.findField(PARTITION_FIELD_ID).type().asStructType();
    computeAndValidatePartitionStats(
        false, // incremental
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            9L,
            3,
            3 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            eqDeletes.recordCount(),
            1,
            null,
            snapshot3.timestampMillis(),
            snapshot3.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            3L,
            3,
            3 * dataFiles.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3L,
            3,
            3 * dataFiles.get(2).fileSizeInBytes(),
            posDeletes.recordCount(),
            1,
            0L,
            0,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            6L,
            3,
            3 * dataFiles.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));
  }

  @Test
  @SuppressWarnings("MethodLength")
  public void testPartitionStatsIncremental() throws Exception {
    Table testTable =
        TestTables.create(
            tempDir("partition_stats_incremental"), "partition_stats_incremental", SCHEMA, SPEC, 2);

    List<Record> records = prepareRecords(testTable.schema());
    List<DataFile> dataFiles = dataFiles(testTable, records);

    Snapshot snapshot1 = appendMultipleAndValidate(testTable, dataFiles);

    commitDeletesAndValidate(testTable, dataFiles, snapshot1);

    AppendFiles appendFiles = testTable.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();

    Snapshot snapshot = testTable.currentSnapshot();

    Schema recordSchema = PartitionStatsHandler.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema.findField(PARTITION_FIELD_ID).type().asStructType();
    computeAndValidatePartitionStats(
        true, // incremental
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            12L,
            4,
            4 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            2L,
            1,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            4L,
            4,
            4 * dataFiles.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            4L,
            4,
            4 * dataFiles.get(2).fileSizeInBytes(),
            2L,
            1,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            8L,
            4,
            4 * dataFiles.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()));

    // test schema evolution with incremental compute
    testTable.updateSpec().addField("c1").commit();
    Types.StructType newPartitionType = Partitioning.partitionType(testTable);
    // append one record of "foo,A,0"
    DataFile newFile =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "A", 0), records.subList(0, 1));
    testTable.newAppend().appendFile(newFile).commit();
    Snapshot latestSnapshot = testTable.currentSnapshot();
    computeAndValidatePartitionStats(
        true, // incremental
        testTable,
        PartitionStatsHandler.schema(newPartitionType),
        Tuple.tuple(
            partitionRecord(newPartitionType, "foo", "A", 0),
            1, // new spec id
            1L,
            1,
            newFile.fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            latestSnapshot.timestampMillis(),
            latestSnapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(newPartitionType, "foo", "A", null),
            0,
            12L,
            4,
            4 * dataFiles.get(0).fileSizeInBytes(),
            0L,
            0,
            2L,
            1,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(newPartitionType, "foo", "B", null),
            0,
            4L,
            4,
            4 * dataFiles.get(1).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(newPartitionType, "bar", "A", null),
            0,
            4L,
            4,
            4 * dataFiles.get(2).fileSizeInBytes(),
            2L,
            1,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()),
        Tuple.tuple(
            partitionRecord(newPartitionType, "bar", "B", null),
            0,
            8L,
            4,
            4 * dataFiles.get(3).fileSizeInBytes(),
            0L,
            0,
            0L,
            0,
            null,
            snapshot.timestampMillis(),
            snapshot.snapshotId()));
  }

  @Test
  public void testIncrementalComputeWithoutPreviousStats() throws Exception {
    Table testTable =
        TestTables.create(tempDir("incremental_error"), "incremental_error", SCHEMA, SPEC, 2);

    List<DataFile> dataFiles = dataFiles(testTable, prepareRecords(testTable.schema()));
    AppendFiles appendFiles = testTable.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();

    assertThatThrownBy(
            () ->
                PartitionStatsHandler.computeAndWriteStatsFileIncremental(
                    testTable, testTable.currentSnapshot().snapshotId()))
        .isInstanceOf(RuntimeException.class)
        .hasMessage("Previous stats not found for incremental compute. Try full compute");
  }

  @Test
  public void testLatestStatsFile() throws Exception {
    Table testTable = TestTables.create(tempDir("stats_file"), "stats_file", SCHEMA, SPEC, 2);

    List<DataFile> dataFiles = dataFiles(testTable, prepareRecords(testTable.schema()));
    AppendFiles appendFiles = testTable.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();

    PartitionStatisticsFile statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(
            testTable, testTable.currentSnapshot().snapshotId());
    testTable.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();

    PartitionStatisticsFile latestStatsFile =
        latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);

    // another commit but without stats file
    appendFiles = testTable.newAppend();
    dataFiles.forEach(appendFiles::appendFile);
    appendFiles.commit();
    // should point to last stats file
    latestStatsFile = latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);

    // compute stats
    statisticsFile =
        PartitionStatsHandler.computeAndWriteStatsFile(
            testTable, testTable.currentSnapshot().snapshotId());
    testTable.updatePartitionStatistics().setPartitionStatistics(statisticsFile).commit();
    latestStatsFile = latestStatsFile(testTable, testTable.currentSnapshot().snapshotId());
    assertThat(latestStatsFile).isEqualTo(statisticsFile);
  }

  private OutputFile outputFile() throws IOException {
    return Files.localOutput(File.createTempFile("data", null, tempDir("stats")));
  }

  private static StructLike partitionRecord(Types.StructType partitionType, Object... fields) {
    GenericRecord record = GenericRecord.create(partitionType);
    for (int i = 0; i < fields.length; i++) {
      record.set(i, fields[i]);
    }

    return record;
  }

  private static List<Record> prepareRecords(Schema schema) {
    GenericRecord record = GenericRecord.create(schema);
    List<Record> records = Lists.newArrayList();
    // foo 4 records, bar 3 records
    // foo, A -> 3 records
    records.add(record.copy("c1", 0, "c2", "foo", "c3", "A"));
    records.add(record.copy("c1", 1, "c2", "foo", "c3", "A"));
    records.add(record.copy("c1", 2, "c2", "foo", "c3", "A"));
    // foo, B -> 1 record
    records.add(record.copy("c1", 3, "c2", "foo", "c3", "B"));
    // bar, A -> 1 record
    records.add(record.copy("c1", 4, "c2", "bar", "c3", "A"));
    // bar, B -> 2 records
    records.add(record.copy("c1", 5, "c2", "bar", "c3", "B"));
    records.add(record.copy("c1", 6, "c2", "bar", "c3", "B"));
    return records;
  }

  private List<DataFile> dataFiles(Table testTable, List<Record> records) throws IOException {
    List<DataFile> files = Lists.newArrayList();
    files.add(
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "A"), records.subList(0, 3)));
    files.add(
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "B"), records.subList(3, 4)));
    files.add(
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "A"), records.subList(4, 5)));
    files.add(
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "B"), records.subList(5, 7)));
    return files;
  }

  private static void computeAndValidatePartitionStats(
      boolean incremental, Table testTable, Schema recordSchema, Tuple... expectedValues)
      throws IOException {
    // compute and commit partition stats file
    Snapshot currentSnapshot = testTable.currentSnapshot();
    PartitionStatisticsFile result;
    if (incremental) {
      result =
          PartitionStatsHandler.computeAndWriteStatsFileIncremental(
              testTable, testTable.currentSnapshot().snapshotId());
    } else {
      result = PartitionStatsHandler.computeAndWriteStatsFile(testTable);
    }

    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();
    assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // read the partition entries from the stats file
    List<PartitionStats> partitionStats;
    try (CloseableIterable<PartitionStats> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            recordSchema, Files.localInput(result.path()))) {
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
            PartitionStats::lastUpdatedSnapshotId)
        .containsExactlyInAnyOrder(expectedValues);
  }

  private DeleteFile commitEqualityDeletes(Table testTable) throws IOException {
    Schema deleteRowSchema = testTable.schema().select("c1");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(dataDelete.copy("c1", 1), dataDelete.copy("c1", 2));

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            testTable,
            Files.localOutput(File.createTempFile("junit", null, tempDir("eq_delete"))),
            TestHelpers.Row.of("foo", "A"),
            dataDeletes,
            deleteRowSchema);
    testTable.newRowDelta().addDeletes(eqDeletes).commit();
    return eqDeletes;
  }

  private DeleteFile commitPositionDeletes(Table testTable, DataFile dataFile1) throws IOException {
    List<PositionDelete<?>> deletes = Lists.newArrayList();
    for (long i = 0; i < 2; i++) {
      deletes.add(
          positionDelete(testTable.schema(), dataFile1.location(), i, (int) i, String.valueOf(i)));
    }

    DeleteFile posDeletes =
        FileHelpers.writePosDeleteFile(
            testTable,
            Files.localOutput(File.createTempFile("junit", null, tempDir("pos_delete"))),
            TestHelpers.Row.of("bar", "A"),
            deletes);
    testTable.newRowDelta().addDeletes(posDeletes).commit();
    return posDeletes;
  }

  private static PositionDelete<GenericRecord> positionDelete(
      Schema tableSchema, CharSequence path, Long position, Object... values) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(tableSchema);
    for (int i = 0; i < values.length; i++) {
      nested.set(i, values[i]);
    }

    posDelete.set(path, position, nested);
    return posDelete;
  }

  private File tempDir(String folderName) throws IOException {
    return java.nio.file.Files.createTempDirectory(temp.toPath(), folderName).toFile();
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
