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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPartitionStatsGenerator {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testPartitionStatsOnEmptyTable() throws Exception {
    Table testTable =
        TestTables.create(
            temp.newFolder("empty_table"), "empty_table", SCHEMA, SPEC, SortOrder.unsorted(), 2);

    PartitionStatsGenerator partitionStatsGenerator = new PartitionStatsGenerator(testTable);
    assertThat(partitionStatsGenerator.generate()).isNull();
  }

  @Test
  public void testPartitionStatsOnEmptyBranch() throws Exception {
    Table testTable =
        TestTables.create(
            temp.newFolder("empty_branch"), "empty_branch", SCHEMA, SPEC, SortOrder.unsorted(), 2);

    testTable.manageSnapshots().createBranch("b1").commit();

    PartitionStatsGenerator partitionStatsGenerator = new PartitionStatsGenerator(testTable, "b1");
    // creates an empty stats file since the dummy snapshot exist
    assertThat(partitionStatsGenerator.generate())
        .extracting(PartitionStatisticsFile::fileSizeInBytes)
        .isEqualTo(0L);
    assertThat(partitionStatsGenerator.generate())
        .extracting(PartitionStatisticsFile::snapshotId)
        .isEqualTo(testTable.refs().get("b1").snapshotId());
  }

  @Test
  public void testPartitionStatsOnInvalidSnapshot() throws Exception {
    Table testTable =
        TestTables.create(
            temp.newFolder("invalid_snapshot"),
            "invalid_snapshot",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            2);

    PartitionStatsGenerator partitionStatsGenerator =
        new PartitionStatsGenerator(testTable, "INVALID_BRANCH");
    assertThatThrownBy(partitionStatsGenerator::generate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Couldn't find the snapshot for the branch INVALID_BRANCH");
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void testPartitionStats() throws Exception {
    Table testTable =
        TestTables.create(
            temp.newFolder("partition_stats"),
            "partition_stats_compute",
            SCHEMA,
            SPEC,
            SortOrder.unsorted(),
            2);

    List<Record> records = prepareRecords(testTable.schema());
    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "A"), records.subList(0, 3));
    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "B"), records.subList(3, 4));
    DataFile dataFile3 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "A"), records.subList(4, 5));
    DataFile dataFile4 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "B"), records.subList(5, 7));

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
    Schema recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    computeAndValidatePartitionStats(
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            9L,
            3,
            3 * dataFile1.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            0,
            3L,
            3,
            3 * dataFile2.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3L,
            3,
            3 * dataFile3.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            6L,
            3,
            3 * dataFile4.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));

    DeleteFile posDeletes = commitPositionDeletes(testTable, dataFile1);
    Snapshot snapshot2 = testTable.currentSnapshot();

    DeleteFile eqDeletes = commitEqualityDeletes(testTable);
    Snapshot snapshot3 = testTable.currentSnapshot();

    recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    computeAndValidatePartitionStats(
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            0,
            9L,
            3,
            3 * dataFile1.fileSizeInBytes(),
            null,
            null,
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
            3 * dataFile2.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            0,
            3L,
            3,
            3 * dataFile3.fileSizeInBytes(),
            posDeletes.recordCount(),
            1,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            0,
            6L,
            3,
            3 * dataFile4.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  public void testPartitionStatsWithSchemaEvolution() throws Exception {
    final PartitionSpec specBefore = PartitionSpec.builderFor(SCHEMA).identity("c2").build();

    Table testTable =
        TestTables.create(
            temp.newFolder("partition_stats_schema_evolve"),
            "partition_stats_schema_evolve",
            SCHEMA,
            specBefore,
            SortOrder.unsorted(),
            2);
    // foo -> 4 records, bar -> 3 records
    List<Record> records = prepareRecords(testTable.schema());
    DataFile dataFile1 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo"), records.subList(0, 4));
    DataFile dataFile2 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar"), records.subList(4, 7));

    for (int i = 0; i < 2; i++) {
      // insert same set of seven records twice to have a new manifest files
      testTable.newAppend().appendFile(dataFile1).appendFile(dataFile2).commit();
    }

    Snapshot snapshot1 = testTable.currentSnapshot();
    Schema recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();

    computeAndValidatePartitionStats(
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo"),
            0,
            8L,
            2,
            2 * dataFile1.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar"),
            0,
            6L,
            2,
            2 * dataFile2.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()));

    // Evolve the partition spec to include c3
    testTable.updateSpec().addField("c3").commit();
    records = prepareRecords(testTable.schema());
    // append null partition record
    GenericRecord record = GenericRecord.create(testTable.schema());
    records.add(record.copy("c1", 0, "c2", "bar", "c3", null));

    DataFile dataFile3 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "A"), records.subList(0, 3));
    DataFile dataFile4 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("foo", "B"), records.subList(3, 4));
    DataFile dataFile5 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "A"), records.subList(4, 5));
    DataFile dataFile6 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", "B"), records.subList(5, 7));
    DataFile dataFile7 =
        FileHelpers.writeDataFile(
            testTable, outputFile(), TestHelpers.Row.of("bar", null), records.subList(7, 8));

    testTable
        .newAppend()
        .appendFile(dataFile3)
        .appendFile(dataFile4)
        .appendFile(dataFile5)
        .appendFile(dataFile6)
        .appendFile(dataFile7)
        .commit();

    Snapshot snapshot2 = testTable.currentSnapshot();
    recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    computeAndValidatePartitionStats(
        testTable,
        recordSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", null),
            0,
            8L,
            2,
            2 * dataFile1.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot1.timestampMillis(),
            snapshot1.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", null),
            1, // observe the old spec id as the record is unmodified
            7L,
            3,
            2 * dataFile2.fileSizeInBytes() + dataFile7.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            1,
            3L,
            1,
            dataFile3.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            1,
            1L,
            1,
            dataFile4.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            1,
            1L,
            1,
            dataFile5.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            1,
            2L,
            1,
            dataFile6.fileSizeInBytes(),
            null,
            null,
            null,
            null,
            null,
            snapshot2.timestampMillis(),
            snapshot2.snapshotId()));
  }

  private OutputFile outputFile() throws IOException {
    return Files.localOutput(File.createTempFile("data", null, temp.newFolder()));
  }

  private static Record partitionRecord(Types.StructType partitionType, String c2, String c3) {
    Record partitionData = GenericRecord.create(partitionType);
    partitionData.set(0, c2);
    partitionData.set(1, c3);
    return partitionData;
  }

  private static Record partitionRecord(Types.StructType partitionType, String c2) {
    Record partitionData = GenericRecord.create(partitionType);
    partitionData.set(0, c2);
    return partitionData;
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

  private static void computeAndValidatePartitionStats(
      Table testTable, Schema recordSchema, Tuple... expectedValues) throws IOException {
    // compute and commit partition stats file
    Snapshot currentSnapshot = testTable.currentSnapshot();
    PartitionStatsGenerator partitionStatsGenerator = new PartitionStatsGenerator(testTable);
    PartitionStatisticsFile result = partitionStatsGenerator.generate();
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();
    assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // read the partition entries from the stats file
    List<Record> rows;
    try (CloseableIterable<Record> recordIterator =
        PartitionStatsWriterUtil.readPartitionStatsFile(
            recordSchema, Files.localInput(result.path()))) {
      rows = Lists.newArrayList(recordIterator);
    }
    assertThat(rows)
        .extracting(
            row -> row.get(PartitionStatsUtil.Column.PARTITION.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.SPEC_ID.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_FILE_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.POSITION_DELETE_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.POSITION_DELETE_FILE_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.EQUALITY_DELETE_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.EQUALITY_DELETE_FILE_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.TOTAL_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.LAST_UPDATED_AT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.LAST_UPDATED_SNAPSHOT_ID.ordinal()))
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
            Files.localOutput(File.createTempFile("junit", null, temp.newFolder())),
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
          positionDelete(testTable.schema(), dataFile1.path(), i, (int) i, String.valueOf(i)));
    }
    DeleteFile posDeletes =
        FileHelpers.writePosDeleteFile(
            testTable,
            Files.localOutput(File.createTempFile("junit", null, temp.newFolder())),
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
}
