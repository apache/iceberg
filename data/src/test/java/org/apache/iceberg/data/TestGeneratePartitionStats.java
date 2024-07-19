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
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGeneratePartitionStats {
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

    GeneratePartitionStats generatePartitionStats = new GeneratePartitionStats(testTable);
    assertThat(generatePartitionStats.generate()).isNull();
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

    GeneratePartitionStats generatePartitionStats =
        new GeneratePartitionStats(testTable, "INVALID_BRANCH");
    assertThatThrownBy(generatePartitionStats::generate)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Couldn't find the snapshot for the branch INVALID_BRANCH");
  }

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

    Schema recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    List<Record> rows = computeAndReadPartitionStats(testTable, recordSchema);
    Types.StructType partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    assertThat(rows)
        .extracting(
            row -> row.get(PartitionStatsUtil.Column.PARTITION.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_FILE_COUNT.ordinal()))
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 9L, 3),
            Tuple.tuple(partitionRecord(partitionType, "foo", "B"), 3L, 3),
            Tuple.tuple(partitionRecord(partitionType, "bar", "A"), 3L, 3),
            Tuple.tuple(partitionRecord(partitionType, "bar", "B"), 6L, 3));
  }

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

    Schema recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    List<Record> rows = computeAndReadPartitionStats(testTable, recordSchema);
    Types.StructType partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    assertThat(rows)
        .extracting(
            row -> row.get(PartitionStatsUtil.Column.PARTITION.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_FILE_COUNT.ordinal()))
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo"), 8L, 2),
            Tuple.tuple(partitionRecord(partitionType, "bar"), 6L, 2));

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

    recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    rows = computeAndReadPartitionStats(testTable, recordSchema);
    partitionType =
        recordSchema.findField(PartitionStatsUtil.Column.PARTITION.name()).type().asStructType();
    assertThat(rows)
        .extracting(
            row -> row.get(PartitionStatsUtil.Column.PARTITION.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_FILE_COUNT.ordinal()))
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", null), 8L, 2),
            Tuple.tuple(partitionRecord(partitionType, "bar", null), 7L, 3),
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 3L, 1),
            Tuple.tuple(partitionRecord(partitionType, "foo", "B"), 1L, 1),
            Tuple.tuple(partitionRecord(partitionType, "bar", "A"), 1L, 1),
            Tuple.tuple(partitionRecord(partitionType, "bar", "B"), 2L, 1));
  }

  private static List<Record> computeAndReadPartitionStats(Table testTable, Schema recordSchema)
      throws IOException {
    Snapshot currentSnapshot = testTable.currentSnapshot();
    GeneratePartitionStats generatePartitionStats = new GeneratePartitionStats(testTable);
    PartitionStatisticsFile result = generatePartitionStats.generate();
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();

    assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // read the partition entries from the stats file
    List<Record> rows;
    try (CloseableIterable<Record> recordIterator =
        PartitionStatsWriterUtil.readPartitionStatsFile(
            recordSchema, Files.localInput(result.path()))) {
      rows = Lists.newArrayList(recordIterator);
    }
    return rows;
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

  // TODO: add the testcase with delete files (position and equality)
  //  and also validate each and every field of partition stats.
}
