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
import org.assertj.core.api.Assertions;
import org.assertj.core.groups.Tuple;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestComputePartitionStats {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c2").identity("c3").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();

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

    List<Record> records = Lists.newArrayList();
    Record record = GenericRecord.create(testTable.schema());
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
      // insert same set of records thrice to have a new manifest files
      testTable
          .newAppend()
          .appendFile(dataFile1)
          .appendFile(dataFile2)
          .appendFile(dataFile3)
          .appendFile(dataFile4)
          .commit();
    }

    Snapshot currentSnapshot = testTable.currentSnapshot();
    GeneratePartitionStats computePartitionStats = new GeneratePartitionStats(testTable);
    PartitionStatisticsFile result = computePartitionStats.generate();
    testTable.updatePartitionStatistics().setPartitionStatistics(result).commit();

    Assertions.assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // read the partition entries from the stats file
    Schema recordSchema = PartitionStatsUtil.schema(Partitioning.partitionType(testTable));
    Types.StructType partitionType =
        recordSchema
            .findField(PartitionStatsUtil.Column.PARTITION_DATA.name())
            .type()
            .asStructType();

    List<Record> rows;
    try (CloseableIterable<Record> recordIterator =
        PartitionStatsWriterUtil.readPartitionStatsFile(
            recordSchema, Files.localInput(result.path()))) {
      rows = Lists.newArrayList(recordIterator);
    }
    Assertions.assertThat(rows)
        .extracting(
            row -> row.get(PartitionStatsUtil.Column.PARTITION_DATA.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_RECORD_COUNT.ordinal()),
            row -> row.get(PartitionStatsUtil.Column.DATA_FILE_COUNT.ordinal()))
        .containsExactlyInAnyOrder(
            Tuple.tuple(partitionRecord(partitionType, "foo", "A"), 9L, 3),
            Tuple.tuple(partitionRecord(partitionType, "foo", "B"), 3L, 3),
            Tuple.tuple(partitionRecord(partitionType, "bar", "A"), 3L, 3),
            Tuple.tuple(partitionRecord(partitionType, "bar", "B"), 6L, 3));
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

  // TODO: add the testcase with delete files (position and equality)
  //  and also validate each and every field of partition stats.
}
