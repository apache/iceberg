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
package org.apache.iceberg.spark.actions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsHandler;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.types.Types;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

public class TestComputePartitionStatsAction extends CatalogTestBase {

  private static final int DEFAULT_SPEC_ID = 0;
  private static final long DEFAULT_POS_DEL_RECORD_COUNT = 0L;
  private static final int DEFAULT_POS_DEL_FILE_COUNT = 0;
  private static final long DEFAULT_EQ_DEL_RECORD_COUNT = 0L;
  private static final int DEFAULT_EQ_DEL_FILE_COUNT = 0;
  private static final Long DEFAULT_TOTAL_RECORD_COUNT = null;

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void emptyTable() {
    createPartitionedTable();
    Table table = validationCatalog.loadTable(tableIdent);
    ComputePartitionStatsSparkAction.Result result =
        SparkActions.get().computePartitionStats(table).execute();
    assertThat(result.statisticsFile()).isNull();
  }

  @TestTemplate
  public void emptyBranch() {
    createPartitionedTable();
    Table table = validationCatalog.loadTable(tableIdent);
    table.manageSnapshots().createBranch("b1").commit();
    ComputePartitionStatsSparkAction.Result result =
        SparkActions.get()
            .computePartitionStats(table)
            .snapshot(table.refs().get("b1").snapshotId())
            .execute();
    assertThat(result.statisticsFile()).isNull();
  }

  @TestTemplate
  public void invalidSnapshot() {
    createPartitionedTable();
    Table table = validationCatalog.loadTable(tableIdent);
    assertThatThrownBy(
            () -> SparkActions.get().computePartitionStats(table).snapshot(42L).execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Snapshot not found: 42");
  }

  @TestTemplate
  public void partitionStatsComputeOnLatestSnapshot() throws IOException {
    createPartitionedTable();
    // foo, A -> 4 records,
    // foo, B -> 2 records,
    // bar, A -> 2 records,
    // bar, B -> 1 record
    sql(
        "INSERT into %s values (0, 'foo', 'A'), (1, 'foo', 'A'), (2, 'foo', 'B'), (3, 'foo', 'B')",
        tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot1 = table.currentSnapshot();
    sql("INSERT into %s values(4, 'bar', 'A'), (5, 'bar', 'A'), (6, 'bar', 'B')", tableName);
    table.refresh();
    Snapshot snapshot2 = table.currentSnapshot();
    sql("INSERT into %s values(7, 'foo', 'A'), (8, 'foo', 'A')", tableName);
    // snapshot3 is unused for partition stats as the same partition is modified by snapshot4

    // delete one record of foo, A
    sql("DELETE FROM %s WHERE c1=1", tableName);
    table.refresh();
    Snapshot snapshot4 = table.currentSnapshot();

    assertThat(table.partitionStatisticsFiles()).isEmpty();

    PartitionStatisticsFile statisticsFile =
        SparkActions.get().computePartitionStats(table).execute().statisticsFile();
    assertThat(statisticsFile.fileSizeInBytes()).isGreaterThan(0);
    assertThat(statisticsFile.snapshotId()).isEqualTo(snapshot4.snapshotId());
    // check table metadata registration
    assertThat(table.partitionStatisticsFiles()).containsExactly(statisticsFile);

    Types.StructType partitionType = Partitioning.partitionType(table);
    Schema dataSchema = PartitionStatsHandler.schema(partitionType);
    validatePartitionStats(
        statisticsFile,
        dataSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            DEFAULT_SPEC_ID,
            4L, // dataRecordCount (total 4 records for this partition)
            2, // dataFileCount
            totalDataFileSizeInBytes("foo", "A"),
            1L, // positionDeleteRecordCount (from delete operation)
            1, // positionDeleteFileCount (from delete operation)
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot4.timestampMillis(), // lastUpdatedAt (last modified by snapshot4)
            snapshot4.snapshotId() // lastUpdatedSnapshotId
            ),
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            DEFAULT_SPEC_ID,
            2L, // dataRecordCount
            1, // dataFileCount
            totalDataFileSizeInBytes("foo", "B"),
            DEFAULT_POS_DEL_RECORD_COUNT,
            DEFAULT_POS_DEL_FILE_COUNT,
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot1.timestampMillis(), // lastUpdatedAt (added by snapshot1)
            snapshot1.snapshotId() // lastUpdatedSnapshotId
            ),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "A"),
            DEFAULT_SPEC_ID,
            2L, // dataRecordCount
            1, // dataFileCount
            totalDataFileSizeInBytes("bar", "A"),
            DEFAULT_POS_DEL_RECORD_COUNT,
            DEFAULT_POS_DEL_FILE_COUNT,
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot2.timestampMillis(), // lastUpdatedAt (added by snapshot2)
            snapshot2.snapshotId() // lastUpdatedSnapshotId
            ),
        Tuple.tuple(
            partitionRecord(partitionType, "bar", "B"),
            DEFAULT_SPEC_ID,
            1L, // dataRecordCount
            1, // dataFileCount
            totalDataFileSizeInBytes("bar", "B"),
            DEFAULT_POS_DEL_RECORD_COUNT,
            DEFAULT_POS_DEL_FILE_COUNT,
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot2.timestampMillis(), // lastUpdatedAt
            snapshot2.snapshotId() // lastUpdatedSnapshotId
            ));
  }

  @TestTemplate
  public void partitionStatsComputeOnSnapshot() throws IOException {
    createPartitionedTableV1();
    // foo, A -> 2 records,
    // foo, B -> 1 record,
    // bar, A -> 2 records,
    sql("INSERT into %s values (0, 'foo', 'A'), (1, 'foo', 'A'), (2, 'foo', 'B')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snapshot1 = table.currentSnapshot();
    sql("INSERT into %s values(3, 'bar', 'A'), (4, 'bar', 'A')", tableName);
    table.refresh();

    assertThat(table.partitionStatisticsFiles()).isEmpty();

    PartitionStatisticsFile statisticsFile =
        SparkActions.get()
            .computePartitionStats(table)
            .snapshot(snapshot1.snapshotId())
            .execute()
            .statisticsFile();
    assertThat(statisticsFile.fileSizeInBytes()).isGreaterThan(0);
    // should be mapped to snapshot1 instead of latest snapshot
    assertThat(statisticsFile.snapshotId()).isEqualTo(snapshot1.snapshotId());
    // check table metadata registration
    assertThat(table.partitionStatisticsFiles()).containsExactly(statisticsFile);

    Types.StructType partitionType = Partitioning.partitionType(table);
    Schema dataSchema = PartitionStatsHandler.schema(partitionType);
    // should contain stats for only partitions of snapshot1 (no entry for partition bar, A)
    validatePartitionStats(
        statisticsFile,
        dataSchema,
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "A"),
            DEFAULT_SPEC_ID,
            2L, // dataRecordCount
            1, // dataFileCount
            totalDataFileSizeInBytes("foo", "A"),
            DEFAULT_POS_DEL_RECORD_COUNT,
            DEFAULT_POS_DEL_FILE_COUNT,
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot1.timestampMillis(), // lastUpdatedAt
            snapshot1.snapshotId()), // lastUpdatedSnapshotId
        Tuple.tuple(
            partitionRecord(partitionType, "foo", "B"),
            DEFAULT_SPEC_ID,
            1L, // dataRecordCount
            1, // dataFileCount
            totalDataFileSizeInBytes("foo", "B"),
            DEFAULT_POS_DEL_RECORD_COUNT,
            DEFAULT_POS_DEL_FILE_COUNT,
            DEFAULT_EQ_DEL_RECORD_COUNT,
            DEFAULT_EQ_DEL_FILE_COUNT,
            DEFAULT_TOTAL_RECORD_COUNT,
            snapshot1.timestampMillis(), // lastUpdatedAt
            snapshot1.snapshotId() // lastUpdatedSnapshotId
            ));

    // try again on same snapshot
    PartitionStatisticsFile newStatsFile =
        SparkActions.get()
            .computePartitionStats(table)
            .snapshot(snapshot1.snapshotId())
            .execute()
            .statisticsFile();
    assertThat(newStatsFile).isEqualTo(statisticsFile);
  }

  private long totalDataFileSizeInBytes(String col1, String col2) {
    return (long)
        sql(
                "SELECT sum(file_size_in_bytes) FROM %s.data_files WHERE partition.c2 = '%s' AND partition.c3 = '%s'",
                tableName, col1, col2)
            .get(0)[0];
  }

  private void createPartitionedTable() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg PARTITIONED BY (c2, c3) TBLPROPERTIES('write.delete.mode'='merge-on-read')",
        tableName);
  }

  private void createPartitionedTableV1() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg PARTITIONED BY (c2, c3) TBLPROPERTIES('format-version'='1')",
        tableName);
  }

  private void validatePartitionStats(
      PartitionStatisticsFile result, Schema recordSchema, Tuple... expectedValues)
      throws IOException {
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

  private StructLike partitionRecord(Types.StructType partitionType, String val1, String val2) {
    GenericRecord record = GenericRecord.create(partitionType);
    record.set(0, val1);
    record.set(1, val2);
    return record;
  }
}
