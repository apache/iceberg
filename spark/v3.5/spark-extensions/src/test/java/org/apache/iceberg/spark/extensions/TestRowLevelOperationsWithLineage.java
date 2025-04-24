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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableUtil.schemaWithRowLineage;
import static org.apache.iceberg.spark.Spark3Util.loadIcebergTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestRowLevelOperationsWithLineage extends SparkRowLevelOperationsTestBase {
  static final Function<Record, StructLike> UNPARTITIONED_GENERATOR = record -> null;
  // Use the first field in the row as identity partition
  static final Function<Record, StructLike> IDENTITY_PARTITIONED_GENERATOR =
      record -> TestHelpers.Row.of(record.get(0, Integer.class));

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @BeforeEach
  public void beforeEach() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // ToDo: Remove these as row lineage inheritance gets implemented in the other readers
    assumeThat(fileFormat).isEqualTo(FileFormat.PARQUET);
    assumeThat(vectorized).isFalse();
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @TestTemplate
  public void testMergeIntoWithBothMatchedAndNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(
            schemaWithRowLineage(table), table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 5678}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    List<Object[]> rowsWhereIdShouldBePreserved =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data != 5678 ORDER BY _row_id",
            selectTarget());
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(0, 0L, 0L),
            row(1234, 1L, updateSequenceNumber),
            row(2, 2L, 2L),
            row(3, 3L, 3L),
            row(4, 4L, 4L)),
        rowsWhereIdShouldBePreserved);

    // New row with data 5678 should have any row ID higher than the max in the previous commit
    // version (4), and have the latest seq number
    long previousHighestRowId = 4;
    List<Object[]> newRows =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data = 5678 ORDER BY _row_id",
            selectTarget());
    assertRowsHaveRowIdsGreaterThan(newRows, 5678, previousHighestRowId, updateSequenceNumber);
  }

  @TestTemplate
  public void testMergeIntoWithBothMatchedAndNonMatchedPartitioned()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    createAndInitTable("data INT", "PARTITIONED BY (data)", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = schemaWithRowLineage(table);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(schema, table.spec(), 5, IDENTITY_PARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 5678}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    List<Object[]> rowsWhereIdShouldBePreserved =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data != 5678 ORDER BY _row_id",
            selectTarget());
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(0, 0L, 0L),
            row(1234, 1L, updateSequenceNumber),
            row(2, 2L, 2L),
            row(3, 3L, 3L),
            row(4, 4L, 4L)),
        rowsWhereIdShouldBePreserved);

    // New row with data 5678 should have any row ID higher than the max in the previous commit
    // version (4), and have the latest seq number
    long previousHighestRowId = 4;
    List<Object[]> newRows =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data = 5678 ORDER BY _row_id",
            selectTarget());
    assertRowsHaveRowIdsGreaterThan(newRows, 5678, previousHighestRowId, updateSequenceNumber);
  }

  @TestTemplate
  public void testMergeIntoWithOnlyNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = schemaWithRowLineage(table);

    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(schema, table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 5678}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data "
            + "WHEN NOT MATCHED THEN "
            + "INSERT *",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    List<Object[]> rowsWhereIdShouldBePreserved =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data != 5678 ORDER BY _row_id",
            selectTarget());
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(0, 0L, 0L), row(1, 1L, 1L), row(2, 2L, 2L), row(3, 3L, 3L), row(4, 4L, 4L)),
        rowsWhereIdShouldBePreserved);

    // New row with data 5678 should have any row ID higher than the max in the previous commit
    // version (4), and have the latest seq number
    long previousHighestRowId = 4;
    List<Object[]> newRows =
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s WHERE data = 5678 ORDER BY _row_id",
            selectTarget());
    assertThat(newRows).hasSize(1);
    assertRowsHaveRowIdsGreaterThan(newRows, 5678, previousHighestRowId, updateSequenceNumber);
  }

  @TestTemplate
  public void testMergeIntoWithOnlyMatched()
      throws IOException, NoSuchTableException, ParseException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(
            schemaWithRowLineage(table), table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 ",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(0, 0L, 0L),
            row(1234, 1L, updateSequenceNumber),
            row(2, 2L, 2L),
            row(3, 3L, 3L),
            row(4, 4L, 4L)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeMatchedDelete() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = schemaWithRowLineage(table);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(schema, table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN MATCHED THEN DELETE",
        commitTarget());

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(row(0, 0L, 0L), row(2, 2L, 2L), row(3, 3L, 3L), row(4, 4L, 4L)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySource()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);

    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(
            schemaWithRowLineage(table), table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 42}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.data == s.data AND t.data = 1"
            + " WHEN MATCHED THEN UPDATE set data = 1234 "
            + "WHEN NOT MATCHED BY SOURCE THEN UPDATE set data = 5678",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(5678, 0L, updateSequenceNumber),
            row(1234, 1L, updateSequenceNumber),
            row(5678, 2L, updateSequenceNumber),
            row(5678, 3L, updateSequenceNumber),
            row(5678, 4L, updateSequenceNumber)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySourceDelete()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(
            schemaWithRowLineage(table), table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 42}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.data == s.data AND t.data = 1"
            + " WHEN MATCHED THEN UPDATE set data = 1234 "
            + "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        commitTarget());

    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();
    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(row(1234, 1L, updateSequenceNumber)),
        rowsWithLineage());
  }

  @TestTemplate
  public void testUpdate() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = schemaWithRowLineage(table);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(schema, table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);

    sql("UPDATE %s AS t set data = 5678 WHERE data = 1", commitTarget());
    long updateSequenceNumber = latestSnapshot(table).sequenceNumber();

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(
            row(0, 0L, 0L),
            row(5678, 1L, updateSequenceNumber),
            row(2, 2L, 2L),
            row(3, 3L, 3L),
            row(4, 4L, 4L)),
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s ORDER BY _row_id",
            selectTarget()));
  }

  @TestTemplate
  public void testDelete() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = schemaWithRowLineage(table);
    PartitionMap<List<Record>> recordsByPartition =
        createRecordsWithRowLineage(schema, table.spec(), 5, UNPARTITIONED_GENERATOR);
    appendRecords(table, recordsByPartition);

    sql("DELETE FROM %s WHERE data = 1", commitTarget());

    assertEquals(
        "Rows which are carried over or updated should have expected lineage",
        ImmutableList.of(row(0, 0L, 0L), row(2, 2L, 2L), row(3, 3L, 3L), row(4, 4L, 4L)),
        sql(
            "SELECT data, _row_id, _last_updated_sequence_number FROM %s ORDER BY _row_id",
            selectTarget()));
  }

  private List<Object[]> rowsWithLineage() {
    return sql(
        "SELECT data, _row_id, _last_updated_sequence_number FROM %s ORDER BY _row_id",
        selectTarget());
  }

  protected Record createRecordWithRowLineage(
      Schema schema, int id, Long rowId, Long lastUpdatedSequenceNumber) {
    Record record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, rowId);
    record.set(2, lastUpdatedSequenceNumber);
    return record;
  }

  /**
   * Generates numRecords records with data values as 0 to numRecords - 1, with row lineage, each
   * record is partitioned based on the provided partition generator. The row ID and last updated
   * sequence number assigned is the same as the data value for that row.
   *
   * @return a partitioned map
   */
  protected PartitionMap<List<Record>> createRecordsWithRowLineage(
      Schema schema,
      PartitionSpec spec,
      int numRecords,
      Function<Record, StructLike> partitionGenerator) {
    PartitionMap<List<Record>> recordsByPartition =
        PartitionMap.create(Map.of(spec.specId(), spec));
    for (int i = 0; i < numRecords; i++) {
      long rowId = i;
      long lastUpdatedSequenceNumber = i;
      Record record = createRecordWithRowLineage(schema, i, rowId, lastUpdatedSequenceNumber);
      StructLike partition = partitionGenerator != null ? partitionGenerator.apply(record) : null;
      List<Record> recordsForPartition = recordsByPartition.get(spec.specId(), partition);
      if (recordsForPartition == null) {
        recordsForPartition = Lists.newArrayList();
      }

      recordsForPartition.add(record);
      recordsByPartition.put(spec.specId(), partition, recordsForPartition);
    }

    return recordsByPartition;
  }

  protected void appendRecords(Table table, PartitionMap<List<Record>> partitionedRecords)
      throws IOException {
    AppendFiles append = table.newAppend();

    for (Map.Entry<Pair<Integer, StructLike>, List<Record>> entry : partitionedRecords.entrySet()) {
      OutputFile file = Files.localOutput(temp.resolve(UUID.randomUUID().toString()).toFile());
      DataWriter<Record> writer =
          new GenericAppenderFactory(schemaWithRowLineage(table), table.spec())
              .newDataWriter(
                  EncryptionUtil.plainAsEncryptedOutput(file), fileFormat, entry.getKey().second());
      List<Record> recordsForPartition = entry.getValue();
      writer.write(recordsForPartition);
      writer.close();
      append =
          append
              .appendFile(writer.toDataFile())
              .toBranch(branch != null ? branch : SnapshotRef.MAIN_BRANCH);
    }

    append.commit();
  }

  protected void assertRecordsAreEqual(List<Record> expected, List<Record> actual) {
    assertThat(actual.size()).isEqualTo(expected.size());
    for (int i = 0; i < expected.size(); i++) {
      Record expectedRecord = expected.get(i);
      Record actualRecord = actual.get(i);
      assertThat(expectedRecord.get(0)).isEqualTo(actualRecord.get(0));
      assertThat(expectedRecord.get(1)).isEqualTo(actualRecord.get(1));
      assertThat(expectedRecord.get(2)).isEqualTo(actualRecord.get(2));
    }
  }

  private Snapshot latestSnapshot(Table table) {
    return branch != null ? table.snapshot(branch) : table.currentSnapshot();
  }

  private void assertRowsHaveRowIdsGreaterThan(
      List<Object[]> rows,
      int expectedData,
      long rowIdWatermark,
      long expectedLastUpdatedSequenceNumber) {
    for (Object[] row : rows) {
      assertThat(row[0]).isEqualTo(expectedData);
      assertThat((long) row[1]).isGreaterThan(rowIdWatermark);
      assertThat((long) row[2]).isEqualTo(expectedLastUpdatedSequenceNumber);
    }
  }
}
