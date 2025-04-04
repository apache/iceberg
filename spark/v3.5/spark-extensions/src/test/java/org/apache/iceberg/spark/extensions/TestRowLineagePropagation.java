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

import static org.apache.iceberg.spark.Spark3Util.loadIcebergTable;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.execution.SparkPlan;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;

public abstract class TestRowLineagePropagation extends SparkRowLevelOperationsTestBase {

  static Schema rowLineageSchema =
      new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
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
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (data) VALUES (5678)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0),
            row(2),
            row(3),
            row(4),
            row(1234), // updated
            row(5678) // inserted
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY data", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 1234, 1L, null);
    Record insertedRecord = createRecord(schema, 5678, null, null);
    expectedRecords.addAll(ImmutableList.of(insertedRecord, updatedRecord));

    table.refresh();

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  @TestTemplate
  public void testMergeIntoWithBothMatchedAndNonMatchedPartitioned()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", "PARTITIONED BY (data)", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<StructLike> partitions =
        IntStream.range(0, 5)
            .mapToObj(
                partitionValue ->
                    createPartitionStructure(
                        table.spec().partitionType(), ImmutableMap.of("data", partitionValue)))
            .collect(Collectors.toList());
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5, partitions);

    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (data) VALUES (5678)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0),
            row(2),
            row(3),
            row(4),
            row(1234), // updated
            row(5678) // inserted
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY data", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 1234, 1L, null);
    Record insertedRecord = createRecord(schema, 5678, null, null);
    expectedRecords.addAll(ImmutableList.of(insertedRecord, updatedRecord));

    table.refresh();

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  private StructLike createPartitionStructure(
      Types.StructType partitionType, Map<String, Object> partitionValues) {
    Record record = GenericRecord.create(partitionType);
    return record.copy(partitionValues);
  }

  @TestTemplate
  public void testMergeIntoWithOnlyNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (data) VALUES (5678)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0), row(1), row(2), row(3), row(4), row(5678) // inserted
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY data", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(generatedRecords);
    Record insertedRecord = createRecord(schema, 5678, null, null);
    expectedRecords.add(insertedRecord);
    table.refresh();

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  @TestTemplate
  public void testMergeIntoWithOnlyMatched()
      throws IOException, NoSuchTableException, ParseException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.data == s.data AND t.data = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.data = 1234 ",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0), row(2), row(3), row(4), row(1234) // updated
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY data", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 1234, 1L, null);
    expectedRecords.add(updatedRecord);
    table.refresh();

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  @TestTemplate
  public void testMergeMatchedDelete() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 2}\n");
    SparkPlan plan =
        executeAndKeepPlan(
            "MERGE INTO %s AS t USING source AS s "
                + "ON t.data == s.data AND t.data = 1 "
                + "WHEN MATCHED THEN DELETE",
            commitTarget());

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySource()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 42}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.data == s.data AND t.data = 1"
            + " WHEN MATCHED THEN UPDATE set data = 1234 "
            + "WHEN NOT MATCHED BY SOURCE THEN UPDATE set data = 5678",
        commitTarget());

    List<Record> expectedRecords = Lists.newArrayList();
    // Every record should preserve its row ID and null out its sequence number.
    // Every record other than id 1 should be 5678 and
    for (Record record : generatedRecords) {
      Record newRecord = record.copy();
      if (record.get(0, Integer.class) == 1) {
        newRecord.set(0, 1234);
        newRecord.set(2, null);
      } else {
        newRecord.set(0, 5678);
        newRecord.set(2, null);
      }

      expectedRecords.add(newRecord);
    }

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  @TestTemplate
  public void testMergeWhenNotMatchedBySourceDelete()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    createAndInitTable("data INT", null);
    createBranchIfNeeded();
    Table table = loadIcebergTable(spark, tableName);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "data INT", "{ \"data\": 1}\n" + "{ \"data\": 42}\n");

    sql(
        "MERGE INTO %s AS t USING source AS s ON t.data == s.data AND t.data = 1"
            + " WHEN MATCHED THEN UPDATE set data = 1234 "
            + "WHEN NOT MATCHED BY SOURCE THEN DELETE",
        commitTarget());

    Record recordWithData1 =
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class).equals(1))
            .findFirst()
            .get();
    List<Record> expectedRecords = Lists.newArrayList();
    Record record = recordWithData1.copy();
    record.set(0, 1234);
    record.set(2, null);
    expectedRecords.add(record);

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
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
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);

    sql("UPDATE %s AS t set data = 5678 WHERE data = 1", commitTarget());

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 5678, 1L, null);
    expectedRecords.add(updatedRecord);
    table.refresh();

    assertRecordsAreEqual(sortedRecordsByRowId(expectedRecords), recordsSortedByRowId(table));
  }

  protected Record createRecord(Schema schema, int id, Long rowId, Long lastUpdatedSequenceNumber) {
    Record record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, rowId);
    record.set(2, lastUpdatedSequenceNumber);
    return record;
  }

  protected List<Record> recordsSortedByRowId(Table table) throws IOException {
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> records = Lists.newArrayList();
    table.refresh();
    Snapshot snapshot = table.currentSnapshot();
    if (branch != null) {
      snapshot = table.snapshot(branch);
    }

    try (CloseableIterable<Record> writtenRecords =
        IcebergGenerics.read(table).project(schema).useSnapshot(snapshot.snapshotId()).build()) {
      for (Record record : writtenRecords) {
        records.add(record);
      }
    }

    records.sort(
        Comparator.comparing(
            record -> record.get(1, Long.class), Comparator.nullsFirst(Comparator.naturalOrder())));
    return records;
  }

  protected List<Record> sortedRecordsByRowId(List<Record> records) {
    records.sort(
        Comparator.comparing(
            record -> record.get(1, Long.class), Comparator.nullsFirst(Comparator.naturalOrder())));
    return records;
  }

  protected List<Record> generateRecordsAndAppendToTable(
      Table table, int numRecords, List<StructLike> partitions) throws IOException {
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> records = Lists.newArrayList();
    Map<Integer, List<Record>> partitionToRecords = Maps.newHashMap();
    Map<Integer, StructLike> partitionToPartitions = Maps.newHashMap();
    for (int i = 0; i < numRecords; i++) {
      records.add(createRecord(schema, i, (long) i, (long) i * 2));
      int partitionKey = partitions.get(i) != null ? i : -1;
      List<Record> existingRecords =
          partitionToRecords.getOrDefault(partitionKey, Lists.newArrayList());
      existingRecords.add(records.get(i));
      partitionToRecords.put(partitionKey, existingRecords);
    }

    for (StructLike partition : partitions) {
      if (partition != null) {
        partitionToPartitions.put(partition.get(0, Integer.class), partition);
      }
    }

    AppendFiles append = table.newAppend();
    for (Map.Entry<Integer, List<Record>> entry : partitionToRecords.entrySet()) {
      OutputFile file = Files.localOutput(temp.resolve(UUID.randomUUID().toString()).toFile());
      StructLike partition =
          entry.getKey() == -1 ? null : partitionToPartitions.get(entry.getKey());
      DataWriter<Record> writer =
          new GenericAppenderFactory(schema, table.spec())
              .newDataWriter(EncryptionUtil.plainAsEncryptedOutput(file), fileFormat, partition);
      List<Record> recordsForPartition = partitionToRecords.get(entry.getKey());
      writer.write(recordsForPartition);
      writer.close();
      append =
          append
              .appendFile(writer.toDataFile())
              .toBranch(branch != null ? branch : SnapshotRef.MAIN_BRANCH);
    }

    append.commit();

    return records;
  }

  protected List<Record> generateRecordsAndAppendToTable(Table table, int numRecords)
      throws IOException {
    List<StructLike> partitions = Lists.newArrayList();
    for (int i = 0; i < numRecords; i++) {
      partitions.add(null);
    }

    return generateRecordsAndAppendToTable(table, numRecords, partitions);
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
}
