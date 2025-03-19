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

import static org.assertj.core.api.Assumptions.assumeThat;

import com.clearspring.analytics.util.Lists;
import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
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
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.TypeUtil;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestRowLineagePropogation extends SparkRowLevelOperationsTestBase {

  public TestRowLineagePropogation(
      String catalogName,
      String implementation,
      Map<String, String> config,
      String fileFormat,
      boolean vectorized,
      String distributionMode,
      boolean fanoutEnabled,
      String branch,
      PlanningMode planningMode,
      int formatVersion) {
    super(
        catalogName,
        implementation,
        config,
        fileFormat,
        vectorized,
        distributionMode,
        fanoutEnabled,
        branch,
        planningMode,
        formatVersion);
  }

  static Schema rowLineageSchema =
      new Schema(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER);

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @Test
  public void testMergeIntoWithBothMatchedAndNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("id INT", null);
    createBranchIfNeeded();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "id INT", "{ \"id\": 1}\n" + "{ \"id\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id AND t.id = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.id = 1234 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (id) VALUES (5678)",
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
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 1234, 1L, null);
    Record insertedRecord = createRecord(schema, 5678, null, null);
    expectedRecords.addAll(ImmutableList.of(updatedRecord, insertedRecord));
    table.refresh();

    List<Record> actualRecords = recordsSortedById(table);

    assertRecordsAreEqual(expectedRecords, actualRecords);
  }

  @Test
  public void testMergeIntoWithOnlyNonMatched()
      throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("id INT", null);
    createBranchIfNeeded();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "id INT", "{ \"id\": 1}\n" + "{ \"id\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id AND t.id = 1 "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT (id) VALUES (5678)",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0), row(1), row(2), row(3), row(4), row(5678) // inserted
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(generatedRecords);
    Record insertedRecord = createRecord(schema, 5678, null, null);
    expectedRecords.add(insertedRecord);
    table.refresh();

    List<Record> actualRecords = recordsSortedById(table);

    assertRecordsAreEqual(expectedRecords, actualRecords);
  }

  @Test
  public void testMergeIntoWithOnlyMatched()
      throws IOException, NoSuchTableException, ParseException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("id INT", null);
    createBranchIfNeeded();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "id INT", "{ \"id\": 1}\n" + "{ \"id\": 2}\n");
    sql(
        "MERGE INTO %s AS t USING source AS s "
            + "ON t.id == s.id AND t.id = 1 "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET t.id = 1234 ",
        commitTarget());

    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(0), row(2), row(3), row(4), row(1234) // updated
            );

    assertEquals(
        "Output should match", expectedRows, sql("SELECT * FROM %s ORDER BY id", selectTarget()));

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
        generatedRecords.stream()
            .filter(record -> record.get(0, Integer.class) != 1)
            .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 1234, 1L, null);
    expectedRecords.add(updatedRecord);
    table.refresh();

    List<Record> actualRecords = recordsSortedById(table);

    assertRecordsAreEqual(expectedRecords, actualRecords);
  }

  @Test
  public void testMergeMatchedDelete() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("id INT", null);
    createBranchIfNeeded();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);
    createOrReplaceView("source", "id INT", "{ \"id\": 1}\n" + "{ \"id\": 2}\n");
    sql(
            "MERGE INTO %s AS t USING source AS s "
                    + "ON t.id == s.id AND t.id = 1 "
                    + "WHEN MATCHED THEN DELETE",
            commitTarget());
  }

  @Test
  public void testUpdate() throws NoSuchTableException, ParseException, IOException {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);
    // write the data file with <data, _row_id, _last_updated_sequence_number>
    // merge, update data to data + 1 on odd values of data. _row_id should be preserved for those
    // cases, _last_updated_sequence_number should be null
    // even values of data should be preserved as is
    createAndInitTable("id INT", null);
    createBranchIfNeeded();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> generatedRecords = generateRecordsAndAppendToTable(table, 5);

    sql("UPDATE %s AS t SET id = 5678 WHERE id = 1", commitTarget());

    List<Record> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(
            generatedRecords.stream()
                    .filter(record -> record.get(0, Integer.class) != 1)
                    .collect(Collectors.toList()));
    Record updatedRecord = createRecord(schema, 5678, 1L, null);
    expectedRecords.add(updatedRecord);
    table.refresh();

    List<Record> actualRecords = recordsSortedById(table);

    assertRecordsAreEqual(expectedRecords, actualRecords);
  }

  protected Record createRecord(Schema schema, int id, Long rowId, Long lastUpdatedSequenceNumber) {
    Record record = GenericRecord.create(schema);
    record.set(0, id);
    record.set(1, rowId);
    record.set(2, lastUpdatedSequenceNumber);
    return record;
  }

  protected List<Record> recordsSortedById(Table table) throws IOException {
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> records = Lists.newArrayList();
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

    records.sort(Comparator.comparing(record -> record.get(0, Integer.class)));
    return records;
  }

  protected List<Record> generateRecordsAndAppendToTable(Table table, int numRecords)
      throws IOException {
    Schema schema = TypeUtil.join(table.schema(), rowLineageSchema);
    List<Record> records = Lists.newArrayList();

    for (int i = 0; i < numRecords; i++) {
      records.add(createRecord(schema, i, (long) i, (long) i * 2));
    }

    OutputFile file = Files.localOutput(temp.newFile());
    try (DataWriter<Record> writer =
        new GenericAppenderFactory(schema)
            .newDataWriter(
                EncryptionUtil.plainAsEncryptedOutput(file),
                FileFormat.fromString(fileFormat),
                null)) {
      for (Record record : records) {
        writer.write(record);
      }
      writer.close();
      table
          .newAppend()
          .appendFile(writer.toDataFile())
          .toBranch(branch != null ? branch : SnapshotRef.MAIN_BRANCH)
          .commit();
    }

    return records;
  }

  protected void assertRecordsAreEqual(List<Record> expected, List<Record> actual) {
    Assert.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assert.assertEquals(expected.get(i).get(0), actual.get(i).get(0));
      Assert.assertEquals(expected.get(i).get(1), actual.get(i).get(1));
      Assert.assertEquals(expected.get(i).get(2), actual.get(i).get(2));
    }
  }
}
