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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.collections.ListUtils;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.util.DateTimeUtils;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestMetadataTables extends SparkExtensionsTestBase {

  public TestMetadataTables(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 1 data manifest", 1, expectedDataManifests.size());
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".files").schema();

    // check delete files table
    Dataset<Row> actualDeleteFilesDs = spark.sql("SELECT * FROM " + tableName + ".delete_files");
    List<Row> actualDeleteFiles = TestHelpers.selectNonDerived(actualDeleteFilesDs).collectAsList();
    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());

    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be one delete file manifest entry", 1, expectedDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // check data files table
    Dataset<Row> actualDataFilesDs = spark.sql("SELECT * FROM " + tableName + ".data_files");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());

    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    // check all files table
    Dataset<Row> actualFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".files ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();

    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());

    List<Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have two files manifest entries", 2, expectedFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testPartitionedTable() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark
        .createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    List<SimpleRecord> recordsB =
        Lists.newArrayList(new SimpleRecord(1, "b"), new SimpleRecord(2, "b"));
    spark
        .createDataset(recordsB, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id=1 AND data='a'", tableName);
    sql("DELETE FROM %s WHERE id=1 AND data='b'", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();

    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    Assert.assertEquals("Should have 2 delete manifests", 2, expectedDeleteManifests.size());

    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".delete_files").schema();

    // Check delete files table
    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals(
        "Should have one delete file manifest entry", 1, expectedDeleteFiles.size());

    Dataset<Row> actualDeleteFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".delete_files " + "WHERE partition.data='a'");
    List<Row> actualDeleteFiles = actualDeleteFilesDs.collectAsList();

    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // Check data files table
    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should have one data file manifest entry", 1, expectedDataFiles.size());

    Dataset<Row> actualDataFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".data_files " + "WHERE partition.data='a'");

    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    List<Row> actualPartitionsWithProjection =
        spark.sql("SELECT file_count FROM " + tableName + ".partitions ").collectAsList();
    Assert.assertEquals(
        "Metadata table should return two partitions record",
        2,
        actualPartitionsWithProjection.size());
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(1, actualPartitionsWithProjection.get(i).get(0));
    }

    // Check files table
    List<Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have two file manifest entries", 2, expectedFiles.size());

    Dataset<Row> actualFilesDs =
        spark.sql(
            "SELECT * FROM " + tableName + ".files " + "WHERE partition.data='a' ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testAllFilesUnpartitioned() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Create delete file
    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    Assert.assertEquals("Should have 1 data manifest", 1, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    List<Object[]> results = sql("DELETE FROM %s", tableName);
    Assert.assertEquals("Table should be cleared", 0, results.size());

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".all_data_files").schema();

    // Check all data files table
    Dataset<Row> actualDataFilesDs = spark.sql("SELECT * FROM " + tableName + ".all_data_files");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();

    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    // Check all delete files table
    Dataset<Row> actualDeleteFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".all_delete_files");
    List<Row> actualDeleteFiles = TestHelpers.selectNonDerived(actualDeleteFilesDs).collectAsList();
    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be one delete file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // Check all files table
    Dataset<Row> actualFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".all_files ORDER BY content");
    List<Row> actualFiles = actualFilesDs.collectAsList();
    List<Record> expectedFiles = ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles, actualFiles);
  }

  @Test
  public void testAllFilesPartitioned() throws Exception {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark
        .createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    List<SimpleRecord> recordsB =
        Lists.newArrayList(new SimpleRecord(1, "b"), new SimpleRecord(2, "b"));
    spark
        .createDataset(recordsB, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Create delete file
    sql("DELETE FROM %s WHERE id=1", tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = TestHelpers.deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    List<Object[]> results = sql("DELETE FROM %s", tableName);
    Assert.assertEquals("Table should be cleared", 0, results.size());

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".all_data_files").schema();

    // Check all data files table
    Dataset<Row> actualDataFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".all_data_files " + "WHERE partition.data='a'");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEqualsSafe(
        SparkSchemaUtil.convert(TestHelpers.selectNonDerived(actualDataFilesDs).schema())
            .asStruct(),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    // Check all delete files table
    Dataset<Row> actualDeleteFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".all_delete_files " + "WHERE partition.data='a'");
    List<Row> actualDeleteFiles = TestHelpers.selectNonDerived(actualDeleteFilesDs).collectAsList();

    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDeleteFiles.size());

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // Check all files table
    Dataset<Row> actualFilesDs =
        spark.sql(
            "SELECT * FROM "
                + tableName
                + ".all_files WHERE partition.data='a' "
                + "ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();

    List<Record> expectedFiles = ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs), expectedFiles, actualFiles);
  }

  @Test
  public void testMetadataLogEntries() throws Exception {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES "
            + "('format-version'='2')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark.createDataset(recordsA, Encoders.bean(SimpleRecord.class)).writeTo(tableName).append();

    List<SimpleRecord> recordsB =
        Lists.newArrayList(new SimpleRecord(1, "b"), new SimpleRecord(2, "b"));
    spark.createDataset(recordsB, Encoders.bean(SimpleRecord.class)).writeTo(tableName).append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Long currentSnapshotId = table.currentSnapshot().snapshotId();
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    Snapshot currentSnapshot = tableMetadata.currentSnapshot();
    Snapshot parentSnapshot = table.snapshot(currentSnapshot.parentId());
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayList(tableMetadata.previousFiles());

    // Check metadataLog table
    List<Object[]> metadataLogs = sql("SELECT * FROM %s.metadata_log_entries", tableName);
    assertEquals(
        "MetadataLogEntriesTable result should match the metadataLog entries",
        ImmutableList.of(
            row(
                DateTimeUtils.toJavaTimestamp(metadataLogEntries.get(0).timestampMillis() * 1000),
                metadataLogEntries.get(0).file(),
                null,
                null,
                null),
            row(
                DateTimeUtils.toJavaTimestamp(metadataLogEntries.get(1).timestampMillis() * 1000),
                metadataLogEntries.get(1).file(),
                parentSnapshot.snapshotId(),
                parentSnapshot.schemaId(),
                parentSnapshot.sequenceNumber()),
            row(
                DateTimeUtils.toJavaTimestamp(currentSnapshot.timestampMillis() * 1000),
                tableMetadata.metadataFileLocation(),
                currentSnapshot.snapshotId(),
                currentSnapshot.schemaId(),
                currentSnapshot.sequenceNumber())),
        metadataLogs);

    // test filtering
    List<Object[]> metadataLogWithFilters =
        sql(
            "SELECT * FROM %s.metadata_log_entries WHERE latest_snapshot_id = %s",
            tableName, currentSnapshotId);
    Assert.assertEquals(
        "metadataLogEntries table should return 1 row", 1, metadataLogWithFilters.size());
    assertEquals(
        "Result should match the latest snapshot entry",
        ImmutableList.of(
            row(
                DateTimeUtils.toJavaTimestamp(
                    tableMetadata.currentSnapshot().timestampMillis() * 1000),
                tableMetadata.metadataFileLocation(),
                tableMetadata.currentSnapshot().snapshotId(),
                tableMetadata.currentSnapshot().schemaId(),
                tableMetadata.currentSnapshot().sequenceNumber())),
        metadataLogWithFilters);

    // test projection
    List<String> metadataFiles =
        metadataLogEntries.stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toList());
    metadataFiles.add(tableMetadata.metadataFileLocation());
    List<Object[]> metadataLogWithProjection =
        sql("SELECT file FROM %s.metadata_log_entries", tableName);
    Assert.assertEquals(
        "metadataLogEntries table should return 3 rows", 3, metadataLogWithProjection.size());
    assertEquals(
        "metadataLog entry should be of same file",
        metadataFiles.stream().map(this::row).collect(Collectors.toList()),
        metadataLogWithProjection);
  }

  @Test
  public void testFilesTableTimeTravelWithSchemaEvolution() throws Exception {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark
        .createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    table.updateSchema().addColumn("category", Types.StringType.get()).commit();

    List<Row> newRecords =
        Lists.newArrayList(RowFactory.create(3, "b", "c"), RowFactory.create(4, "b", "c"));

    StructType newSparkSchema =
        SparkSchemaUtil.convert(
            new Schema(
                optional(1, "id", Types.IntegerType.get()),
                optional(2, "data", Types.StringType.get()),
                optional(3, "category", Types.StringType.get())));

    spark.createDataFrame(newRecords, newSparkSchema).coalesce(1).writeTo(tableName).append();

    Long currentSnapshotId = table.currentSnapshot().snapshotId();

    Dataset<Row> actualFilesDs =
        spark.sql(
            "SELECT * FROM "
                + tableName
                + ".files VERSION AS OF "
                + currentSnapshotId
                + " ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();
    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    List<Record> expectedFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);

    Assert.assertEquals("actualFiles size should be 2", 2, actualFiles.size());

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));

    Assert.assertEquals(
        "expectedFiles and actualFiles size should be the same",
        actualFiles.size(),
        expectedFiles.size());
  }

  @Test
  public void testSnapshotReferencesMetatable() throws Exception {
    // Create table and insert data
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES"
            + "('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);

    List<SimpleRecord> recordsA =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "a"));
    spark
        .createDataset(recordsA, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    List<SimpleRecord> recordsB =
        Lists.newArrayList(new SimpleRecord(1, "b"), new SimpleRecord(2, "b"));
    spark
        .createDataset(recordsB, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    Long currentSnapshotId = table.currentSnapshot().snapshotId();

    // Create branch
    table
        .manageSnapshots()
        .createBranch("testBranch", currentSnapshotId)
        .setMaxRefAgeMs("testBranch", 10)
        .setMinSnapshotsToKeep("testBranch", 20)
        .setMaxSnapshotAgeMs("testBranch", 30)
        .commit();
    // Create Tag
    table
        .manageSnapshots()
        .createTag("testTag", currentSnapshotId)
        .setMaxRefAgeMs("testTag", 50)
        .commit();
    // Check refs table
    List<Row> references = spark.sql("SELECT * FROM " + tableName + ".refs").collectAsList();
    Assert.assertEquals("Refs table should return 3 rows", 3, references.size());
    List<Row> branches =
        spark.sql("SELECT * FROM " + tableName + ".refs WHERE type='BRANCH'").collectAsList();
    Assert.assertEquals("Refs table should return 2 branches", 2, branches.size());
    List<Row> tags =
        spark.sql("SELECT * FROM " + tableName + ".refs WHERE type='TAG'").collectAsList();
    Assert.assertEquals("Refs table should return 1 tag", 1, tags.size());

    // Check branch entries in refs table
    List<Row> mainBranch =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'main' AND type='BRANCH'")
            .collectAsList();
    Assert.assertEquals("main", mainBranch.get(0).getAs("name"));
    Assert.assertEquals("BRANCH", mainBranch.get(0).getAs("type"));
    Assert.assertEquals(currentSnapshotId, mainBranch.get(0).getAs("snapshot_id"));

    List<Row> testBranch =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'testBranch' AND type='BRANCH'")
            .collectAsList();
    Assert.assertEquals("testBranch", testBranch.get(0).getAs("name"));
    Assert.assertEquals("BRANCH", testBranch.get(0).getAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranch.get(0).getAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(10), testBranch.get(0).getAs("max_reference_age_in_ms"));
    Assert.assertEquals(Integer.valueOf(20), testBranch.get(0).getAs("min_snapshots_to_keep"));
    Assert.assertEquals(Long.valueOf(30), testBranch.get(0).getAs("max_snapshot_age_in_ms"));

    // Check tag entries in refs table
    List<Row> testTag =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'testTag' AND type='TAG'")
            .collectAsList();
    Assert.assertEquals("testTag", testTag.get(0).getAs("name"));
    Assert.assertEquals("TAG", testTag.get(0).getAs("type"));
    Assert.assertEquals(currentSnapshotId, testTag.get(0).getAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(50), testTag.get(0).getAs("max_reference_age_in_ms"));

    // Check projection in refs table
    List<Row> testTagProjection =
        spark
            .sql(
                "SELECT name,type,snapshot_id,max_reference_age_in_ms,min_snapshots_to_keep FROM "
                    + tableName
                    + ".refs where type='TAG'")
            .collectAsList();
    Assert.assertEquals("testTag", testTagProjection.get(0).getAs("name"));
    Assert.assertEquals("TAG", testTagProjection.get(0).getAs("type"));
    Assert.assertEquals(currentSnapshotId, testTagProjection.get(0).getAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(50), testTagProjection.get(0).getAs("max_reference_age_in_ms"));
    Assert.assertNull(testTagProjection.get(0).getAs("min_snapshots_to_keep"));

    List<Row> mainBranchProjection =
        spark
            .sql(
                "SELECT name, type FROM "
                    + tableName
                    + ".refs WHERE name = 'main' AND type = 'BRANCH'")
            .collectAsList();
    Assert.assertEquals("main", mainBranchProjection.get(0).getAs("name"));
    Assert.assertEquals("BRANCH", mainBranchProjection.get(0).getAs("type"));

    List<Row> testBranchProjection =
        spark
            .sql(
                "SELECT type, name, max_reference_age_in_ms, snapshot_id FROM "
                    + tableName
                    + ".refs WHERE name = 'testBranch' AND type = 'BRANCH'")
            .collectAsList();
    Assert.assertEquals("testBranch", testBranchProjection.get(0).getAs("name"));
    Assert.assertEquals("BRANCH", testBranchProjection.get(0).getAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranchProjection.get(0).getAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(10), testBranchProjection.get(0).getAs("max_reference_age_in_ms"));
  }

  /**
   * Find matching manifest entries of an Iceberg table
   *
   * @param table iceberg table
   * @param expectedContent file content to populate on entries
   * @param entriesTableSchema schema of Manifest entries
   * @param manifestsToExplore manifests to explore of the table
   * @param partValue partition value that manifest entries must match, or null to skip filtering
   */
  private List<Record> expectedEntries(
      Table table,
      FileContent expectedContent,
      Schema entriesTableSchema,
      List<ManifestFile> manifestsToExplore,
      String partValue)
      throws IOException {
    List<Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<Record> rows = Avro.read(in).project(entriesTableSchema).build()) {
        for (Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            Record file = (Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              TestHelpers.asMetadataRecord(file, expectedContent);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  private boolean partitionMatch(Record file, String partValue) {
    if (partValue == null) {
      return true;
    }
    Record partition = (Record) file.get(4);
    return partValue.equals(partition.get(0).toString());
  }
}
