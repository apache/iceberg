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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.collections.ListUtils;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.HistoryEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ParameterizedTestExtension;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetadataTables extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
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
    assertThat(expectedDataManifests).as("Should have 1 data manifest").hasSize(1);
    assertThat(expectedDeleteManifests).as("Should have 1 delete manifest").hasSize(1);

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".files").schema();

    // check delete files table
    Dataset<Row> actualDeleteFilesDs = spark.sql("SELECT * FROM " + tableName + ".delete_files");
    List<Row> actualDeleteFiles = TestHelpers.selectNonDerived(actualDeleteFilesDs).collectAsList();
    assertThat(actualDeleteFiles).as("Metadata table should return one delete file").hasSize(1);

    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    assertThat(expectedDeleteFiles).as("Should be one delete file manifest entry").hasSize(1);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // check data files table
    Dataset<Row> actualDataFilesDs = spark.sql("SELECT * FROM " + tableName + ".data_files");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    assertThat(actualDataFiles).as("Metadata table should return one data file").hasSize(1);

    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    assertThat(expectedDataFiles).as("Should be one data file manifest entry").hasSize(1);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    // check all files table
    Dataset<Row> actualFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".files ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();

    assertThat(actualFiles).as("Metadata table should return two files").hasSize(2);

    List<Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    assertThat(expectedFiles).as("Should have two files manifest entries").hasSize(2);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));
  }

  @TestTemplate
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
    assertThat(expectedDataManifests).as("Should have 2 data manifest").hasSize(2);
    assertThat(expectedDeleteManifests).as("Should have 2 delete manifest").hasSize(2);

    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".delete_files").schema();

    // Check delete files table
    List<Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.POSITION_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    assertThat(expectedDeleteFiles).as("Should have one delete file manifest entry").hasSize(1);

    Dataset<Row> actualDeleteFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".delete_files " + "WHERE partition.data='a'");
    List<Row> actualDeleteFiles = actualDeleteFilesDs.collectAsList();

    assertThat(actualDeleteFiles).as("Metadata table should return one delete file").hasSize(1);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDeleteFilesDs),
        expectedDeleteFiles.get(0),
        actualDeleteFiles.get(0));

    // Check data files table
    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    assertThat(expectedDataFiles).as("Should have one data file manifest entry").hasSize(1);

    Dataset<Row> actualDataFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".data_files " + "WHERE partition.data='a'");

    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    assertThat(actualDataFiles).as("Metadata table should return one data file").hasSize(1);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs),
        expectedDataFiles.get(0),
        actualDataFiles.get(0));

    List<Row> actualPartitionsWithProjection =
        spark.sql("SELECT file_count FROM " + tableName + ".partitions ").collectAsList();
    assertThat(actualPartitionsWithProjection)
        .as("Metadata table should return two partitions record")
        .hasSize(2)
        .containsExactly(RowFactory.create(1), RowFactory.create(1));

    // Check files table
    List<Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    assertThat(expectedFiles).as("Should have two file manifest entries").hasSize(2);

    Dataset<Row> actualFilesDs =
        spark.sql(
            "SELECT * FROM " + tableName + ".files " + "WHERE partition.data='a' ORDER BY content");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();
    assertThat(actualFiles).as("Metadata table should return two files").hasSize(2);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));
  }

  @TestTemplate
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
    List<ManifestFile> expectedDataManifests = TestHelpers.allDataManifests(table);
    assertThat(expectedDataManifests).as("Should have 2 data manifests").hasSize(2);
    List<ManifestFile> expectedDeleteManifests = TestHelpers.allDeleteManifests(table);
    assertThat(expectedDeleteManifests).as("Should have 1 delete manifest").hasSize(1);

    // Clear table to test whether 'all_files' can read past files
    List<Object[]> results = sql("DELETE FROM %s", tableName);
    assertThat(results).as("Table should be cleared").isEmpty();

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".all_data_files").schema();

    // Check all data files table
    Dataset<Row> actualDataFilesDs = spark.sql("SELECT * FROM " + tableName + ".all_data_files");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();

    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    assertThat(expectedDataFiles).as("Should be two data file manifest entries").hasSize(2);
    assertThat(actualDataFiles).as("Metadata table should return two data files").hasSize(2);
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
    assertThat(expectedDeleteFiles).as("Should be one delete file manifest entry").hasSize(1);
    assertThat(actualDeleteFiles).as("Metadata table should return one delete file").hasSize(1);
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
    assertThat(actualFiles).as("Metadata table should return three files").hasSize(3);
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles, actualFiles);
  }

  @TestTemplate
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
    List<ManifestFile> expectedDataManifests = TestHelpers.allDataManifests(table);
    assertThat(expectedDataManifests).as("Should have 5 data manifests").hasSize(5); // 3+2
    List<ManifestFile> expectedDeleteManifests = TestHelpers.allDeleteManifests(table);
    assertThat(expectedDeleteManifests).as("Should have 1 delete manifest").hasSize(1); // 1

    // Clear table to test whether 'all_files' can read past files
    List<Object[]> results = sql("DELETE FROM %s", tableName);
    assertThat(results).as("Table should be cleared").isEmpty();

    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    Schema filesTableSchema =
        Spark3Util.loadIcebergTable(spark, tableName + ".all_data_files").schema();

    // Check all data files table
    Dataset<Row> actualDataFilesDs =
        spark.sql("SELECT * FROM " + tableName + ".all_data_files " + "WHERE partition.data='a'");
    List<Row> actualDataFiles = TestHelpers.selectNonDerived(actualDataFilesDs).collectAsList();
    List<Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    assertThat(expectedDataFiles).as("Should be three data file manifest entries").hasSize(3);
    assertThat(actualDataFiles).as("Metadata table should return three data files").hasSize(3);
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
    assertThat(expectedDeleteFiles).as("Should be one data file manifest entry").hasSize(1);
    assertThat(actualDeleteFiles).as("Metadata table should return one data file").hasSize(1);

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
    assertThat(actualFiles).as("Metadata table should return four files").hasSize(4); // 3+1
    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualDataFilesDs), expectedFiles, actualFiles);
  }

  @TestTemplate
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
    assertThat(metadataLogWithFilters)
        .as("metadataLogEntries table should return 1 row")
        .hasSize(1);
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
    assertThat(metadataLogWithProjection)
        .as("metadataLogEntries table should return 3 rows")
        .hasSize(3);
    assertEquals(
        "metadataLog entry should be of same file",
        metadataFiles.stream().map(this::row).collect(Collectors.toList()),
        metadataLogWithProjection);
  }

  @TestTemplate
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
                + " ORDER BY file_path");
    List<Row> actualFiles = TestHelpers.selectNonDerived(actualFilesDs).collectAsList();
    Schema entriesTableSchema = Spark3Util.loadIcebergTable(spark, tableName + ".entries").schema();
    List<ManifestFile> expectedDataManifests = TestHelpers.dataManifests(table);
    List<Record> expectedFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    expectedFiles.sort(Comparator.comparing(r -> r.get("file_path").toString()));

    assertThat(actualFiles).as("actualFiles size should be 2").hasSize(2);

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(0), actualFiles.get(0));

    TestHelpers.assertEqualsSafe(
        TestHelpers.nonDerivedSchema(actualFilesDs), expectedFiles.get(1), actualFiles.get(1));

    assertThat(actualFiles)
        .as("expectedFiles and actualFiles size should be the same")
        .hasSameSizeAs(expectedFiles);
  }

  @TestTemplate
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
    assertThat(references).as("Refs table should return 3 rows").hasSize(3);
    List<Row> branches =
        spark.sql("SELECT * FROM " + tableName + ".refs WHERE type='BRANCH'").collectAsList();
    assertThat(branches).as("Refs table should return 2 branches").hasSize(2);
    List<Row> tags =
        spark.sql("SELECT * FROM " + tableName + ".refs WHERE type='TAG'").collectAsList();
    assertThat(tags).as("Refs table should return 1 tag").hasSize(1);

    // Check branch entries in refs table
    List<Row> mainBranch =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'main' AND type='BRANCH'")
            .collectAsList();
    assertThat(mainBranch)
        .hasSize(1)
        .containsExactly(RowFactory.create("main", "BRANCH", currentSnapshotId, null, null, null));
    assertThat(mainBranch.get(0).schema().fieldNames())
        .containsExactly(
            "name",
            "type",
            "snapshot_id",
            "max_reference_age_in_ms",
            "min_snapshots_to_keep",
            "max_snapshot_age_in_ms");

    List<Row> testBranch =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'testBranch' AND type='BRANCH'")
            .collectAsList();
    assertThat(testBranch)
        .hasSize(1)
        .containsExactly(
            RowFactory.create("testBranch", "BRANCH", currentSnapshotId, 10L, 20L, 30L));
    assertThat(testBranch.get(0).schema().fieldNames())
        .containsExactly(
            "name",
            "type",
            "snapshot_id",
            "max_reference_age_in_ms",
            "min_snapshots_to_keep",
            "max_snapshot_age_in_ms");

    // Check tag entries in refs table
    List<Row> testTag =
        spark
            .sql("SELECT * FROM " + tableName + ".refs WHERE name = 'testTag' AND type='TAG'")
            .collectAsList();
    assertThat(testTag)
        .hasSize(1)
        .containsExactly(RowFactory.create("testTag", "TAG", currentSnapshotId, 50L, null, null));
    assertThat(testTag.get(0).schema().fieldNames())
        .containsExactly(
            "name",
            "type",
            "snapshot_id",
            "max_reference_age_in_ms",
            "min_snapshots_to_keep",
            "max_snapshot_age_in_ms");

    // Check projection in refs table
    List<Row> testTagProjection =
        spark
            .sql(
                "SELECT name,type,snapshot_id,max_reference_age_in_ms,min_snapshots_to_keep FROM "
                    + tableName
                    + ".refs where type='TAG'")
            .collectAsList();
    assertThat(testTagProjection)
        .hasSize(1)
        .containsExactly(RowFactory.create("testTag", "TAG", currentSnapshotId, 50L, null));
    assertThat(testTagProjection.get(0).schema().fieldNames())
        .containsExactly(
            "name", "type", "snapshot_id", "max_reference_age_in_ms", "min_snapshots_to_keep");

    List<Row> mainBranchProjection =
        spark
            .sql(
                "SELECT name, type FROM "
                    + tableName
                    + ".refs WHERE name = 'main' AND type = 'BRANCH'")
            .collectAsList();
    assertThat(mainBranchProjection)
        .hasSize(1)
        .containsExactly(RowFactory.create("main", "BRANCH"));
    assertThat(mainBranchProjection.get(0).schema().fieldNames()).containsExactly("name", "type");

    List<Row> testBranchProjection =
        spark
            .sql(
                "SELECT name, type, snapshot_id, max_reference_age_in_ms FROM "
                    + tableName
                    + ".refs WHERE name = 'testBranch' AND type = 'BRANCH'")
            .collectAsList();
    assertThat(testBranchProjection)
        .hasSize(1)
        .containsExactly(RowFactory.create("testBranch", "BRANCH", currentSnapshotId, 10L));
    assertThat(testBranchProjection.get(0).schema().fieldNames())
        .containsExactly("name", "type", "snapshot_id", "max_reference_age_in_ms");
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

  @TestTemplate
  public void metadataLogEntriesAfterReplacingTable() throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES "
            + "('format-version'='2')",
        tableName);

    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    assertThat(tableMetadata.snapshots()).isEmpty();
    assertThat(tableMetadata.snapshotLog()).isEmpty();
    assertThat(tableMetadata.currentSnapshot()).isNull();

    Object[] firstEntry =
        row(
            DateTimeUtils.toJavaTimestamp(tableMetadata.lastUpdatedMillis() * 1000),
            tableMetadata.metadataFileLocation(),
            null,
            null,
            null);

    assertThat(sql("SELECT * FROM %s.metadata_log_entries", tableName)).containsExactly(firstEntry);

    sql("INSERT INTO %s (id, data) VALUES (1, 'a')", tableName);

    tableMetadata = ((HasTableOperations) table).operations().refresh();
    assertThat(tableMetadata.snapshots()).hasSize(1);
    assertThat(tableMetadata.snapshotLog()).hasSize(1);
    Snapshot currentSnapshot = tableMetadata.currentSnapshot();

    Object[] secondEntry =
        row(
            DateTimeUtils.toJavaTimestamp(tableMetadata.lastUpdatedMillis() * 1000),
            tableMetadata.metadataFileLocation(),
            currentSnapshot.snapshotId(),
            currentSnapshot.schemaId(),
            currentSnapshot.sequenceNumber());

    assertThat(sql("SELECT * FROM %s.metadata_log_entries", tableName))
        .containsExactly(firstEntry, secondEntry);

    sql("INSERT INTO %s (id, data) VALUES (1, 'a')", tableName);

    tableMetadata = ((HasTableOperations) table).operations().refresh();
    assertThat(tableMetadata.snapshots()).hasSize(2);
    assertThat(tableMetadata.snapshotLog()).hasSize(2);
    currentSnapshot = tableMetadata.currentSnapshot();

    Object[] thirdEntry =
        row(
            DateTimeUtils.toJavaTimestamp(tableMetadata.lastUpdatedMillis() * 1000),
            tableMetadata.metadataFileLocation(),
            currentSnapshot.snapshotId(),
            currentSnapshot.schemaId(),
            currentSnapshot.sequenceNumber());

    assertThat(sql("SELECT * FROM %s.metadata_log_entries", tableName))
        .containsExactly(firstEntry, secondEntry, thirdEntry);

    sql(
        "CREATE OR REPLACE TABLE %s (id bigint, data string) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES "
            + "('format-version'='2')",
        tableName);

    tableMetadata = ((HasTableOperations) table).operations().refresh();
    assertThat(tableMetadata.snapshots()).hasSize(2);
    assertThat(tableMetadata.snapshotLog()).hasSize(2);

    // currentSnapshot is null but the metadata_log_entries will refer to the last snapshot from the
    // snapshotLog
    assertThat(tableMetadata.currentSnapshot()).isNull();
    HistoryEntry historyEntry = tableMetadata.snapshotLog().get(1);
    Snapshot lastSnapshot = tableMetadata.snapshot(historyEntry.snapshotId());

    Object[] fourthEntry =
        row(
            DateTimeUtils.toJavaTimestamp(tableMetadata.lastUpdatedMillis() * 1000),
            tableMetadata.metadataFileLocation(),
            lastSnapshot.snapshotId(),
            lastSnapshot.schemaId(),
            lastSnapshot.sequenceNumber());

    assertThat(sql("SELECT * FROM %s.metadata_log_entries", tableName))
        .containsExactly(firstEntry, secondEntry, thirdEntry, fourthEntry);

    sql("INSERT INTO %s (id, data) VALUES (1, 'a')", tableName);

    tableMetadata = ((HasTableOperations) table).operations().refresh();
    assertThat(tableMetadata.snapshots()).hasSize(3);
    assertThat(tableMetadata.snapshotLog()).hasSize(3);
    currentSnapshot = tableMetadata.currentSnapshot();

    Object[] fifthEntry =
        row(
            DateTimeUtils.toJavaTimestamp(tableMetadata.lastUpdatedMillis() * 1000),
            tableMetadata.metadataFileLocation(),
            currentSnapshot.snapshotId(),
            currentSnapshot.schemaId(),
            currentSnapshot.sequenceNumber());

    assertThat(sql("SELECT * FROM %s.metadata_log_entries", tableName))
        .containsExactly(firstEntry, secondEntry, thirdEntry, fourthEntry, fifthEntry);
  }
}
