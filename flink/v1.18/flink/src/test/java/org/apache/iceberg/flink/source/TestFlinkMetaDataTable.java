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
package org.apache.iceberg.flink.source;

import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.avro.generic.GenericData;
import org.apache.commons.collections.ListUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.MetricsUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.FlinkCatalogTestBase;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkMetaDataTable extends FlinkCatalogTestBase {
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private static final TemporaryFolder TEMP = new TemporaryFolder();
  private final boolean isPartition;

  public TestFlinkMetaDataTable(String catalogName, Namespace baseNamespace, Boolean isPartition) {
    super(catalogName, baseNamespace);
    this.isPartition = isPartition;
  }

  @Parameterized.Parameters(name = "catalogName={0}, baseNamespace={1}, isPartition={2}")
  public static Iterable<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();

    for (Boolean isPartition : new Boolean[] {true, false}) {
      String catalogName = "testhadoop";
      Namespace baseNamespace = Namespace.of("default");
      parameters.add(new Object[] {catalogName, baseNamespace, isPartition});
    }
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    Configuration configuration = super.getTableEnv().getConfig().getConfiguration();
    configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @Before
  public void before() {
    super.before();
    sql("USE CATALOG %s", catalogName);
    sql("CREATE DATABASE %s", flinkDatabase);
    sql("USE %s", DATABASE);
    if (isPartition) {
      sql(
          "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) PARTITIONED BY (data) WITH ('format-version'='2', 'write.format.default'='%s')",
          TABLE_NAME, format.name());
      sql("INSERT INTO %s VALUES (1,'a',10),(2,'a',20)", TABLE_NAME);
      sql("INSERT INTO %s VALUES (1,'b',10),(2,'b',20)", TABLE_NAME);
    } else {
      sql(
          "CREATE TABLE %s (id INT, data VARCHAR,d DOUBLE) WITH ('format-version'='2', 'write.format.default'='%s')",
          TABLE_NAME, format.name());
      sql(
          "INSERT INTO %s VALUES (1,'iceberg',10),(2,'b',20),(3,CAST(NULL AS VARCHAR),30)",
          TABLE_NAME);
      sql("INSERT INTO %s VALUES (4,'iceberg',10)", TABLE_NAME);
    }
  }

  @Override
  @After
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
  }

  @Test
  public void testSnapshots() {
    String sql = String.format("SELECT * FROM %s$snapshots ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      Assert.assertEquals(
          "Should have expected timestamp",
          ((Instant) row.getField(0)).toEpochMilli(),
          next.timestampMillis());
      Assert.assertEquals("Should have expected snapshot id", next.snapshotId(), row.getField(1));
      Assert.assertEquals("Should have expected parent id", next.parentId(), row.getField(2));
      Assert.assertEquals("Should have expected operation", next.operation(), row.getField(3));
      Assert.assertEquals(
          "Should have expected manifest list location",
          row.getField(4),
          next.manifestListLocation());
      Assert.assertEquals("Should have expected summary", next.summary(), row.getField(5));
    }
  }

  @Test
  public void testHistory() {
    String sql = String.format("SELECT * FROM %s$history ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      Assert.assertEquals(
          "Should have expected made_current_at",
          ((Instant) row.getField(0)).toEpochMilli(),
          next.timestampMillis());
      Assert.assertEquals("Should have expected snapshot id", next.snapshotId(), row.getField(1));
      Assert.assertEquals("Should have expected parent id", next.parentId(), row.getField(2));

      Assert.assertEquals(
          "Should have expected is current ancestor",
          SnapshotUtil.isAncestorOf(table, table.currentSnapshot().snapshotId(), next.snapshotId()),
          row.getField(3));
    }
  }

  @Test
  public void testManifests() {
    String sql = String.format("SELECT * FROM %s$manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    List<ManifestFile> expectedDataManifests = dataManifests(table);

    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      Assert.assertEquals(
          "Should have expected content", manifestFile.content().id(), row.getField(0));
      Assert.assertEquals("Should have expected path", manifestFile.path(), row.getField(1));
      Assert.assertEquals("Should have expected length", manifestFile.length(), row.getField(2));
      Assert.assertEquals(
          "Should have expected partition_spec_id",
          manifestFile.partitionSpecId(),
          row.getField(3));
      Assert.assertEquals(
          "Should have expected added_snapshot_id", manifestFile.snapshotId(), row.getField(4));
      Assert.assertEquals(
          "Should have expected added_data_files_count",
          manifestFile.addedFilesCount(),
          row.getField(5));
      Assert.assertEquals(
          "Should have expected existing_data_files_count",
          manifestFile.existingFilesCount(),
          row.getField(6));
      Assert.assertEquals(
          "Should have expected deleted_data_files_count",
          manifestFile.deletedFilesCount(),
          row.getField(7));
    }
  }

  @Test
  public void testAllManifests() {
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    String sql = String.format("SELECT * FROM %s$all_manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    List<ManifestFile> expectedDataManifests = allDataManifests(table);

    Assert.assertEquals(expectedDataManifests.size(), result.size());
    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      Assert.assertEquals(
          "Should have expected content", manifestFile.content().id(), row.getField(0));
      Assert.assertEquals("Should have expected path", manifestFile.path(), row.getField(1));
      Assert.assertEquals("Should have expected length", manifestFile.length(), row.getField(2));
      Assert.assertEquals(
          "Should have expected partition_spec_id",
          manifestFile.partitionSpecId(),
          row.getField(3));
      Assert.assertEquals(
          "Should have expected added_snapshot_id", manifestFile.snapshotId(), row.getField(4));
      Assert.assertEquals(
          "Should have expected added_data_files_count",
          manifestFile.addedFilesCount(),
          row.getField(5));
      Assert.assertEquals(
          "Should have expected existing_data_files_count",
          manifestFile.existingFilesCount(),
          row.getField(6));
      Assert.assertEquals(
          "Should have expected deleted_data_files_count",
          manifestFile.deletedFilesCount(),
          row.getField(7));
    }
  }

  @Test
  public void testUnPartitionedTable() throws IOException {
    Assume.assumeFalse(isPartition);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(dataDelete.copy("id", 1));

    TEMP.create();
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(TEMP.newFile()), dataDeletes, deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    Assert.assertEquals("Should have 2 data manifest", 2, expectedDataManifests.size());
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();

    // check delete files table
    Schema deleteFilesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(
                table, MetadataTableType.from("delete_files"))
            .schema();

    List<String> deleteColumns =
        deleteFilesTableSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(c -> !c.equals(MetricsUtil.READABLE_METRICS))
            .collect(Collectors.toList());
    String deleteNames =
        deleteColumns.stream().map(n -> "`" + n + "`").collect(Collectors.joining(","));

    deleteFilesTableSchema = deleteFilesTableSchema.select(deleteColumns);

    List<Row> actualDeleteFiles = sql("SELECT %s FROM %s$delete_files", deleteNames, TABLE_NAME);
    Assert.assertEquals("Metadata table should return 1 delete file", 1, actualDeleteFiles.size());

    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be 1 delete file manifest entry", 1, expectedDeleteFiles.size());
    TestHelpers.assertEquals(
        deleteFilesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check data files table
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("files"))
            .schema();

    List<String> columns =
        filesTableSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(c -> !c.equals(MetricsUtil.READABLE_METRICS))
            .collect(Collectors.toList());
    String names = columns.stream().map(n -> "`" + n + "`").collect(Collectors.joining(","));

    filesTableSchema = filesTableSchema.select(columns);

    List<Row> actualDataFiles = sql("SELECT %s FROM %s$data_files", names, TABLE_NAME);
    Assert.assertEquals("Metadata table should return 2 data file", 2, actualDataFiles.size());

    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be 2 data file manifest entry", 2, expectedDataFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // check all files table
    List<Row> actualFiles = sql("SELECT %s FROM %s$files ORDER BY content", names, TABLE_NAME);
    Assert.assertEquals("Metadata table should return 3 files", 3, actualFiles.size());

    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have 3 files manifest entries", 3, expectedFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testPartitionedTable() throws Exception {
    Assume.assumeFalse(!isPartition);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    TEMP.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    deleteRow.put("data", "a");
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(TEMP.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    deleteRow.put("data", "b");
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(TEMP.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes2).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    Assert.assertEquals("Should have 2 delete manifests", 2, expectedDeleteManifests.size());

    Table deleteFilesTable =
        MetadataTableUtils.createMetadataTableInstance(
            table, MetadataTableType.from("delete_files"));
    Schema filesTableSchema = deleteFilesTable.schema();

    List<String> columns =
        filesTableSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(c -> !c.equals(MetricsUtil.READABLE_METRICS))
            .collect(Collectors.toList());
    String names = columns.stream().map(n -> "`" + n + "`").collect(Collectors.joining(","));

    filesTableSchema = filesTableSchema.select(columns);

    // Check delete files table
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals(
        "Should have one delete file manifest entry", 1, expectedDeleteFiles.size());

    List<Row> actualDeleteFiles =
        sql("SELECT %s FROM %s$delete_files WHERE `partition`.`data`='a'", names, TABLE_NAME);

    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEquals(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check data files table
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should have one data file manifest entry", 1, expectedDataFiles.size());

    List<Row> actualDataFiles =
        sql("SELECT %s FROM %s$data_files  WHERE `partition`.`data`='a'", names, TABLE_NAME);
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    List<Row> actualPartitionsWithProjection =
        sql("SELECT file_count FROM %s$partitions ", TABLE_NAME);
    Assert.assertEquals(
        "Metadata table should return two partitions record",
        2,
        actualPartitionsWithProjection.size());
    for (int i = 0; i < 2; ++i) {
      Assert.assertEquals(1, actualPartitionsWithProjection.get(i).getField(0));
    }

    // Check files table
    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    Assert.assertEquals("Should have two file manifest entries", 2, expectedFiles.size());

    List<Row> actualFiles =
        sql(
            "SELECT %s FROM %s$files WHERE `partition`.`data`='a' ORDER BY content",
            names, TABLE_NAME);
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @Test
  public void testAllFilesUnpartitioned() throws Exception {
    Assume.assumeFalse(isPartition);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    TEMP.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(TEMP.newFile()),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    Assert.assertEquals("Should have 2 data manifest", 2, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(
                table, MetadataTableType.from("all_data_files"))
            .schema();

    List<String> columns =
        filesTableSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(c -> !c.equals(MetricsUtil.READABLE_METRICS))
            .collect(Collectors.toList());
    String names = columns.stream().map(n -> "`" + n + "`").collect(Collectors.joining(","));

    filesTableSchema = filesTableSchema.select(columns);

    // Check all data files table
    List<Row> actualDataFiles =
        sql("SELECT %s FROM %s$all_data_files order by record_count ", names, TABLE_NAME);

    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    Assert.assertEquals("Should be 2 data file manifest entry", 2, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return 2 data file", 2, actualDataFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles, actualDataFiles);

    // Check all delete files table
    List<Row> actualDeleteFiles = sql("SELECT %s FROM %s$all_delete_files", names, TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    Assert.assertEquals("Should be one delete file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals(
        "Metadata table should return one delete file", 1, actualDeleteFiles.size());
    TestHelpers.assertEquals(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check all files table
    List<Row> actualFiles =
        sql("SELECT %s FROM %s$all_files ORDER BY content, record_count asc", names, TABLE_NAME);
    List<GenericData.Record> expectedFiles =
        ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return 3 files", 3, actualFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedFiles, actualFiles);
  }

  @Test
  public void testAllFilesPartitioned() throws Exception {
    Assume.assumeFalse(!isPartition);
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    // Create delete file
    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    TEMP.create();

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(TEMP.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(TEMP.newFile()),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).addDeletes(eqDeletes2).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    Assert.assertEquals("Should have 2 data manifests", 2, expectedDataManifests.size());
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);
    Assert.assertEquals("Should have 1 delete manifest", 1, expectedDeleteManifests.size());

    // Clear table to test whether 'all_files' can read past files
    table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();
    Schema filesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(
                table, MetadataTableType.from("all_data_files"))
            .schema();

    List<String> columns =
        filesTableSchema.columns().stream()
            .map(Types.NestedField::name)
            .filter(c -> !c.equals(MetricsUtil.READABLE_METRICS))
            .collect(Collectors.toList());
    String names = columns.stream().map(n -> "`" + n + "`").collect(Collectors.joining(","));

    filesTableSchema = filesTableSchema.select(columns);

    // Check all data files table
    List<Row> actualDataFiles =
        sql("SELECT %s FROM %s$all_data_files WHERE `partition`.`data`='a'", names, TABLE_NAME);
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDataFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDataFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // Check all delete files table
    List<Row> actualDeleteFiles =
        sql("SELECT %s FROM %s$all_delete_files WHERE `partition`.`data`='a'", names, TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    Assert.assertEquals("Should be one data file manifest entry", 1, expectedDeleteFiles.size());
    Assert.assertEquals("Metadata table should return one data file", 1, actualDeleteFiles.size());

    TestHelpers.assertEquals(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check all files table
    List<Row> actualFiles =
        sql(
            "SELECT %s FROM %s$all_files WHERE `partition`.`data`='a' ORDER BY content",
            names, TABLE_NAME);
    List<GenericData.Record> expectedFiles =
        ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> ((Integer) r.get("content"))));
    Assert.assertEquals("Metadata table should return two files", 2, actualFiles.size());
    TestHelpers.assertEquals(filesTableSchema, expectedFiles, actualFiles);
  }

  @Test
  public void testMetadataLogEntries() {
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Long currentSnapshotId = table.currentSnapshot().snapshotId();
    TableMetadata tableMetadata = ((HasTableOperations) table).operations().current();
    Snapshot currentSnapshot = tableMetadata.currentSnapshot();
    Snapshot parentSnapshot = table.snapshot(currentSnapshot.parentId());
    List<TableMetadata.MetadataLogEntry> metadataLogEntries =
        Lists.newArrayList(tableMetadata.previousFiles());

    // Check metadataLog table
    List<Row> metadataLogs = sql("SELECT * FROM %s$metadata_log_entries", TABLE_NAME);

    Assert.assertEquals("metadataLogEntries table should return 3 row", 3, metadataLogs.size());
    Row metadataLog = metadataLogs.get(0);
    Assert.assertEquals(
        Instant.ofEpochMilli(metadataLogEntries.get(0).timestampMillis()),
        metadataLog.getField("timestamp"));
    Assert.assertEquals(metadataLogEntries.get(0).file(), metadataLog.getField("file"));
    Assert.assertNull(metadataLog.getField("latest_snapshot_id"));
    Assert.assertNull(metadataLog.getField("latest_schema_id"));
    Assert.assertNull(metadataLog.getField("latest_sequence_number"));

    metadataLog = metadataLogs.get(1);
    Assert.assertEquals(
        Instant.ofEpochMilli(metadataLogEntries.get(1).timestampMillis()),
        metadataLog.getField("timestamp"));
    Assert.assertEquals(metadataLogEntries.get(1).file(), metadataLog.getField("file"));
    Assert.assertEquals(parentSnapshot.snapshotId(), metadataLog.getField("latest_snapshot_id"));
    Assert.assertEquals(parentSnapshot.schemaId(), metadataLog.getField("latest_schema_id"));
    Assert.assertEquals(
        parentSnapshot.sequenceNumber(), metadataLog.getField("latest_sequence_number"));

    metadataLog = metadataLogs.get(2);
    Assert.assertEquals(
        Instant.ofEpochMilli(currentSnapshot.timestampMillis()), metadataLog.getField("timestamp"));
    Assert.assertEquals(tableMetadata.metadataFileLocation(), metadataLog.getField("file"));
    Assert.assertEquals(currentSnapshot.snapshotId(), metadataLog.getField("latest_snapshot_id"));
    Assert.assertEquals(currentSnapshot.schemaId(), metadataLog.getField("latest_schema_id"));
    Assert.assertEquals(
        currentSnapshot.sequenceNumber(), metadataLog.getField("latest_sequence_number"));

    // test filtering
    List<Row> metadataLogWithFilters =
        sql(
            "SELECT * FROM %s$metadata_log_entries WHERE latest_snapshot_id = %s",
            TABLE_NAME, currentSnapshotId);
    Assert.assertEquals(
        "metadataLogEntries table should return 1 row", 1, metadataLogWithFilters.size());

    metadataLog = metadataLogWithFilters.get(0);
    Assert.assertEquals(
        Instant.ofEpochMilli(tableMetadata.currentSnapshot().timestampMillis()),
        metadataLog.getField("timestamp"));
    Assert.assertEquals(tableMetadata.metadataFileLocation(), metadataLog.getField("file"));
    Assert.assertEquals(
        tableMetadata.currentSnapshot().snapshotId(), metadataLog.getField("latest_snapshot_id"));
    Assert.assertEquals(
        tableMetadata.currentSnapshot().schemaId(), metadataLog.getField("latest_schema_id"));
    Assert.assertEquals(
        tableMetadata.currentSnapshot().sequenceNumber(),
        metadataLog.getField("latest_sequence_number"));

    // test projection
    List<String> metadataFiles =
        metadataLogEntries.stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toList());
    metadataFiles.add(tableMetadata.metadataFileLocation());
    List<Row> metadataLogWithProjection =
        sql("SELECT file FROM %s$metadata_log_entries", TABLE_NAME);
    Assert.assertEquals(
        "metadataLogEntries table should return 3 rows", 3, metadataLogWithProjection.size());
    for (int i = 0; i < metadataFiles.size(); i++) {
      Assert.assertEquals(metadataFiles.get(i), metadataLogWithProjection.get(i).getField("file"));
    }
  }

  @Test
  public void testSnapshotReferencesMetatable() {
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

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
    List<Row> references = sql("SELECT * FROM %s$refs", TABLE_NAME);
    Assert.assertEquals("Refs table should return 3 rows", 3, references.size());
    List<Row> branches = sql("SELECT * FROM %s$refs WHERE type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("Refs table should return 2 branches", 2, branches.size());
    List<Row> tags = sql("SELECT * FROM %s$refs WHERE type='TAG'", TABLE_NAME);
    Assert.assertEquals("Refs table should return 1 tag", 1, tags.size());

    // Check branch entries in refs table
    List<Row> mainBranch =
        sql("SELECT * FROM %s$refs WHERE name='main' AND type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("main", mainBranch.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", mainBranch.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, mainBranch.get(0).getFieldAs("snapshot_id"));

    List<Row> testBranch =
        sql("SELECT * FROM  %s$refs WHERE name='testBranch' AND type='BRANCH'", TABLE_NAME);
    Assert.assertEquals("testBranch", testBranch.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", testBranch.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranch.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(10), testBranch.get(0).getFieldAs("max_reference_age_in_ms"));
    Assert.assertEquals(Integer.valueOf(20), testBranch.get(0).getFieldAs("min_snapshots_to_keep"));
    Assert.assertEquals(Long.valueOf(30), testBranch.get(0).getFieldAs("max_snapshot_age_in_ms"));

    // Check tag entries in refs table
    List<Row> testTag =
        sql("SELECT * FROM %s$refs WHERE name='testTag' AND type='TAG'", TABLE_NAME);
    Assert.assertEquals("testTag", testTag.get(0).getFieldAs("name"));
    Assert.assertEquals("TAG", testTag.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testTag.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(Long.valueOf(50), testTag.get(0).getFieldAs("max_reference_age_in_ms"));

    // Check projection in refs table
    List<Row> testTagProjection =
        sql(
            "SELECT name,type,snapshot_id,max_reference_age_in_ms,min_snapshots_to_keep FROM %s$refs where type='TAG'",
            TABLE_NAME);
    Assert.assertEquals("testTag", testTagProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("TAG", testTagProjection.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testTagProjection.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(50), testTagProjection.get(0).getFieldAs("max_reference_age_in_ms"));
    Assert.assertNull(testTagProjection.get(0).getFieldAs("min_snapshots_to_keep"));

    List<Row> mainBranchProjection =
        sql("SELECT name, type FROM %s$refs WHERE name='main' AND type = 'BRANCH'", TABLE_NAME);
    Assert.assertEquals("main", mainBranchProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", mainBranchProjection.get(0).getFieldAs("type"));

    List<Row> testBranchProjection =
        sql(
            "SELECT type, name, max_reference_age_in_ms, snapshot_id FROM %s$refs WHERE name='testBranch' AND type = 'BRANCH'",
            TABLE_NAME);
    Assert.assertEquals("testBranch", testBranchProjection.get(0).getFieldAs("name"));
    Assert.assertEquals("BRANCH", testBranchProjection.get(0).getFieldAs("type"));
    Assert.assertEquals(currentSnapshotId, testBranchProjection.get(0).getFieldAs("snapshot_id"));
    Assert.assertEquals(
        Long.valueOf(10), testBranchProjection.get(0).getFieldAs("max_reference_age_in_ms"));
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
  private List<GenericData.Record> expectedEntries(
      Table table,
      FileContent expectedContent,
      Schema entriesTableSchema,
      List<ManifestFile> manifestsToExplore,
      String partValue)
      throws IOException {
    List<GenericData.Record> expected = Lists.newArrayList();
    for (ManifestFile manifest : manifestsToExplore) {
      InputFile in = table.io().newInputFile(manifest.path());
      try (CloseableIterable<GenericData.Record> rows =
          Avro.read(in).project(entriesTableSchema).build()) {
        for (GenericData.Record record : rows) {
          if ((Integer) record.get("status") < 2 /* added or existing */) {
            GenericData.Record file = (GenericData.Record) record.get("data_file");
            if (partitionMatch(file, partValue)) {
              asMetadataRecord(file, expectedContent);
              expected.add(file);
            }
          }
        }
      }
    }
    return expected;
  }

  // Populate certain fields derived in the metadata tables
  private void asMetadataRecord(GenericData.Record file, FileContent content) {
    file.put(0, content.id());
    file.put(3, 0); // specId
  }

  private boolean partitionMatch(GenericData.Record file, String partValue) {
    if (partValue == null) {
      return true;
    }
    GenericData.Record partition = (GenericData.Record) file.get(4);
    return partValue.equals(partition.get(0).toString());
  }

  private List<ManifestFile> dataManifests(Table table) {
    return table.currentSnapshot().dataManifests(table.io());
  }

  private List<ManifestFile> allDataManifests(Table table) {
    List<ManifestFile> manifests = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      manifests.addAll(snapshot.dataManifests(table.io()));
    }
    return manifests;
  }

  private List<ManifestFile> deleteManifests(Table table) {
    return table.currentSnapshot().deleteManifests(table.io());
  }
}
