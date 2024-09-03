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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
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
import org.apache.iceberg.Parameter;
import org.apache.iceberg.Parameters;
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
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestFlinkMetaDataTable extends CatalogTestBase {
  private static final String TABLE_NAME = "test_table";
  private final FileFormat format = FileFormat.AVRO;
  private @TempDir Path temp;

  @Parameter(index = 2)
  private Boolean isPartition;

  @Parameters(name = "catalogName={0}, baseNamespace={1}, isPartition={2}")
  protected static List<Object[]> parameters() {
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

  @BeforeEach
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
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    dropDatabase(flinkDatabase, true);
    super.clean();
  }

  @TestTemplate
  public void testSnapshots() {
    String sql = String.format("SELECT * FROM %s$snapshots ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      assertThat(((Instant) row.getField(0)).toEpochMilli())
          .as("Should have expected timestamp")
          .isEqualTo(next.timestampMillis());
      assertThat(next.snapshotId())
          .as("Should have expected snapshot id")
          .isEqualTo(next.snapshotId());
      assertThat(row.getField(2)).as("Should have expected parent id").isEqualTo(next.parentId());
      assertThat(row.getField(3)).as("Should have expected operation").isEqualTo(next.operation());
      assertThat(row.getField(4))
          .as("Should have expected manifest list location")
          .isEqualTo(next.manifestListLocation());
      assertThat(row.getField(5)).as("Should have expected summary").isEqualTo(next.summary());
    }
  }

  @TestTemplate
  public void testHistory() {
    String sql = String.format("SELECT * FROM %s$history ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    Iterator<Snapshot> snapshots = table.snapshots().iterator();
    for (Row row : result) {
      Snapshot next = snapshots.next();
      assertThat(((Instant) row.getField(0)).toEpochMilli())
          .as("Should have expected made_current_at")
          .isEqualTo(next.timestampMillis());
      assertThat(row.getField(1))
          .as("Should have expected snapshot id")
          .isEqualTo(next.snapshotId());
      assertThat(row.getField(2)).as("Should have expected parent id").isEqualTo(next.parentId());
      assertThat(row.getField(3))
          .as("Should have expected is current ancestor")
          .isEqualTo(
              SnapshotUtil.isAncestorOf(
                  table, table.currentSnapshot().snapshotId(), next.snapshotId()));
    }
  }

  @TestTemplate
  public void testManifests() {
    String sql = String.format("SELECT * FROM %s$manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));
    List<ManifestFile> expectedDataManifests = dataManifests(table);

    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      assertThat(row.getField(0))
          .as("Should have expected content")
          .isEqualTo(manifestFile.content().id());
      assertThat(row.getField(1)).as("Should have expected path").isEqualTo(manifestFile.path());
      assertThat(row.getField(2))
          .as("Should have expected length")
          .isEqualTo(manifestFile.length());
      assertThat(row.getField(3))
          .as("Should have expected partition_spec_id")
          .isEqualTo(manifestFile.partitionSpecId());
      assertThat(row.getField(4))
          .as("Should have expected added_snapshot_id")
          .isEqualTo(manifestFile.snapshotId());
      assertThat(row.getField(5))
          .as("Should have expected added_data_files_count")
          .isEqualTo(manifestFile.addedFilesCount());
      assertThat(row.getField(6))
          .as("Should have expected existing_data_files_count")
          .isEqualTo(manifestFile.existingFilesCount());
      assertThat(row.getField(7))
          .as("Should have expected deleted_data_files_count")
          .isEqualTo(manifestFile.deletedFilesCount());
    }
  }

  @TestTemplate
  public void testAllManifests() {
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    String sql = String.format("SELECT * FROM %s$all_manifests ", TABLE_NAME);
    List<Row> result = sql(sql);

    List<ManifestFile> expectedDataManifests = allDataManifests(table);

    assertThat(expectedDataManifests).hasSize(result.size());
    for (int i = 0; i < result.size(); i++) {
      Row row = result.get(i);
      ManifestFile manifestFile = expectedDataManifests.get(i);
      assertThat(row.getField(0))
          .as("Should have expected content")
          .isEqualTo(manifestFile.content().id());
      assertThat(row.getField(1)).as("Should have expected path").isEqualTo(manifestFile.path());
      assertThat(row.getField(2))
          .as("Should have expected length")
          .isEqualTo(manifestFile.length());
      assertThat(row.getField(3))
          .as("Should have expected partition_spec_id")
          .isEqualTo(manifestFile.partitionSpecId());
      assertThat(row.getField(4))
          .as("Should have expected added_snapshot_id")
          .isEqualTo(manifestFile.snapshotId());
      assertThat(row.getField(5))
          .as("Should have expected added_data_files_count")
          .isEqualTo(manifestFile.addedFilesCount());
      assertThat(row.getField(6))
          .as("Should have expected existing_data_files_count")
          .isEqualTo(manifestFile.existingFilesCount());
      assertThat(row.getField(7))
          .as("Should have expected deleted_data_files_count")
          .isEqualTo(manifestFile.deletedFilesCount());
    }
  }

  @TestTemplate
  public void testUnPartitionedTable() throws IOException {
    assumeThat(isPartition).isFalse();
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(dataDelete.copy("id", 1));
    File testFile = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(testFile), dataDeletes, deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    assertThat(expectedDataManifests).hasSize(2);
    assertThat(expectedDeleteManifests).hasSize(1);

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
    assertThat(actualDeleteFiles).hasSize(1);
    assertThat(expectedDeleteManifests).as("Should have 1 delete manifest").hasSize(1);

    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    assertThat(expectedDeleteFiles).as("Should be 1 delete file manifest entry").hasSize(1);
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

    List<Row> actualDataFiles =
        sql("SELECT %s FROM %s$data_files ORDER BY file_path", names, TABLE_NAME);
    assertThat(actualDataFiles).as("Metadata table should return 2 data file").hasSize(2);
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    expectedDataFiles.sort(Comparator.comparing(r -> r.get("file_path").toString()));

    assertThat(expectedDataFiles).as("Should be 2 data file manifest entry").hasSize(2);
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // check all files table
    List<Row> actualFiles = sql("SELECT %s FROM %s$files ORDER BY file_path", names, TABLE_NAME);
    assertThat(actualFiles).as("Metadata table should return 3 files").hasSize(3);
    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    expectedFiles.sort(Comparator.comparing(r -> r.get("file_path").toString()));

    assertThat(expectedFiles).as("Should have 3 files manifest entriess").hasSize(3);
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @TestTemplate
  public void testPartitionedTable() throws Exception {
    assumeThat(isPartition).isTrue();
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    deleteRow.put("data", "a");
    File testFile = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(testFile),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    deleteRow.put("data", "b");
    File testFile2 = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(testFile2),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes2).commit();

    Schema entriesTableSchema =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.from("entries"))
            .schema();

    List<ManifestFile> expectedDataManifests = dataManifests(table);
    List<ManifestFile> expectedDeleteManifests = deleteManifests(table);

    assertThat(expectedDataManifests).hasSize(2);
    assertThat(expectedDeleteManifests).hasSize(2);
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
    assertThat(expectedDeleteFiles).hasSize(1);
    List<Row> actualDeleteFiles =
        sql("SELECT %s FROM %s$delete_files WHERE `partition`.`data`='a'", names, TABLE_NAME);

    assertThat(actualDeleteFiles).hasSize(1);
    TestHelpers.assertEquals(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check data files table
    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, "a");
    assertThat(expectedDataFiles).hasSize(1);
    List<Row> actualDataFiles =
        sql("SELECT %s FROM %s$data_files  WHERE `partition`.`data`='a'", names, TABLE_NAME);
    assertThat(actualDataFiles).hasSize(1);
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    List<Row> actualPartitionsWithProjection =
        sql("SELECT file_count FROM %s$partitions ", TABLE_NAME);
    assertThat(actualPartitionsWithProjection).hasSize(2);
    for (int i = 0; i < 2; ++i) {
      assertThat(actualPartitionsWithProjection.get(i).getField(0)).isEqualTo(1);
    }

    // Check files table
    List<GenericData.Record> expectedFiles =
        Stream.concat(expectedDataFiles.stream(), expectedDeleteFiles.stream())
            .collect(Collectors.toList());
    assertThat(expectedFiles).hasSize(2);
    List<Row> actualFiles =
        sql(
            "SELECT %s FROM %s$files WHERE `partition`.`data`='a' ORDER BY content",
            names, TABLE_NAME);
    assertThat(actualFiles).hasSize(2);
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(0), actualFiles.get(0));
    TestHelpers.assertEquals(filesTableSchema, expectedFiles.get(1), actualFiles.get(1));
  }

  @TestTemplate
  public void testAllFilesUnpartitioned() throws Exception {
    assumeThat(isPartition).isFalse();
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    Schema deleteRowSchema = table.schema().select("id", "data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    File testFile = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(testFile),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).commit();

    List<ManifestFile> expectedDataManifests = allDataManifests(table);
    assertThat(expectedDataManifests).hasSize(5); // 3 + 2
    List<ManifestFile> expectedDeleteManifests = allDeleteManifests(table);
    assertThat(expectedDeleteManifests).hasSize(1);

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
        sql("SELECT %s FROM %s$all_data_files order by file_path", names, TABLE_NAME);

    List<GenericData.Record> expectedDataFiles =
        expectedEntries(table, FileContent.DATA, entriesTableSchema, expectedDataManifests, null);
    expectedDataFiles.sort(Comparator.comparing(r -> r.get("file_path").toString()));

    assertThat(expectedDataFiles).hasSize(5);
    assertThat(actualDataFiles).hasSize(5);
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles, actualDataFiles);

    // Check all delete files table
    List<Row> actualDeleteFiles = sql("SELECT %s FROM %s$all_delete_files", names, TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, null);
    assertThat(expectedDeleteFiles).hasSize(1);
    assertThat(actualDeleteFiles).hasSize(1);
    TestHelpers.assertEquals(
        filesTableSchema, expectedDeleteFiles.get(0), actualDeleteFiles.get(0));

    // Check all files table
    List<Row> actualFiles =
        sql("SELECT %s FROM %s$all_files ORDER BY file_path", names, TABLE_NAME);
    List<GenericData.Record> expectedFiles =
        ListUtils.union(expectedDataFiles, expectedDeleteFiles);
    expectedFiles.sort(Comparator.comparing(r -> r.get("file_path").toString()));
    assertThat(actualFiles).hasSize(6); // 3 + 2 + 1
    TestHelpers.assertEquals(filesTableSchema, expectedFiles, actualFiles);
  }

  @TestTemplate
  public void testAllFilesPartitioned() throws Exception {
    assumeThat(!isPartition).isFalse();
    Table table = validationCatalog.loadTable(TableIdentifier.of(icebergNamespace, TABLE_NAME));

    // Create delete file
    Schema deleteRowSchema = table.schema().select("id");
    Record dataDelete = GenericRecord.create(deleteRowSchema);

    Map<String, Object> deleteRow = Maps.newHashMap();
    deleteRow.put("id", 1);
    File testFile = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(testFile),
            org.apache.iceberg.TestHelpers.Row.of("a"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    File testFile2 = File.createTempFile("junit", null, temp.toFile());
    DeleteFile eqDeletes2 =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(testFile2),
            org.apache.iceberg.TestHelpers.Row.of("b"),
            Lists.newArrayList(dataDelete.copy(deleteRow)),
            deleteRowSchema);
    table.newRowDelta().addDeletes(eqDeletes).addDeletes(eqDeletes2).commit();

    List<ManifestFile> expectedDataManifests = allDataManifests(table);
    assertThat(expectedDataManifests).hasSize(5); // 3 + 2
    List<ManifestFile> expectedDeleteManifests = allDeleteManifests(table);
    assertThat(expectedDeleteManifests).hasSize(1);
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
    assertThat(expectedDataFiles).hasSize(3);
    assertThat(actualDataFiles).hasSize(3);
    TestHelpers.assertEquals(filesTableSchema, expectedDataFiles.get(0), actualDataFiles.get(0));

    // Check all delete files table
    List<Row> actualDeleteFiles =
        sql("SELECT %s FROM %s$all_delete_files WHERE `partition`.`data`='a'", names, TABLE_NAME);
    List<GenericData.Record> expectedDeleteFiles =
        expectedEntries(
            table, FileContent.EQUALITY_DELETES, entriesTableSchema, expectedDeleteManifests, "a");
    assertThat(expectedDeleteFiles).hasSize(1);
    assertThat(actualDeleteFiles).hasSize(1);
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
    assertThat(actualFiles).hasSize(4); // 3 + 1
    TestHelpers.assertEquals(filesTableSchema, expectedFiles, actualFiles);
  }

  @TestTemplate
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

    assertThat(metadataLogs).hasSize(3);
    Row metadataLog = metadataLogs.get(0);
    assertThat(metadataLog.getField("timestamp"))
        .isEqualTo(Instant.ofEpochMilli(metadataLogEntries.get(0).timestampMillis()));
    assertThat(metadataLog.getField("file")).isEqualTo(metadataLogEntries.get(0).file());
    assertThat(metadataLog.getField("latest_snapshot_id")).isNull();
    assertThat(metadataLog.getField("latest_schema_id")).isNull();
    assertThat(metadataLog.getField("latest_sequence_number")).isNull();

    metadataLog = metadataLogs.get(1);
    assertThat(metadataLog.getField("timestamp"))
        .isEqualTo(Instant.ofEpochMilli(metadataLogEntries.get(1).timestampMillis()));
    assertThat(metadataLog.getField("file")).isEqualTo(metadataLogEntries.get(1).file());
    assertThat(metadataLog.getField("latest_snapshot_id")).isEqualTo(parentSnapshot.snapshotId());
    assertThat(metadataLog.getField("latest_schema_id")).isEqualTo(parentSnapshot.schemaId());
    assertThat(metadataLog.getField("latest_sequence_number"))
        .isEqualTo(parentSnapshot.sequenceNumber());
    assertThat(metadataLog.getField("latest_snapshot_id")).isEqualTo(parentSnapshot.snapshotId());

    metadataLog = metadataLogs.get(2);
    assertThat(metadataLog.getField("timestamp"))
        .isEqualTo(Instant.ofEpochMilli(currentSnapshot.timestampMillis()));
    assertThat(metadataLog.getField("file")).isEqualTo(tableMetadata.metadataFileLocation());
    assertThat(metadataLog.getField("latest_snapshot_id")).isEqualTo(currentSnapshot.snapshotId());
    assertThat(metadataLog.getField("latest_schema_id")).isEqualTo(currentSnapshot.schemaId());
    assertThat(metadataLog.getField("latest_sequence_number"))
        .isEqualTo(currentSnapshot.sequenceNumber());

    // test filtering
    List<Row> metadataLogWithFilters =
        sql(
            "SELECT * FROM %s$metadata_log_entries WHERE latest_snapshot_id = %s",
            TABLE_NAME, currentSnapshotId);
    assertThat(metadataLogWithFilters).hasSize(1);
    metadataLog = metadataLogWithFilters.get(0);
    assertThat(Instant.ofEpochMilli(tableMetadata.currentSnapshot().timestampMillis()))
        .isEqualTo(metadataLog.getField("timestamp"));

    assertThat(metadataLog.getField("file")).isEqualTo(tableMetadata.metadataFileLocation());
    assertThat(metadataLog.getField("latest_snapshot_id"))
        .isEqualTo(tableMetadata.currentSnapshot().snapshotId());
    assertThat(metadataLog.getField("latest_schema_id"))
        .isEqualTo(tableMetadata.currentSnapshot().schemaId());
    assertThat(metadataLog.getField("latest_sequence_number"))
        .isEqualTo(tableMetadata.currentSnapshot().sequenceNumber());

    // test projection
    List<String> metadataFiles =
        metadataLogEntries.stream()
            .map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toList());
    metadataFiles.add(tableMetadata.metadataFileLocation());
    List<Row> metadataLogWithProjection =
        sql("SELECT file FROM %s$metadata_log_entries", TABLE_NAME);
    assertThat(metadataLogWithProjection).hasSize(3);
    for (int i = 0; i < metadataFiles.size(); i++) {
      assertThat(metadataLogWithProjection.get(i).getField("file")).isEqualTo(metadataFiles.get(i));
    }
  }

  @TestTemplate
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
    List<Row> branches = sql("SELECT * FROM %s$refs WHERE type='BRANCH'", TABLE_NAME);
    assertThat(references).hasSize(3);
    assertThat(branches).hasSize(2);
    List<Row> tags = sql("SELECT * FROM %s$refs WHERE type='TAG'", TABLE_NAME);
    assertThat(tags).hasSize(1);
    // Check branch entries in refs table
    List<Row> mainBranch =
        sql("SELECT * FROM %s$refs WHERE name='main' AND type='BRANCH'", TABLE_NAME);
    assertThat((String) mainBranch.get(0).getFieldAs("name")).isEqualTo("main");
    assertThat((String) mainBranch.get(0).getFieldAs("type")).isEqualTo("BRANCH");
    assertThat((Long) mainBranch.get(0).getFieldAs("snapshot_id")).isEqualTo(currentSnapshotId);
    List<Row> testBranch =
        sql("SELECT * FROM  %s$refs WHERE name='testBranch' AND type='BRANCH'", TABLE_NAME);
    assertThat((String) testBranch.get(0).getFieldAs("name")).isEqualTo("testBranch");
    assertThat((String) testBranch.get(0).getFieldAs("type")).isEqualTo("BRANCH");
    assertThat((Long) testBranch.get(0).getFieldAs("snapshot_id")).isEqualTo(currentSnapshotId);
    assertThat((Long) testBranch.get(0).getFieldAs("max_reference_age_in_ms"))
        .isEqualTo(Long.valueOf(10));
    assertThat((Integer) testBranch.get(0).getFieldAs("min_snapshots_to_keep"))
        .isEqualTo(Integer.valueOf(20));
    assertThat((Long) testBranch.get(0).getFieldAs("max_snapshot_age_in_ms"))
        .isEqualTo(Long.valueOf(30));

    // Check tag entries in refs table
    List<Row> testTag =
        sql("SELECT * FROM %s$refs WHERE name='testTag' AND type='TAG'", TABLE_NAME);
    assertThat((String) testTag.get(0).getFieldAs("name")).isEqualTo("testTag");
    assertThat((String) testTag.get(0).getFieldAs("type")).isEqualTo("TAG");
    assertThat((Long) testTag.get(0).getFieldAs("snapshot_id")).isEqualTo(currentSnapshotId);
    assertThat((Long) testTag.get(0).getFieldAs("max_reference_age_in_ms"))
        .isEqualTo(Long.valueOf(50));
    // Check projection in refs table
    List<Row> testTagProjection =
        sql(
            "SELECT name,type,snapshot_id,max_reference_age_in_ms,min_snapshots_to_keep FROM %s$refs where type='TAG'",
            TABLE_NAME);
    assertThat((String) testTagProjection.get(0).getFieldAs("name")).isEqualTo("testTag");
    assertThat((String) testTagProjection.get(0).getFieldAs("type")).isEqualTo("TAG");
    assertThat((Long) testTagProjection.get(0).getFieldAs("snapshot_id"))
        .isEqualTo(currentSnapshotId);
    assertThat((Long) testTagProjection.get(0).getFieldAs("max_reference_age_in_ms"))
        .isEqualTo(Long.valueOf(50));
    assertThat((String) testTagProjection.get(0).getFieldAs("min_snapshots_to_keep")).isNull();
    List<Row> mainBranchProjection =
        sql("SELECT name, type FROM %s$refs WHERE name='main' AND type = 'BRANCH'", TABLE_NAME);
    assertThat((String) mainBranchProjection.get(0).getFieldAs("name")).isEqualTo("main");
    assertThat((String) mainBranchProjection.get(0).getFieldAs("type")).isEqualTo("BRANCH");
    List<Row> testBranchProjection =
        sql(
            "SELECT type, name, max_reference_age_in_ms, snapshot_id FROM %s$refs WHERE name='testBranch' AND type = 'BRANCH'",
            TABLE_NAME);
    assertThat((String) testBranchProjection.get(0).getFieldAs("name")).isEqualTo("testBranch");
    assertThat((String) testBranchProjection.get(0).getFieldAs("type")).isEqualTo("BRANCH");
    assertThat((Long) testBranchProjection.get(0).getFieldAs("snapshot_id"))
        .isEqualTo(currentSnapshotId);
    assertThat((Long) testBranchProjection.get(0).getFieldAs("max_reference_age_in_ms"))
        .isEqualTo(Long.valueOf(10));
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

  private List<ManifestFile> allDeleteManifests(Table table) {
    List<ManifestFile> manifests = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      manifests.addAll(snapshot.deleteManifests(table.io()));
    }
    return manifests;
  }
}
