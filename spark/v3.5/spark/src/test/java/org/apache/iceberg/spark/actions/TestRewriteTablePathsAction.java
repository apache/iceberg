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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.Tuple2;

public class TestRewriteTablePathsAction extends TestBase {

  @TempDir private Path staging;
  @TempDir private Path tableDir;
  @TempDir private Path newTableDir;
  @TempDir private Path targetTableDir;

  protected ActionsProvider actions() {
    return SparkActions.get();
  }

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected String tableLocation = null;
  private Table table = null;

  private final String ns = "testns";
  private final String backupNs = "backupns";

  @BeforeEach
  public void setupTableLocation() throws Exception {
    this.tableLocation = tableDir.toFile().toURI().toString();
    this.table = createATableWith2Snapshots(tableLocation);
    createNameSpaces();
  }

  @AfterEach
  public void cleanupTableSetup() throws Exception {
    dropNameSpaces();
  }

  private Table createATableWith2Snapshots(String location) {
    return createTableWithSnapshots(location, 2);
  }

  private Table createTableWithSnapshots(String location, int snapshotNumber) {
    return createTableWithSnapshots(location, snapshotNumber, Maps.newHashMap());
  }

  protected Table createTableWithSnapshots(
      String location, int snapshotNumber, Map<String, String> properties) {
    Table newTable = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, location);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }

    return newTable;
  }

  private void createNameSpaces() {
    sql("CREATE DATABASE IF NOT EXISTS %s", ns);
    sql("CREATE DATABASE IF NOT EXISTS %s", backupNs);
  }

  private void dropNameSpaces() {
    sql("DROP DATABASE IF EXISTS %s CASCADE", ns);
    sql("DROP DATABASE IF EXISTS %s CASCADE", backupNs);
  }

  @Test
  public void testRewritePath() throws Exception {
    String targetTableLocation = targetTableLocation();

    // check the data file location before the rebuild
    List<String> validDataFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assertions.assertEquals(2, validDataFiles.size(), "Should be 2 valid data files");

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation)
            .endVersion("v3.metadata.json")
            .execute();

    Assertions.assertEquals(
        "v3.metadata.json", result.latestVersion(), "The latest version should be");

    checkFileNum(3, 2, 2, 9, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // verify the data file path after the rebuild
    List<String> validDataFilesAfterRebuilt =
        spark
            .read()
            .format("iceberg")
            .load(targetTableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assertions.assertEquals(2, validDataFilesAfterRebuilt.size(), "Should be 2 valid data files");
    for (String item : validDataFilesAfterRebuilt) {
      assertTrue(
          item.startsWith(targetTableLocation), "Data file should point to the new location");
    }

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetTableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.sort("c1", "c2", "c3").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    expectedRecords.add(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Assertions.assertEquals(expectedRecords, actualRecords, "Rows must match");
  }

  @Test
  public void testSameLocations() throws Exception {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            actions()
                .rewriteTablePath(table)
                .rewriteLocationPrefix(tableLocation, tableLocation)
                .endVersion("v1.metadata.json")
                .execute(),
        "Source prefix cannot be the same as target prefix");
  }

  @Test
  public void testStartVersion() throws Exception {
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation())
            .startVersion("v2.metadata.json")
            .execute();

    checkFileNum(1, 1, 1, 4, result);

    List<Tuple2<String, String>> paths = readPathPairList(result.fileListLocation());

    String currentSnapshotId = String.valueOf(table.currentSnapshot().snapshotId());
    Assertions.assertEquals(
        1,
        paths.stream().filter(c -> c._2().contains(currentSnapshotId)).count(),
        "Should have the current snapshot file");

    String parentSnapshotId = String.valueOf(table.currentSnapshot().parentId());
    Assertions.assertEquals(
        0,
        paths.stream().filter(c -> c._2().contains(parentSnapshotId)).count(),
        "Should NOT have the parent snapshot file");
  }

  @Test
  public void testTableWith3Snapshots(@TempDir Path location1, @TempDir Path location2)
      throws Exception {
    String location = newTableLocation();
    Table tableWith3Snaps = createTableWithSnapshots(location, 3);
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(tableWith3Snaps)
            .rewriteLocationPrefix(location, toAbsolute(location1))
            .startVersion("v2.metadata.json")
            .execute();

    checkFileNum(2, 2, 2, 8, result);

    // start from the first version
    RewriteTablePath.Result result1 =
        actions()
            .rewriteTablePath(tableWith3Snaps)
            .rewriteLocationPrefix(location, toAbsolute(location2))
            .startVersion("v1.metadata.json")
            .execute();

    checkFileNum(3, 3, 3, 12, result1);
  }

  @Test
  public void testFullTableRewritePath() throws Exception {
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation())
            .execute();

    checkFileNum(3, 2, 2, 9, result);
  }

  @Test
  public void testDeleteDataFile() throws Exception {
    List<String> validDataFiles =
        spark
            .read()
            .format("iceberg")
            .load(table.location() + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();

    table.newDelete().deleteFile(validDataFiles.stream().findFirst().get()).commit();

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .stagingLocation(stagingLocation())
            .execute();

    checkFileNum(4, 3, 3, 12, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // verify data rows
    Dataset<Row> resultDF = spark.read().format("iceberg").load(targetTableLocation());
    Assertions.assertEquals(
        1,
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).count(),
        "There are only one row left since we deleted a data file");
  }

  @Test
  public void testPositionDeletes() throws Exception {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(
                table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location(),
                0L));

    File file = new File(removePrefix(table.location() + "/data/deeply/nested/file.parquet"));
    DeleteFile positionDeletes =
        FileHelpers.writeDeleteFile(
                table, table.io().newOutputFile(file.toURI().toString()), deletes)
            .first();

    table.newRowDelta().addDeletes(positionDeletes).commit();

    Assertions.assertEquals(1, spark.read().format("iceberg").load(table.location()).count());

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .execute();

    // We have one more snapshot, an additional manifest list, and a new (delete) manifest,
    // and an additional position delete
    checkFileNum(4, 3, 3, 13, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // Positional delete affects a single row, so only one row must remain
    Assertions.assertEquals(1, spark.read().format("iceberg").load(targetTableLocation()).count());
  }

  @Test
  public void testEqualityDeletes() throws Exception {
    Table sourceTable = createTableWithSnapshots(newTableLocation(), 1);

    // Add more varied data
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(3, "BBBBBBBBBB", "BBBB"),
            new ThreeColumnRecord(4, "CCCCCCCCCC", "CCCC"),
            new ThreeColumnRecord(5, "DDDDDDDDDD", "DDDD"));
    spark
        .createDataFrame(records, ThreeColumnRecord.class)
        .coalesce(1)
        .select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(newTableLocation());

    Schema deleteRowSchema = sourceTable.schema().select("c2");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("c2", "AAAAAAAAAA"), dataDelete.copy("c2", "CCCCCCCCCC"));
    File file = new File(removePrefix(sourceTable.location()) + "/data/deeply/nested/file.parquet");
    DeleteFile equalityDeletes =
        FileHelpers.writeDeleteFile(
            sourceTable,
            sourceTable.io().newOutputFile(file.toURI().toString()),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);
    sourceTable.newRowDelta().addDeletes(equalityDeletes).commit();

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .execute();

    // We have four metadata files: for the table creation, for the initial snapshot, for the
    // second append here, and for commit with equality deletes. Thus, we have three manifest lists.
    // We have a data file for each snapshot (two with data, one with equality deletes)
    checkFileNum(4, 3, 3, 13, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // Equality deletes affect three rows, so just two rows must remain
    Assertions.assertEquals(2, spark.read().format("iceberg").load(targetTableLocation()).count());
  }

  @Test
  public void testFullTableRewritePathWithDeletedVersionFiles() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createTableWithSnapshots(location, 2);
    // expire the first snapshot
    Table staticTable = newStaticTable(location + "metadata/v2.metadata.json", table.io());
    actions()
        .expireSnapshots(sourceTable)
        .expireSnapshotId(staticTable.currentSnapshot().snapshotId())
        .execute();

    // create 100 more snapshots
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    for (int i = 0; i < 100; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }
    sourceTable.refresh();

    // v1/v2/v3.metadata.json has been deleted in v104.metadata.json, and there is no way to find
    // the first snapshot
    // from the version file history
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(location, targetTableLocation())
            .execute();

    checkFileNum(101, 101, 101, 406, result);
  }

  @Test
  public void testRewritePathWithoutSnapshot() throws Exception {
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(tableLocation, newTableLocation())
            .endVersion("v1.metadata.json")
            .execute();

    // the only rebuilt file is v1.metadata.json since it contains no snapshot
    checkFileNum(1, 0, 0, 1, result);
  }

  @Test
  public void testExpireSnapshotBeforeRewrite() throws Exception {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .execute();

    checkFileNum(4, 1, 2, 9, result);
  }

  @Test
  public void testStartSnapshotWithoutValidSnapshot() throws Exception {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    Assertions.assertEquals(
        1, ((List) table.snapshots()).size(), "1 out 2 snapshot has been removed");

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .stagingLocation(stagingLocation())
            .startVersion("v2.metadata.json")
            .execute();

    // 2 metadata.json, 1 manifest list file, 1 manifest files
    checkFileNum(2, 1, 1, 5, result);
  }

  @Test
  public void testMoveTheVersionExpireSnapshot() throws Exception {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    // only move version v4, which is the version generated by snapshot expiration
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .stagingLocation(stagingLocation())
            .startVersion("v3.metadata.json")
            .execute();

    // only v4.metadata.json needs to move
    checkFileNum(1, 0, 0, 1, result);
  }

  @Test
  public void testMoveVersionWithInvalidSnapshots() throws Exception {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    Assertions.assertThrows(
        UnsupportedOperationException.class,
        () ->
            actions()
                .rewriteTablePath(table)
                .rewriteLocationPrefix(table.location(), newTableLocation())
                .stagingLocation(stagingLocation())
                .endVersion("v3.metadata.json")
                .execute(),
        "Copy a version with invalid snapshots aren't allowed");
  }

  @Test
  public void testRollBack() throws Exception {
    long secondSnapshotId = table.currentSnapshot().snapshotId();

    // roll back to the first snapshot(v2)
    table.manageSnapshots().setCurrentSnapshot(table.currentSnapshot().parentId()).commit();

    // add a new snapshot
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(table.location());

    table.refresh();

    // roll back to the second snapshot(v3)
    table.manageSnapshots().setCurrentSnapshot(secondSnapshotId).commit();

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), newTableLocation())
            .stagingLocation(stagingLocation())
            .execute();
    checkFileNum(6, 3, 3, 15, result);
  }

  @Test
  public void testWriteAuditPublish() throws Exception {
    // enable WAP
    table.updateProperties().set(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true").commit();
    spark.conf().set("spark.wap.id", "1");

    // add a new snapshot without changing the current snapshot of the table
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(table.location());

    table.refresh();

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), newTableLocation())
            .stagingLocation(stagingLocation())
            .execute();

    // There are 3 snapshots in total, although the current snapshot is the second one.
    checkFileNum(5, 3, 3, 14, result);
  }

  @Test
  public void testSchemaChange() throws Exception {
    // change the schema
    table.updateSchema().addColumn("c4", Types.StringType.get()).commit();

    // copy table
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), newTableLocation())
            .stagingLocation(stagingLocation())
            .execute();

    // check the result
    checkFileNum(4, 2, 2, 10, result);
  }

  @Test
  public void testSnapshotIdInheritanceEnabled() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.SNAPSHOT_ID_INHERITANCE_ENABLED, "true");

    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
            .execute();

    checkFileNum(3, 2, 2, 9, result);
  }

  @Test
  public void testMetadataCompression() throws Exception {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 2, properties);

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
            .endVersion("v2.gz.metadata.json")
            .execute();

    checkFileNum(2, 1, 1, 5, result);

    result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
            .startVersion("v1.gz.metadata.json")
            .execute();

    checkFileNum(2, 2, 2, 8, result);
  }

  @Test
  public void testInvalidArgs() {
    RewriteTablePath actions = actions().rewriteTablePath(table);

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.rewriteLocationPrefix("", null),
        "Source prefix('') cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.rewriteLocationPrefix(null, null),
        "Source prefix('null') cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.stagingLocation(""),
        "Staging location('') cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.stagingLocation(null),
        "Staging location('null') cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.startVersion(null),
        "Start version('null') cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.endVersion(" "),
        "End version cannot be empty");

    assertThrows(
        IllegalArgumentException.class,
        () -> actions.endVersion(null),
        "End version cannot be empty");
  }

  @Test
  public void testStatisticFile() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String tableName = "v2tblwithstats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 0);

    TableMetadata metadata = currentMetadata(sourceTable);
    TableMetadata withStatistics =
        TableMetadata.buildFrom(metadata)
            .setStatistics(
                43,
                new GenericStatisticsFile(
                    43, "/some/path/to/stats/file", 128, 27, ImmutableList.of()))
            .build();

    OutputFile file = sourceTable.io().newOutputFile(metadata.metadataFileLocation());
    TableMetadataParser.overwrite(withStatistics, file);

    assertThrows(
        IllegalArgumentException.class,
        () -> {
          actions()
              .rewriteTablePath(sourceTable)
              .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
              .execute();
        },
        "Should fail to copy a table with the statistics field");
  }

  @Test
  public void testMetadataCompressionWithMetastoreTable() throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    Table sourceTable =
        createMetastoreTable(
            newTableLocation(), properties, "default", "testMetadataCompression", 2);

    TableMetadata currentMetadata = currentMetadata(sourceTable);

    // set the second version as the endVersion
    String endVersion = fileName(currentMetadata.previousFiles().get(1).file());
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .endVersion(endVersion)
            .execute();

    checkFileNum(2, 1, 1, 5, result);

    // set the first version as the lastCopiedVersion
    String firstVersion = fileName(currentMetadata.previousFiles().get(0).file());
    result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .startVersion(firstVersion)
            .execute();

    checkFileNum(2, 2, 2, 8, result);
  }

  // Metastore table tests
  @Test
  public void testMetadataLocationChange() throws Exception {
    Table sourceTable =
        createMetastoreTable(newTableLocation(), Maps.newHashMap(), "default", "tbl", 1);
    String metadataFilePath = currentMetadata(sourceTable).metadataFileLocation();

    String newMetadataDir = "new-metadata-dir";
    sourceTable
        .updateProperties()
        .set(TableProperties.WRITE_METADATA_LOCATION, newTableLocation() + newMetadataDir)
        .commit();

    spark.sql("insert into hive.default.tbl values (1, 'AAAAAAAAAA', 'AAAA')");
    sourceTable.refresh();

    // copy table
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .execute();

    checkFileNum(4, 2, 2, 10, result);

    // pick up a version from the old metadata dir as the end version
    RewriteTablePath.Result result1 =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .endVersion(fileName(metadataFilePath))
            .execute();

    checkFileNum(2, 1, 1, 5, result1);

    // pick up a version from the old metadata dir as the last copied version
    RewriteTablePath.Result result2 =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .startVersion(fileName(metadataFilePath))
            .execute();

    checkFileNum(2, 1, 1, 5, result2);
  }

  @Test
  public void testV2Table() throws Exception {
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    properties.put("write.delete.mode", "merge-on-read");
    String tableName = "v2tbl";
    Table sourceTable =
        createMetastoreTable(newTableLocation(), properties, "default", tableName, 0);
    // ingest data
    List<ThreeColumnRecord> records =
        Lists.newArrayList(
            new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"),
            new ThreeColumnRecord(3, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .saveAsTable("hive.default." + tableName);
    sourceTable.refresh();

    // generate position delete files
    spark.sql(String.format("delete from hive.default.%s where c1 = 1", tableName));
    sourceTable.refresh();

    List<Object[]> originalData =
        rowsToJava(
            spark
                .read()
                .format("iceberg")
                .load("hive.default." + tableName)
                .sort("c1", "c2", "c3")
                .collectAsList());
    // two rows
    Assertions.assertEquals(2, originalData.size());

    // copy table and check the results
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(newTableLocation(), targetTableLocation())
            .execute();

    checkFileNum(3, 2, 2, 9, result);
    // one data and one metadata file
    copyTableFiles(result);

    // register table
    String metadataLocation = currentMetadata(sourceTable).metadataFileLocation();
    String versionFile = fileName(metadataLocation);
    String targetTableName = "copiedV2Table";
    TableIdentifier tableIdentifier = TableIdentifier.of("default", targetTableName);
    catalog.registerTable(tableIdentifier, targetTableLocation() + "/metadata/" + versionFile);

    List<Object[]> copiedData =
        rowsToJava(
            spark
                .read()
                .format("iceberg")
                .load("hive.default." + targetTableName)
                .sort("c1", "c2", "c3")
                .collectAsList());

    assertEquals("Rows must match", originalData, copiedData);
  }

  protected void checkFileNum(
      int versionFileCount,
      int manifestListCount,
      int manifestFileCount,
      int totalCount,
      RewriteTablePath.Result result) {
    List<String> filesToMove =
        spark
            .read()
            .format("text")
            .load(result.fileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Assertions.assertEquals(totalCount, filesToMove.size(), "Wrong total file count");
    Assertions.assertEquals(
        versionFileCount,
        filesToMove.stream().filter(f -> f.endsWith(".metadata.json")).count(),
        "Wrong rebuilt version file count");
    Assertions.assertEquals(
        manifestListCount,
        filesToMove.stream().filter(f -> f.contains("snap-")).count(),
        "Wrong rebuilt Manifest list file count");
    Assertions.assertEquals(
        manifestFileCount,
        filesToMove.stream().filter(f -> f.endsWith("-m0.avro")).count(),
        "Wrong rebuilt Manifest file file count");
  }

  protected String newTableLocation() throws IOException {
    return toAbsolute(newTableDir);
  }

  protected String targetTableLocation() throws IOException {
    return toAbsolute(targetTableDir);
  }

  protected String stagingLocation() throws IOException {
    return toAbsolute(staging);
  }

  protected String toAbsolute(Path relative) throws IOException {
    return relative.toFile().toURI().toString();
  }

  private void copyTableFiles(RewriteTablePath.Result result) throws Exception {
    List<Tuple2<String, String>> filesToMove = readPathPairList(result.fileListLocation());

    for (Tuple2<String, String> pathPair : filesToMove) {
      FileUtils.copyFile(new File(URI.create(pathPair._1())), new File(URI.create(pathPair._2())));
    }
  }

  private String removePrefix(String path) {
    return path.substring(path.lastIndexOf(":") + 1);
  }

  protected Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  private List<Tuple2<String, String>> readPathPairList(String path) {
    Encoder<Tuple2<String, String>> encoder = Encoders.tuple(Encoders.STRING(), Encoders.STRING());
    return spark
        .read()
        .format("csv")
        .schema(encoder.schema())
        .load(path)
        .as(encoder)
        .collectAsList();
  }

  private Table createMetastoreTable(
      String location,
      Map<String, String> properties,
      String namespace,
      String tableName,
      int snapshotNumber) {
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");
    spark.conf().set("spark.sql.catalog.hive.cache-enabled", "false");

    StringBuilder propertiesStr = new StringBuilder();
    properties.forEach((k, v) -> propertiesStr.append("'" + k + "'='" + v + "',"));
    String tblProperties =
        propertiesStr.substring(0, propertiesStr.length() > 0 ? propertiesStr.length() - 1 : 0);

    sql("DROP TABLE IF EXISTS hive.%s.%s", namespace, tableName);
    if (tblProperties.isEmpty()) {
      String sqlStr =
          String.format(
              "CREATE TABLE hive.%s.%s (c1 bigint, c2 string, c3 string)", namespace, tableName);
      if (!location.isEmpty()) {
        sqlStr = String.format("%s USING iceberg LOCATION '%s'", sqlStr, location);
      }
      sql(sqlStr);
    } else {
      String sqlStr =
          String.format(
              "CREATE TABLE hive.%s.%s (c1 bigint, c2 string, c3 string)", namespace, tableName);
      if (!location.isEmpty()) {
        sqlStr = String.format("%s USING iceberg LOCATION '%s'", sqlStr, location);
      }

      sqlStr = String.format("%s TBLPROPERTIES (%s)", sqlStr, tblProperties);
      sql(sqlStr);
    }

    for (int i = 0; i < snapshotNumber; i++) {
      sql("insert into hive.%s.%s values (%s, 'AAAAAAAAAA', 'AAAA')", namespace, tableName, i);
    }
    return catalog.loadTable(TableIdentifier.of(namespace, tableName));
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }
}
