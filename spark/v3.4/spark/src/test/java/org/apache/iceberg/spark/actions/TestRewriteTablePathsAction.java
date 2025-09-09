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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.actions.RewriteManifests;
import org.apache.iceberg.actions.RewriteTablePath;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.SparkEnv;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockInfoManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.BroadcastBlockId;
import org.junit.jupiter.api.AfterEach;
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
  public void setupTableLocation() {
    this.tableLocation = tableDir.toFile().toURI().toString();
    this.table = createATableWith2Snapshots(tableLocation);
    createNameSpaces();
  }

  @AfterEach
  public void cleanupTableSetup() {
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
    return createTableWithSnapshots(location, snapshotNumber, properties, "append");
  }

  private Table createTableWithSnapshots(
      String location, int snapshotNumber, Map<String, String> properties, String mode) {
    Table newTable = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, location);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode(mode).save(location);
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
    assertThat(validDataFiles).hasSize(2);

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(tableLocation, targetTableLocation)
            .endVersion("v3.metadata.json")
            .execute();

    assertThat(result.latestVersion()).isEqualTo("v3.metadata.json");

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
    assertThat(validDataFilesAfterRebuilt)
        .hasSize(2)
        .allMatch(item -> item.startsWith(targetTableLocation));

    // verify data rows
    List<Object[]> actual = rows(targetTableLocation);
    List<Object[]> expected = rows(tableLocation);
    assertEquals("Rows should match after copy", expected, actual);
  }

  @Test
  public void testSameLocations() {
    assertThatThrownBy(
            () ->
                actions()
                    .rewriteTablePath(table)
                    .rewriteLocationPrefix(tableLocation, tableLocation)
                    .endVersion("v1.metadata.json")
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Source prefix cannot be the same as target prefix");
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
    assertThat(paths.stream().filter(c -> c._2().contains(currentSnapshotId)))
        .as("Should have the current snapshot file")
        .hasSize(1);

    String parentSnapshotId = String.valueOf(table.currentSnapshot().parentId());
    assertThat(paths.stream().filter(c -> c._2().contains(parentSnapshotId)))
        .as("Should NOT have the parent snapshot file")
        .isEmpty();
  }

  @Test
  public void testIncrementalRewrite() throws Exception {
    String location = newTableLocation();
    Table sourceTable =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), location);
    List<ThreeColumnRecord> recordsA =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> dfA = spark.createDataFrame(recordsA, ThreeColumnRecord.class).coalesce(1);

    // Write first increment to source table
    dfA.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    assertThat(spark.read().format("iceberg").load(location).collectAsList()).hasSize(1);

    // Replicate first increment to target table
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTable.location(), targetTableLocation())
            .execute();
    copyTableFiles(result);
    assertThat(spark.read().format("iceberg").load(targetTableLocation()).collectAsList())
        .hasSize(1);

    // Write second increment to source table
    List<ThreeColumnRecord> recordsB =
        Lists.newArrayList(new ThreeColumnRecord(2, "BBBBBBBBB", "BBB"));
    Dataset<Row> dfB = spark.createDataFrame(recordsB, ThreeColumnRecord.class).coalesce(1);
    dfB.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    assertThat(spark.read().format("iceberg").load(location).collectAsList()).hasSize(2);

    // Replicate second increment to target table
    sourceTable.refresh();
    Table targetTable = TABLES.load(targetTableLocation());
    String targetTableMetadata = currentMetadata(targetTable).metadataFileLocation();
    String startVersion = fileName(targetTableMetadata);
    RewriteTablePath.Result incrementalRewriteResult =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTable.location(), targetTableLocation())
            .startVersion(startVersion)
            .execute();
    copyTableFiles(incrementalRewriteResult);
    List<Object[]> actual = rowsSorted(targetTableLocation(), "c1");
    List<Object[]> expected = rowsSorted(location, "c1");
    assertEquals("Rows should match after copy", expected, actual);
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
  public void testManifestRewriteAndIncrementalCopy() throws Exception {
    RewriteTablePath.Result initialResult =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .stagingLocation(stagingLocation())
            .execute();
    checkFileNum(3, 2, 2, 9, initialResult);

    // rewrite manifest without change data files
    RewriteManifests.Result rewriteManifestResult = actions().rewriteManifests(table).execute();
    int addedManifest = Iterables.size(rewriteManifestResult.addedManifests());

    // only move version v4, which is the version generated by rewrite manifest
    RewriteTablePath.Result postReweiteResult =
        actions()
            .rewriteTablePath(table)
            .rewriteLocationPrefix(table.location(), targetTableLocation())
            .stagingLocation(stagingLocation())
            .startVersion("v3.metadata.json")
            .execute();

    // no data files need to move
    checkFileNum(1, 1, addedManifest, 3, postReweiteResult);
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
    assertThat(resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList())
        .as("There are only one row left since we deleted a data file")
        .hasSize(1);
  }

  @Test
  public void testPositionDeletes() throws Exception {
    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(
                table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location(),
                0L));

    File file = new File(removePrefix(table.location() + "/data/deeply/nested/deletes.parquet"));
    DeleteFile positionDeletes =
        FileHelpers.writeDeleteFile(
                table, table.io().newOutputFile(file.toURI().toString()), deletes)
            .first();

    table.newRowDelta().addDeletes(positionDeletes).commit();

    assertThat(spark.read().format("iceberg").load(table.location()).collectAsList()).hasSize(1);

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
    assertThat(spark.read().format("iceberg").load(targetTableLocation()).collectAsList())
        .hasSize(1);
  }

  @Test
  public void testPositionDeleteWithRow() throws Exception {
    String dataFileLocation =
        table.currentSnapshot().addedDataFiles(table.io()).iterator().next().location();
    List<PositionDelete<?>> deletes = Lists.newArrayList();
    OutputFile deleteFile =
        table
            .io()
            .newOutputFile(
                new File(removePrefix(table.location() + "/data/deeply/nested/deletes.parquet"))
                    .toURI()
                    .toString());
    deletes.add(positionDelete(SCHEMA, dataFileLocation, 0L, 1, "AAAAAAAAAA", "AAAA"));
    DeleteFile positionDeletes = FileHelpers.writePosDeleteFile(table, deleteFile, null, deletes);
    table.newRowDelta().addDeletes(positionDeletes).commit();

    assertThat(spark.read().format("iceberg").load(table.location()).collectAsList()).hasSize(1);

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

    // check copied position delete row
    Object[] deletedRow = (Object[]) rows(targetTableLocation() + "#position_deletes").get(0)[2];
    assertEquals(
        "Position deletes should be equal", new Object[] {1, "AAAAAAAAAA", "AAAA"}, deletedRow);

    // Positional delete affects a single row, so only one row must remain
    assertThat(spark.read().format("iceberg").load(targetTableLocation()).collectAsList())
        .hasSize(1);
  }

  @Test
  public void testPositionDeletesAcrossFiles() throws Exception {
    Stream<DataFile> allFiles =
        StreamSupport.stream(table.snapshots().spliterator(), false)
            .flatMap(s -> StreamSupport.stream(s.addedDataFiles(table.io()).spliterator(), false));
    List<Pair<CharSequence, Long>> deletes =
        allFiles.map(f -> Pair.of((CharSequence) f.location(), 0L)).collect(Collectors.toList());

    // a single position delete with two entries
    assertThat(deletes).hasSize(2);

    File file = new File(removePrefix(table.location() + "/data/deeply/nested/file.parquet"));
    DeleteFile positionDeletes =
        FileHelpers.writeDeleteFile(
                table, table.io().newOutputFile(file.toURI().toString()), deletes)
            .first();

    table.newRowDelta().addDeletes(positionDeletes).commit();

    assertThat(spark.read().format("iceberg").load(table.location()).collectAsList()).isEmpty();

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

    assertThat(spark.read().format("iceberg").load(targetTableLocation()).collectAsList())
        .isEmpty();
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
    assertThat(spark.read().format("iceberg").load(targetTableLocation()).collectAsList())
        .hasSize(2);
  }

  @Test
  public void testFullTableRewritePathWithDeletedVersionFiles() throws Exception {
    String location = newTableLocation();
    Table sourceTable = createTableWithSnapshots(location, 2);
    // expire the first snapshot
    Table staticTable = newStaticTable(location + "metadata/v2.metadata.json", table.io());
    int expiredManifestListCount = 1;
    ExpireSnapshots.Result expireResult =
        actions()
            .expireSnapshots(sourceTable)
            .expireSnapshotId(staticTable.currentSnapshot().snapshotId())
            .execute();
    assertThat(expireResult.deletedManifestListsCount()).isEqualTo(expiredManifestListCount);

    // create 100 more snapshots
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    for (int i = 0; i < 100; i++) {
      df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(location);
    }
    sourceTable.refresh();

    // each iteration generate 1 version file, 1 manifest list, 1 manifest and 1 data file
    int totalIteration = 102;
    // v1/v2/v3.metadata.json has been deleted in v104.metadata.json, and there is no way to find
    // the first snapshot
    // from the version file history
    int missingVersionFile = 1;
    // since first snapshot cannot be found, first data files will also be skipped
    int missingDataFile = 1;
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(location, targetTableLocation())
            .execute();

    checkFileNum(
        totalIteration - missingVersionFile,
        totalIteration - expiredManifestListCount,
        totalIteration,
        totalIteration * 4 - missingVersionFile - expiredManifestListCount - missingDataFile,
        result);
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
  public void testRewritePathWithNonLiveEntry() throws Exception {
    String location = newTableLocation();
    // first overwrite generate 1 manifest and 1 data file
    // each subsequent overwrite on unpartitioned table generate 2 manifests and 1 data file
    Table tableWith3Snaps = createTableWithSnapshots(location, 3, Maps.newHashMap(), "overwrite");

    Snapshot oldest = SnapshotUtil.oldestAncestor(tableWith3Snaps);
    String oldestDataFilePath =
        Iterables.getOnlyElement(
                tableWith3Snaps.snapshot(oldest.snapshotId()).addedDataFiles(tableWith3Snaps.io()))
            .location();
    String deletedDataFilePathInTargetLocation =
        String.format("%sdata/%s", targetTableLocation(), fileName(oldestDataFilePath));

    // expire the oldest snapshot and remove oldest DataFile
    ExpireSnapshots.Result expireResult =
        actions().expireSnapshots(tableWith3Snaps).expireSnapshotId(oldest.snapshotId()).execute();
    assertThat(expireResult)
        .as("Should deleted 1 data files in root snapshot")
        .extracting(
            ExpireSnapshots.Result::deletedManifestListsCount,
            ExpireSnapshots.Result::deletedManifestsCount,
            ExpireSnapshots.Result::deletedDataFilesCount)
        .contains(1L, 1L, 1L);

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(tableWith3Snaps)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(tableWith3Snaps.location(), targetTableLocation())
            .execute();

    // 5 version files include 1 table creation 3 overwrite and 1 snapshot expiration
    // 3 overwrites generate 3 manifest list and 5 manifests with 3 data files
    // snapshot expiration removed 1 of each
    checkFileNum(5, 2, 4, 13, result);

    // copy the metadata files and data files
    copyTableFiles(result);

    // expect deleted data file is excluded from rewrite and copy
    List<String> copiedDataFiles =
        spark
            .read()
            .format("iceberg")
            .load(targetTableLocation() + "#all_files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    assertThat(copiedDataFiles).hasSize(2).doesNotContain(deletedDataFilePathInTargetLocation);

    // expect manifest entries still contain deleted entry
    List<String> copiedEntries =
        spark
            .read()
            .format("iceberg")
            .load(targetTableLocation() + "#all_entries")
            .filter("status == 2")
            .select("data_file.file_path")
            .as(Encoders.STRING())
            .collectAsList();
    assertThat(copiedEntries).contains(deletedDataFilePathInTargetLocation);
  }

  @Test
  public void testStartSnapshotWithoutValidSnapshot() throws Exception {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    assertThat(table.snapshots()).hasSize(1);

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
  public void testMoveVersionWithInvalidSnapshots() {
    // expire one snapshot
    actions().expireSnapshots(table).expireSnapshotId(table.currentSnapshot().parentId()).execute();

    assertThatThrownBy(
            () ->
                actions()
                    .rewriteTablePath(table)
                    .rewriteLocationPrefix(table.location(), newTableLocation())
                    .stagingLocation(stagingLocation())
                    .endVersion("v3.metadata.json")
                    .execute())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining(
            "Unable to build the manifest files dataframe. The end version in use may contain invalid snapshots. "
                + "Please choose an earlier version without invalid snapshots.");
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

    assertThatThrownBy(() -> actions.rewriteLocationPrefix("", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Source prefix('') cannot be empty");

    assertThatThrownBy(() -> actions.rewriteLocationPrefix(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Source prefix('null') cannot be empty");

    assertThatThrownBy(() -> actions.stagingLocation(""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Staging location('') cannot be empty");

    assertThatThrownBy(() -> actions.stagingLocation(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Staging location('null') cannot be empty");

    assertThatThrownBy(() -> actions.startVersion(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Start version('null') cannot be empty");

    assertThatThrownBy(() -> actions.endVersion(" "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("End version(' ') cannot be empty");

    assertThatThrownBy(() -> actions.endVersion(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("End version('null') cannot be empty");
  }

  @Test
  public void testPartitionStatisticFile() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String tableName = "v2tblwithPartStats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 0);

    TableMetadata metadata = currentMetadata(sourceTable);
    TableMetadata withPartStatistics =
        TableMetadata.buildFrom(metadata)
            .setPartitionStatistics(
                ImmutableGenericPartitionStatisticsFile.builder()
                    .snapshotId(11L)
                    .path("/some/partition/stats/file.parquet")
                    .fileSizeInBytes(42L)
                    .build())
            .build();

    OutputFile file = sourceTable.io().newOutputFile(metadata.metadataFileLocation());
    TableMetadataParser.overwrite(withPartStatistics, file);

    assertThatThrownBy(
            () ->
                actions()
                    .rewriteTablePath(sourceTable)
                    .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
                    .execute())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Partition statistics files are not supported yet");
  }

  @Test
  public void testTableWithManyStatisticFiles() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String tableName = "v2tblwithmanystats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 0);

    int iterations = 10;
    for (int i = 0; i < iterations; i++) {
      sql("insert into hive.default.%s values (%s, 'AAAAAAAAAA', 'AAAA')", tableName, i);
      sourceTable.refresh();
      actions().computeTableStats(sourceTable).execute();
    }

    sourceTable.refresh();
    assertThat(sourceTable.statisticsFiles()).hasSize(iterations);

    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
            .execute();

    checkFileNum(
        iterations * 2 + 1, iterations, iterations, iterations, iterations * 6 + 1, result);
  }

  @Test
  public void testStatisticsFileSourcePath() throws IOException {
    String sourceTableLocation = newTableLocation();
    Map<String, String> properties = Maps.newHashMap();
    properties.put("format-version", "2");
    String tableName = "v2tblwithstats";
    Table sourceTable =
        createMetastoreTable(sourceTableLocation, properties, "default", tableName, 1);

    // Compute table statistics to generate a .stats file
    actions().computeTableStats(sourceTable).execute();

    assertThat(sourceTable.statisticsFiles())
        .as("Should include 1 statistics file after compute stats")
        .hasSize(1);

    String targetTableLocation = targetTableLocation();
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation)
            .execute();

    checkFileNum(3, 1, 1, 1, 7, result);

    // Read the file list to verify statistics file paths
    List<Tuple2<String, String>> filesToMove = readPathPairList(result.fileListLocation());

    // Find the statistics file entry in the file list using stream
    Tuple2<String, String> statsFilePathPair =
        filesToMove.stream()
            .filter(pathPair -> pathPair._1().endsWith(".stats"))
            .findFirst()
            .orElse(null);

    assertThat(statsFilePathPair).as("Should find statistics file in file list").isNotNull();

    // Verify the source path points to the actual source location, not staging
    assertThat(statsFilePathPair._1())
        .as("Statistics file source should point to source table location and NOT staging")
        .startsWith(sourceTableLocation)
        .contains("/metadata/")
        .doesNotContain("staging");

    // Verify the target path is correctly rewritten
    assertThat(statsFilePathPair._2())
        .as("Statistics file target should point to target table location")
        .startsWith(targetTableLocation);
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
  public void testDeleteFrom() throws Exception {
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
    assertThat(originalData).hasSize(2);

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

  @Test
  public void testKryoDeserializeBroadcastValues() {
    sparkContext.getConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    RewriteTablePathSparkAction action =
        (RewriteTablePathSparkAction) actions().rewriteTablePath(table);
    Broadcast<Table> tableBroadcast = action.tableBroadcast();
    // force deserializing broadcast values
    removeBroadcastValuesFromLocalBlockManager(tableBroadcast.id());
    assertThat(tableBroadcast.getValue().uuid()).isEqualTo(table.uuid());
  }

  @Test
  public void testNestedDirectoryStructurePreservation() throws Exception {
    String sourceTableLocation = newTableLocation();
    Table sourceTable = createTableWithSnapshots(sourceTableLocation, 1);

    // Create position delete files with same names in different nested directories
    // This simulates the scenario tested in
    // TestRewriteTablePathUtil.testStagingPathPreservesDirectoryStructure
    List<Pair<CharSequence, Long>> deletes1 =
        Lists.newArrayList(
            Pair.of(
                sourceTable
                    .currentSnapshot()
                    .addedDataFiles(sourceTable.io())
                    .iterator()
                    .next()
                    .location(),
                0L));

    List<Pair<CharSequence, Long>> deletes2 =
        Lists.newArrayList(
            Pair.of(
                sourceTable
                    .currentSnapshot()
                    .addedDataFiles(sourceTable.io())
                    .iterator()
                    .next()
                    .location(),
                0L));

    // Create delete files with same name in different nested paths (hash1/ and hash2/)
    File file1 =
        new File(removePrefix(sourceTable.location() + "/data/hash1/delete_0_0_0.parquet"));
    File file2 =
        new File(removePrefix(sourceTable.location() + "/data/hash2/delete_0_0_0.parquet"));

    DeleteFile positionDeletes1 =
        FileHelpers.writeDeleteFile(
                sourceTable, sourceTable.io().newOutputFile(file1.toURI().toString()), deletes1)
            .first();

    DeleteFile positionDeletes2 =
        FileHelpers.writeDeleteFile(
                sourceTable, sourceTable.io().newOutputFile(file2.toURI().toString()), deletes2)
            .first();

    sourceTable.newRowDelta().addDeletes(positionDeletes1).commit();
    sourceTable.newRowDelta().addDeletes(positionDeletes2).commit();

    // Perform rewrite with staging location to test directory structure preservation
    RewriteTablePath.Result result =
        actions()
            .rewriteTablePath(sourceTable)
            .stagingLocation(stagingLocation())
            .rewriteLocationPrefix(sourceTableLocation, targetTableLocation())
            .execute();

    // Copy the files and verify structure is preserved
    copyTableFiles(result);

    // Read the file paths from the rewritten result to verify directory structure
    List<Tuple2<String, String>> filePaths = readPathPairList(result.fileListLocation());

    // Find the delete files in the result
    List<Tuple2<String, String>> deleteFilePaths =
        filePaths.stream()
            .filter(pair -> pair._2().contains("delete_0_0_0.parquet"))
            .collect(Collectors.toList());

    // Should have 2 delete files with different paths
    assertThat(deleteFilePaths).hasSize(2);

    // Verify that the directory structure is preserved in target paths
    assertThat(deleteFilePaths)
        .anyMatch(pair -> pair._2().contains("/hash1/delete_0_0_0.parquet"))
        .anyMatch(pair -> pair._2().contains("/hash2/delete_0_0_0.parquet"));

    // Verify that the files have different target paths (no conflicts)
    String targetPath1 = deleteFilePaths.get(0)._2();
    String targetPath2 = deleteFilePaths.get(1)._2();
    assertThat(targetPath1).isNotEqualTo(targetPath2);

    // Verify both target paths start with the target table location
    assertThat(targetPath1).startsWith(targetTableLocation());
    assertThat(targetPath2).startsWith(targetTableLocation());
  }

  protected void checkFileNum(
      int versionFileCount,
      int manifestListCount,
      int manifestFileCount,
      int totalCount,
      RewriteTablePath.Result result) {
    checkFileNum(versionFileCount, manifestListCount, manifestFileCount, 0, totalCount, result);
  }

  protected void checkFileNum(
      int versionFileCount,
      int manifestListCount,
      int manifestFileCount,
      int statisticsFileCount,
      int totalCount,
      RewriteTablePath.Result result) {
    List<String> filesToMove =
        spark
            .read()
            .format("text")
            .load(result.fileListLocation())
            .as(Encoders.STRING())
            .collectAsList();
    Predicate<String> isManifest =
        f ->
            (f.contains("optimized-m-") && f.endsWith(".avro"))
                || f.endsWith("-m0.avro")
                || f.endsWith("-m1.avro");
    Predicate<String> isManifestList = f -> f.contains("snap-") && f.endsWith(".avro");
    Predicate<String> isMetadataJSON = f -> f.endsWith(".metadata.json");

    assertThat(filesToMove.stream().filter(isMetadataJSON))
        .as("Wrong rebuilt version file count")
        .hasSize(versionFileCount);
    assertThat(filesToMove.stream().filter(isManifestList))
        .as("Wrong rebuilt Manifest list file count")
        .hasSize(manifestListCount);
    assertThat(filesToMove.stream().filter(isManifest))
        .as("Wrong rebuilt Manifest file file count")
        .hasSize(manifestFileCount);
    assertThat(filesToMove.stream().filter(f -> f.endsWith(".stats")))
        .as("Wrong rebuilt Statistic file count")
        .hasSize(statisticsFileCount);
    assertThat(filesToMove).as("Wrong total file count").hasSize(totalCount);
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

  protected String toAbsolute(Path relative) {
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

  private List<Object[]> rows(String location) {
    return rowsToJava(spark.read().format("iceberg").load(location).collectAsList());
  }

  private List<Object[]> rowsSorted(String location, String sortCol) {
    return rowsToJava(spark.read().format("iceberg").load(location).sort(sortCol).collectAsList());
  }

  private PositionDelete<GenericRecord> positionDelete(
      Schema tableSchema, CharSequence path, Long position, Object... values) {
    PositionDelete<GenericRecord> posDelete = PositionDelete.create();
    GenericRecord nested = GenericRecord.create(tableSchema);
    for (int i = 0; i < values.length; i++) {
      nested.set(i, values[i]);
    }
    posDelete.set(path, position, nested);
    return posDelete;
  }

  private void removeBroadcastValuesFromLocalBlockManager(long id) {
    BlockId blockId = new BroadcastBlockId(id, "");
    SparkEnv env = SparkEnv.get();
    env.broadcastManager().cachedValues().clear();
    BlockManager blockManager = env.blockManager();
    BlockInfoManager blockInfoManager = blockManager.blockInfoManager();
    blockInfoManager.lockForWriting(blockId, true);
    blockInfoManager.removeBlock(blockId);
    blockManager.memoryStore().remove(blockId);
  }
}
