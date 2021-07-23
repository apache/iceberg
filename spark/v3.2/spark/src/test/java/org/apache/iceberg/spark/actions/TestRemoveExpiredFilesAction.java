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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.CheckSnapshotIntegrity;
import org.apache.iceberg.actions.CopyTable;
import org.apache.iceberg.actions.RemoveExpiredFiles;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestRemoveExpiredFilesAction extends SparkTestBase {
  private ActionsProvider actions() {
    return SparkActions.get();
  }

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;
  private Table table = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = createTableWith2Snapshots();
  }

  @Test
  public void testRemoveExpiredFiles() throws Exception {
    String tblLocation = newTableLocation();
    Table tbl = createTableWith2Snapshots(tblLocation);
    String targetLocation = newTableLocation();

    // copy it to a new place, we have two identical tables after this
    CopyTable.Result result = actions().copyTable(tbl).rewriteLocationPrefix(tblLocation, targetLocation).execute();
    moveTableFiles(tblLocation, targetLocation, stagingDir(result));
    String tgtMetadataLoc = targetLocation + "metadata/" + result.latestVersion();
    Table targetTable = newStaticTable(tgtMetadataLoc, tbl.io());

    // expire the first snapshot in the source table
    Table staticTable = newStaticTable(tblLocation + "metadata/v2.metadata.json", tbl.io());
    actions().expireSnapshots(tbl)
        .expireSnapshotId(staticTable.currentSnapshot().snapshotId())
        .execute();

    // copy it again
    CopyTable.Result result2 = actions().copyTable(tbl)
        .targetTable(targetTable)
        .rewriteLocationPrefix(tblLocation, targetLocation)
        .execute();
    moveTableFiles(tblLocation, targetLocation, stagingDir(result2));
    String tgtMetadataLocNew = targetLocation + "metadata/" + result2.latestVersion();

    // remove expired files
    RemoveExpiredFiles.Result integrityResult =
        actions().removeExpiredFiles(targetTable).targetVersion(tgtMetadataLocNew).execute();

    // one manifest list file is deleted
    Assert.assertEquals("Deleted manifest list file count should be", 1L, integrityResult.deletedManifestListsCount());
    Assert.assertEquals("Deleted data file count should be", 0L, integrityResult.deletedDataFilesCount());
    Assert.assertEquals("Deleted manifest file count should be", 0L, integrityResult.deletedManifestsCount());

    // check file count
    Assert.assertEquals("Source table file count should equal to target table one", validFiles(tblLocation).size(),
        validFiles(targetLocation).size());

    // verify data rows
    Assert.assertEquals("Rows must match", records(tblLocation), records(targetLocation));
  }

  @Test
  public void testInputs() {
    CheckSnapshotIntegrity actions = actions().checkSnapshotIntegrity(table);

    AssertHelpers.assertThrows("", IllegalArgumentException.class, () -> actions.targetVersion(""));
    AssertHelpers.assertThrows("", IllegalArgumentException.class, () -> actions.targetVersion(null));
    AssertHelpers.assertThrows("", IllegalArgumentException.class, () -> actions.targetVersion("invalid"));

    // either version file name or path are valid
    String versionFilePath = currentMetadata(table).metadataFileLocation();
    actions.targetVersion(versionFilePath);
    String versionFilename = fileName(versionFilePath);
    actions.targetVersion(versionFilename);
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }

  private static String fileName(String path) {
    String filename = path;
    int lastIndex = path.lastIndexOf(File.separator);
    if (lastIndex != -1) {
      filename = path.substring(lastIndex + 1);
    }
    return filename;
  }

  private List<ThreeColumnRecord> records(String location) {
    Dataset<Row> resultDF = spark.read().format("iceberg").load(location);
    return resultDF.sort("c1", "c2", "c3")
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
  }

  private String stagingDir(CopyTable.Result result) {
    String metadataFileListPath = result.metadataFileListLocation();
    return metadataFileListPath.substring(0, metadataFileListPath.lastIndexOf(File.separator));
  }

  private void moveTableFiles(String sourceDir, String targetDir, String stagingDir) throws Exception {
    FileUtils.copyDirectory(new File(removePrefix(sourceDir) + "data/"), new File(removePrefix(targetDir) + "/data/"));
    FileUtils.copyDirectory(new File(removePrefix(stagingDir)), new File(removePrefix(targetDir) + "/metadata/"));
  }

  private String removePrefix(String path) {
    return path.substring(path.lastIndexOf(":") + 1);
  }

  private Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  private List<String> validFiles(String location) {
    return spark.read().format("iceberg")
        .load(location + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
  }

  private Table createTableWith2Snapshots() {
    return createTableWith2Snapshots(tableLocation);
  }

  private Table createTableWith2Snapshots(String tblLocation) {
    return createTableWithSnapshots(tblLocation, 2);
  }

  private Table createTableWithSnapshots(String tblLocation, int snapshotNumber) {
    Table tbl = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tblLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("append")
          .save(tblLocation);
    }

    tbl.refresh();

    return tbl;
  }

  protected String newTableLocation() throws IOException {
    return temp.newFolder().toURI().toString();
  }
}
