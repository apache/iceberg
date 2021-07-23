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
import java.util.List;
import java.util.Set;
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

public class TestCheckSnapshotIntegrityAction extends SparkTestBase {
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
  public void testCheckSnapshotIntegrity() {
    List<String> validFiles = validFiles();

    String metadataPath = ((HasTableOperations) table).operations().current().metadataFileLocation();
    String startVersionFile = tableLocation + "metadata/v1.metadata.json";
    Table startTable = newStaticTable(startVersionFile, table.io());
    CheckSnapshotIntegrity.Result result =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(0, result);

    // delete one file
    table.io().deleteFile(validFiles.get(0));
    CheckSnapshotIntegrity.Result result1 =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(1, result1);

    // delete another file
    table.io().deleteFile(validFiles.get(1));
    CheckSnapshotIntegrity.Result result2 =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(2, result2);
  }

  @Test
  public void testStartFromFirstSnapshot() {
    List<String> validFiles = validFiles();

    String metadataPath = ((HasTableOperations) table).operations().current().metadataFileLocation();

    String startVersionFile = tableLocation + "metadata/v2.metadata.json";
    Table startTable = newStaticTable(startVersionFile, table.io());
    CheckSnapshotIntegrity.Result result =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(0, result);

    // delete the file added by the first snapshot
    table.io().deleteFile(validFiles.get(1));
    CheckSnapshotIntegrity.Result result1 =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(0, result1);

    // delete the file added by the first snapshot
    table.io().deleteFile(validFiles.get(0));
    CheckSnapshotIntegrity.Result result2 =
        actions().checkSnapshotIntegrity(startTable).targetVersion(metadataPath).execute();
    checkMissingFiles(1, result2);
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

  private void checkMissingFiles(int num, CheckSnapshotIntegrity.Result result) {
    Assert.assertEquals("Missing file count should be", num, ((Set) result.missingFileLocations()).size());
  }

  private Table newStaticTable(String metadataFileLocation, FileIO io) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  private List<String> validFiles() {
    return validFiles(tableLocation);
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
}
