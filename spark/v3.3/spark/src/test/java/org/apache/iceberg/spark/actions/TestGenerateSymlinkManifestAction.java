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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestGenerateSymlinkManifestAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testGenerateSymlinkManifestEmptyTableFails() {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);
    AssertHelpers.assertThrows(
        "Should not support generating symlink manifest for empty table",
        ValidationException.class,
        "Cannot generate symlink manifest for empty table",
        () -> SparkActions.get().generateSymlinkManifest(table).execute());
  }

  @Test
  public void testGenerateSymlinkManifestWithDeleteFilesFailsWithoutIgnore() {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);
    table.newDelete().deleteFromRowFilter(Expressions.equal("c1", 1)).commit();
    AssertHelpers.assertThrows(
        "Should not support generate symlink manifest when there are delete files",
        UnsupportedOperationException.class,
        "Cannot generate symlink manifest when there are delete files",
        () -> SparkActions.get().generateSymlinkManifest(table).execute());
  }

  @Test
  public void testGenerateSymlinkManifestUnpartitioned() throws IOException {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);
    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    SparkActions.get().generateSymlinkManifest(table).ignoreDeleteFiles().execute();

    long snapshot = table.currentSnapshot().snapshotId();
    Path dataPath = new Path(tableLocation + "_symlink_format_manifest/" + snapshot + "/");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles =
        Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
            .filter(FileStatus::isFile)
            .map(file -> file.getPath().toString())
            .collect(Collectors.toList());
    Assert.assertEquals("Should be 1 file", 1, allFiles.size());
  }

  @Test
  public void testGenerateSymlinkManifestPartitioned() {}

  @Test
  public void testGenerateSymlinkManifestPartitionedCustomLocation() {}
}
