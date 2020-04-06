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

package org.apache.iceberg;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestRemoveOrphanFilesAction {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .truncate("c2", 2)
      .identity("c3")
      .build();

  private static SparkSession spark;

  @BeforeClass
  public static void startSpark() {
    TestRemoveOrphanFilesAction.spark = SparkSession.builder()
        .master("local[2]")
        .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestRemoveOrphanFilesAction.spark;
    TestRemoveOrphanFilesAction.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testDryRun() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<String> validFiles = spark.read().format("iceberg")
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
    Assert.assertEquals("Should be 2 valid files", 2, validFiles.size());

    df.write().mode("append").parquet(tableLocation + "/data");

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles = Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
        .filter(FileStatus::isFile)
        .map(file -> file.getPath().toString())
        .collect(Collectors.toList());
    Assert.assertEquals("Should be 3 files", 3, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeAll(validFiles);
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    RemoveOrphanFilesActionResult result1 = actions.removeOrphanFiles()
        .allDataFilesTable(tableLocation + "#all_data_files")
        .olderThan(System.currentTimeMillis())
        .dryRun(true)
        .execute();
    Assert.assertEquals("Action should find 1 data file", invalidFiles, result1.dataFiles());
    Assert.assertTrue("Invalid file should be present", fs.exists(new Path(invalidFiles.get(0))));

    RemoveOrphanFilesActionResult result2 = actions.removeOrphanFiles()
        .allDataFilesTable(tableLocation + "#all_data_files")
        .olderThan(System.currentTimeMillis())
        .execute();
    Assert.assertEquals("Action should delete 1 data file", invalidFiles, result2.dataFiles());
    Assert.assertFalse("Invalid file should not be present", fs.exists(new Path(invalidFiles.get(0))));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testAllValidFilesAreKept() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records1 = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df1 = spark.createDataFrame(records1, ThreeColumnRecord.class).coalesce(1);

    // original append
    df1.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<ThreeColumnRecord> records2 = Lists.newArrayList(
        new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df2 = spark.createDataFrame(records2, ThreeColumnRecord.class).coalesce(1);

    // dynamic partition overwrite
    df2.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("overwrite")
        .save(tableLocation);

    // second append
    df2.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    List<String> snapshotFiles1 = snapshotFiles(snapshots.get(0).snapshotId());
    Assert.assertEquals(1, snapshotFiles1.size());

    List<String> snapshotFiles2 = snapshotFiles(snapshots.get(1).snapshotId());
    Assert.assertEquals(1, snapshotFiles2.size());

    List<String> snapshotFiles3 = snapshotFiles(snapshots.get(2).snapshotId());
    Assert.assertEquals(2, snapshotFiles3.size());

    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/invalid/invalid");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    RemoveOrphanFilesActionResult result = actions.removeOrphanFiles()
        .allDataFilesTable(tableLocation + "#all_data_files")
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete 4 data files", 4, result.dataFiles().size());

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());

    for (String fileLocation : snapshotFiles1) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }

    for (String fileLocation : snapshotFiles2) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }

    for (String fileLocation : snapshotFiles3) {
      Assert.assertTrue("All snapshot files must remain", fs.exists(new Path(fileLocation)));
    }
  }

  @Test
  public void testMetadataFolderIsIntact() throws InterruptedException {
    // write data directly to the table location
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_NEW_DATA_LOCATION, tableLocation);
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/c2_trunc=AA/c3=AAAA");

    // sleep for 1 second to unsure files will be old enough
    Thread.sleep(1000);

    Actions actions = Actions.forTable(table);

    RemoveOrphanFilesActionResult result = actions.removeOrphanFiles()
        .allDataFilesTable(tableLocation + "#all_data_files")
        .olderThan(System.currentTimeMillis())
        .execute();

    Assert.assertEquals("Should delete 1 data file", 1, result.dataFiles().size());

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords = resultDF
        .as(Encoders.bean(ThreeColumnRecord.class))
        .collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testOlderThanTimestamp() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    Thread.sleep(1000);

    long timestamp = System.currentTimeMillis();

    Thread.sleep(1000);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    Actions actions = Actions.forTable(table);

    RemoveOrphanFilesActionResult result = actions.removeOrphanFiles()
        .allDataFilesTable(tableLocation + "#all_data_files")
        .olderThan(timestamp)
        .execute();

    Assert.assertEquals("Should delete only 2 data files", 2, result.dataFiles().size());
  }

  private List<String> snapshotFiles(long snapshotId) {
    return spark.read().format("iceberg")
        .option("snapshot-id", snapshotId)
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
  }
}
