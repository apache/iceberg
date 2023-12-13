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
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Files;
import org.apache.iceberg.GenericBlobMetadata;
import org.apache.iceberg.GenericStatisticsFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.actions.DeleteOrphanFilesSparkAction.StringToFileURI;
import org.apache.iceberg.spark.source.FilePathLastModifiedRecord;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class TestRemoveOrphanFilesAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA =
      new Schema(
          optional(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));
  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).truncate("c2", 2).identity("c3").build();

  @Rule public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  @Test
  public void testDryRun() throws IOException, InterruptedException {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    List<String> validFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("Should be 2 valid files", 2, validFiles.size());

    df.write().mode("append").parquet(tableLocation + "/data");

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles =
        Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
            .filter(FileStatus::isFile)
            .map(file -> file.getPath().toString())
            .collect(Collectors.toList());
    Assert.assertEquals("Should be 3 files", 3, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeAll(validFiles);
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result1 =
        actions.deleteOrphanFiles(table).deleteWith(s -> {}).execute();
    Assert.assertTrue(
        "Default olderThan interval should be safe",
        Iterables.isEmpty(result1.orphanFileLocations()));

    DeleteOrphanFiles.Result result2 =
        actions
            .deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis())
            .deleteWith(s -> {})
            .execute();
    Assert.assertEquals("Action should find 1 file", invalidFiles, result2.orphanFileLocations());
    Assert.assertTrue("Invalid file should be present", fs.exists(new Path(invalidFiles.get(0))));

    DeleteOrphanFiles.Result result3 =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();
    Assert.assertEquals("Action should delete 1 file", invalidFiles, result3.orphanFileLocations());
    Assert.assertFalse(
        "Invalid file should not be present", fs.exists(new Path(invalidFiles.get(0))));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testAllValidFilesAreKept() throws IOException, InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df1 = spark.createDataFrame(records1, ThreeColumnRecord.class).coalesce(1);

    // original append
    df1.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df2 = spark.createDataFrame(records2, ThreeColumnRecord.class).coalesce(1);

    // dynamic partition overwrite
    df2.select("c1", "c2", "c3").write().format("iceberg").mode("overwrite").save(tableLocation);

    // second append
    df2.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

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

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 4 files", 4, Iterables.size(result.orphanFileLocations()));

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
  public void orphanedFileRemovedWithParallelTasks() throws InterruptedException, IOException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records1 =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df1 = spark.createDataFrame(records1, ThreeColumnRecord.class).coalesce(1);

    // original append
    df1.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    List<ThreeColumnRecord> records2 =
        Lists.newArrayList(new ThreeColumnRecord(2, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df2 = spark.createDataFrame(records2, ThreeColumnRecord.class).coalesce(1);

    // dynamic partition overwrite
    df2.select("c1", "c2", "c3").write().format("iceberg").mode("overwrite").save(tableLocation);

    // second append
    df2.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df2.coalesce(1).write().mode("append").parquet(tableLocation + "/data/invalid/invalid");

    waitUntilAfter(System.currentTimeMillis());

    Set<String> deletedFiles = Sets.newHashSet();
    Set<String> deleteThreads = ConcurrentHashMap.newKeySet();
    AtomicInteger deleteThreadsIndex = new AtomicInteger(0);

    ExecutorService executorService =
        Executors.newFixedThreadPool(
            4,
            runnable -> {
              Thread thread = new Thread(runnable);
              thread.setName("remove-orphan-" + deleteThreadsIndex.getAndIncrement());
              thread.setDaemon(true);
              return thread;
            });

    DeleteOrphanFiles.Result result =
        SparkActions.get()
            .deleteOrphanFiles(table)
            .executeDeleteWith(executorService)
            .olderThan(System.currentTimeMillis() + 5000) // Ensure all orphan files are selected
            .deleteWith(
                file -> {
                  deleteThreads.add(Thread.currentThread().getName());
                  deletedFiles.add(file);
                })
            .execute();

    // Verifies that the delete methods ran in the threads created by the provided ExecutorService
    // ThreadFactory
    Assert.assertEquals(
        deleteThreads,
        Sets.newHashSet(
            "remove-orphan-0", "remove-orphan-1", "remove-orphan-2", "remove-orphan-3"));

    Assert.assertEquals("Should delete 4 files", 4, deletedFiles.size());
  }

  @Test
  public void testWapFilesAreKept() throws InterruptedException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_AUDIT_PUBLISH_ENABLED, "true");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    // normal write
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    spark.conf().set("spark.wap.id", "1");

    // wap write
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Should not return data from the staged snapshot", records, actualRecords);

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertTrue(
        "Should not delete any files", Iterables.isEmpty(result.orphanFileLocations()));
  }

  @Test
  public void testMetadataFolderIsIntact() throws InterruptedException {
    // write data directly to the table location
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_DATA_LOCATION, tableLocation);
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/c2_trunc=AA/c3=AAAA");

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 1 file", 1, Iterables.size(result.orphanFileLocations()));

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testOlderThanTimestamp() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");
    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    waitUntilAfter(System.currentTimeMillis());

    long timestamp = System.currentTimeMillis();

    waitUntilAfter(System.currentTimeMillis() + 1000L);

    df.write().mode("append").parquet(tableLocation + "/data/c2_trunc=AA/c3=AAAA");

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(timestamp).execute();

    Assert.assertEquals(
        "Should delete only 2 files", 2, Iterables.size(result.orphanFileLocations()));
  }

  @Test
  public void testRemoveUnreachableMetadataVersionFiles() throws InterruptedException {
    Map<String, String> props = Maps.newHashMap();
    props.put(TableProperties.WRITE_DATA_LOCATION, tableLocation);
    props.put(TableProperties.METADATA_PREVIOUS_VERSIONS_MAX, "1");
    Table table = TABLES.create(SCHEMA, SPEC, props, tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 1 file", 1, Iterables.size(result.orphanFileLocations()));
    Assert.assertTrue(
        "Should remove v1 file",
        StreamSupport.stream(result.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("v1.metadata.json")));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);
  }

  @Test
  public void testManyTopLevelPartitions() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i), String.valueOf(i)));
    }

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertTrue(
        "Should not delete any files", Iterables.isEmpty(result.orphanFileLocations()));

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testManyLeafPartitions() throws InterruptedException {
    Table table = TABLES.create(SCHEMA, SPEC, Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList();
    for (int i = 0; i < 100; i++) {
      records.add(new ThreeColumnRecord(i, String.valueOf(i % 3), String.valueOf(i)));
    }

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertTrue(
        "Should not delete any files", Iterables.isEmpty(result.orphanFileLocations()));

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testHiddenPartitionPaths() throws InterruptedException {
    Schema schema =
        new Schema(
            optional(1, "c1", Types.IntegerType.get()),
            optional(2, "_c2", Types.StringType.get()),
            optional(3, "c3", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("_c2", 2).identity("c3").build();
    Table table = TABLES.create(schema, spec, Maps.newHashMap(), tableLocation);

    StructType structType =
        new StructType()
            .add("c1", DataTypes.IntegerType)
            .add("_c2", DataTypes.StringType)
            .add("c3", DataTypes.StringType);
    List<Row> records = Lists.newArrayList(RowFactory.create(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, structType).coalesce(1);

    df.select("c1", "_c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/data/_c2_trunc=AA/c3=AAAA");
    df.write().mode("append").parquet(tableLocation + "/data/_c2_trunc=AA/c3=AAAA");

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 2 files", 2, Iterables.size(result.orphanFileLocations()));
  }

  @Test
  public void testHiddenPartitionPathsWithPartitionEvolution() throws InterruptedException {
    Schema schema =
        new Schema(
            optional(1, "_c1", Types.IntegerType.get()),
            optional(2, "_c2", Types.StringType.get()),
            optional(3, "c3", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("_c2", 2).build();
    Table table = TABLES.create(schema, spec, Maps.newHashMap(), tableLocation);

    StructType structType =
        new StructType()
            .add("_c1", DataTypes.IntegerType)
            .add("_c2", DataTypes.StringType)
            .add("c3", DataTypes.StringType);
    List<Row> records = Lists.newArrayList(RowFactory.create(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, structType).coalesce(1);

    df.select("_c1", "_c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.write().mode("append").parquet(tableLocation + "/data/_c2_trunc=AA");

    table.updateSpec().addField("_c1").commit();

    df.write().mode("append").parquet(tableLocation + "/data/_c2_trunc=AA/_c1=1");

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 2 files", 2, Iterables.size(result.orphanFileLocations()));
  }

  @Test
  public void testHiddenPathsStartingWithPartitionNamesAreIgnored()
      throws InterruptedException, IOException {
    Schema schema =
        new Schema(
            optional(1, "c1", Types.IntegerType.get()),
            optional(2, "_c2", Types.StringType.get()),
            optional(3, "c3", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).truncate("_c2", 2).identity("c3").build();
    Table table = TABLES.create(schema, spec, Maps.newHashMap(), tableLocation);

    StructType structType =
        new StructType()
            .add("c1", DataTypes.IntegerType)
            .add("_c2", DataTypes.StringType)
            .add("c3", DataTypes.StringType);
    List<Row> records = Lists.newArrayList(RowFactory.create(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, structType).coalesce(1);

    df.select("c1", "_c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    Path pathToFileInHiddenFolder = new Path(dataPath, "_c2_trunc/file.txt");
    fs.createNewFile(pathToFileInHiddenFolder);

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    DeleteOrphanFiles.Result result =
        actions.deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals("Should delete 0 files", 0, Iterables.size(result.orphanFileLocations()));
    Assert.assertTrue(fs.exists(pathToFileInHiddenFolder));
  }

  private List<String> snapshotFiles(long snapshotId) {
    return spark
        .read()
        .format("iceberg")
        .option("snapshot-id", snapshotId)
        .load(tableLocation + "#files")
        .select("file_path")
        .as(Encoders.STRING())
        .collectAsList();
  }

  @Test
  public void testRemoveOrphanFilesWithRelativeFilePath() throws IOException, InterruptedException {
    Table table =
        TABLES.create(
            SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableDir.getAbsolutePath());

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableDir.getAbsolutePath());

    List<String> validFiles =
        spark
            .read()
            .format("iceberg")
            .load(tableLocation + "#files")
            .select("file_path")
            .as(Encoders.STRING())
            .collectAsList();
    Assert.assertEquals("Should be 1 valid files", 1, validFiles.size());
    String validFile = validFiles.get(0);

    df.write().mode("append").parquet(tableLocation + "/data");

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<String> allFiles =
        Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
            .filter(FileStatus::isFile)
            .map(file -> file.getPath().toString())
            .collect(Collectors.toList());
    Assert.assertEquals("Should be 2 files", 2, allFiles.size());

    List<String> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeIf(file -> file.contains(validFile));
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();
    DeleteOrphanFiles.Result result =
        actions
            .deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis())
            .deleteWith(s -> {})
            .execute();
    Assert.assertEquals("Action should find 1 file", invalidFiles, result.orphanFileLocations());
    Assert.assertTrue("Invalid file should be present", fs.exists(new Path(invalidFiles.get(0))));
  }

  @Test
  public void testRemoveOrphanFilesWithHadoopCatalog() throws InterruptedException {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), tableLocation);
    String namespaceName = "testDb";
    String tableName = "testTb";

    Namespace namespace = Namespace.of(namespaceName);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
    Table table =
        catalog.createTable(
            tableIdentifier, SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap());

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(table.location());

    df.write().mode("append").parquet(table.location() + "/data");

    waitUntilAfter(System.currentTimeMillis());

    table.refresh();

    DeleteOrphanFiles.Result result =
        SparkActions.get().deleteOrphanFiles(table).olderThan(System.currentTimeMillis()).execute();

    Assert.assertEquals(
        "Should delete only 1 files", 1, Iterables.size(result.orphanFileLocations()));

    Dataset<Row> resultDF = spark.read().format("iceberg").load(table.location());
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", records, actualRecords);
  }

  @Test
  public void testHiveCatalogTable() throws IOException {
    Table table =
        catalog.createTable(
            TableIdentifier.of("default", "hivetestorphan"),
            SCHEMA,
            SPEC,
            tableLocation,
            Maps.newHashMap());

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save("default.hivetestorphan");

    String location = table.location().replaceFirst("file:", "");
    new File(location + "/data/trashfile").createNewFile();

    DeleteOrphanFiles.Result result =
        SparkActions.get()
            .deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Assert.assertTrue(
        "trash file should be removed",
        StreamSupport.stream(result.orphanFileLocations().spliterator(), false)
            .anyMatch(file -> file.contains("file:" + location + "/data/trashfile")));
  }

  @Test
  public void testGarbageCollectionDisabled() {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    table.updateProperties().set(TableProperties.GC_ENABLED, "false").commit();

    AssertHelpers.assertThrows(
        "Should complain about removing orphan files",
        ValidationException.class,
        "Cannot delete orphan files: GC is disabled",
        () -> SparkActions.get().deleteOrphanFiles(table).execute());
  }

  @Test
  public void testCompareToFileList() throws IOException, InterruptedException {
    Table table =
        TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    Path dataPath = new Path(tableLocation + "/data");
    FileSystem fs = dataPath.getFileSystem(spark.sessionState().newHadoopConf());
    List<FilePathLastModifiedRecord> validFiles =
        Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
            .filter(FileStatus::isFile)
            .map(
                file ->
                    new FilePathLastModifiedRecord(
                        file.getPath().toString(), new Timestamp(file.getModificationTime())))
            .collect(Collectors.toList());

    Assert.assertEquals("Should be 2 valid files", 2, validFiles.size());

    df.write().mode("append").parquet(tableLocation + "/data");

    List<FilePathLastModifiedRecord> allFiles =
        Arrays.stream(fs.listStatus(dataPath, HiddenPathFilter.get()))
            .filter(FileStatus::isFile)
            .map(
                file ->
                    new FilePathLastModifiedRecord(
                        file.getPath().toString(), new Timestamp(file.getModificationTime())))
            .collect(Collectors.toList());

    Assert.assertEquals("Should be 3 files", 3, allFiles.size());

    List<FilePathLastModifiedRecord> invalidFiles = Lists.newArrayList(allFiles);
    invalidFiles.removeAll(validFiles);
    List<String> invalidFilePaths =
        invalidFiles.stream()
            .map(FilePathLastModifiedRecord::getFilePath)
            .collect(Collectors.toList());
    Assert.assertEquals("Should be 1 invalid file", 1, invalidFiles.size());

    waitUntilAfter(System.currentTimeMillis());

    SparkActions actions = SparkActions.get();

    Dataset<Row> compareToFileList =
        spark
            .createDataFrame(allFiles, FilePathLastModifiedRecord.class)
            .withColumnRenamed("filePath", "file_path")
            .withColumnRenamed("lastModified", "last_modified");

    DeleteOrphanFiles.Result result1 =
        actions
            .deleteOrphanFiles(table)
            .compareToFileList(compareToFileList)
            .deleteWith(s -> {})
            .execute();
    Assert.assertTrue(
        "Default olderThan interval should be safe",
        Iterables.isEmpty(result1.orphanFileLocations()));

    DeleteOrphanFiles.Result result2 =
        actions
            .deleteOrphanFiles(table)
            .compareToFileList(compareToFileList)
            .olderThan(System.currentTimeMillis())
            .deleteWith(s -> {})
            .execute();
    Assert.assertEquals(
        "Action should find 1 file", invalidFilePaths, result2.orphanFileLocations());
    Assert.assertTrue(
        "Invalid file should be present", fs.exists(new Path(invalidFilePaths.get(0))));

    DeleteOrphanFiles.Result result3 =
        actions
            .deleteOrphanFiles(table)
            .compareToFileList(compareToFileList)
            .olderThan(System.currentTimeMillis())
            .execute();
    Assert.assertEquals(
        "Action should delete 1 file", invalidFilePaths, result3.orphanFileLocations());
    Assert.assertFalse(
        "Invalid file should not be present", fs.exists(new Path(invalidFilePaths.get(0))));

    List<ThreeColumnRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(records);
    expectedRecords.addAll(records);

    Dataset<Row> resultDF = spark.read().format("iceberg").load(tableLocation);
    List<ThreeColumnRecord> actualRecords =
        resultDF.as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Rows must match", expectedRecords, actualRecords);

    List<FilePathLastModifiedRecord> outsideLocationMockFiles =
        Lists.newArrayList(new FilePathLastModifiedRecord("/tmp/mock1", new Timestamp(0L)));

    Dataset<Row> compareToFileListWithOutsideLocation =
        spark
            .createDataFrame(outsideLocationMockFiles, FilePathLastModifiedRecord.class)
            .withColumnRenamed("filePath", "file_path")
            .withColumnRenamed("lastModified", "last_modified");

    DeleteOrphanFiles.Result result4 =
        actions
            .deleteOrphanFiles(table)
            .compareToFileList(compareToFileListWithOutsideLocation)
            .deleteWith(s -> {})
            .execute();
    Assert.assertEquals(
        "Action should find nothing", Lists.newArrayList(), result4.orphanFileLocations());
  }

  protected long waitUntilAfter(long timestampMillis) {
    long current = System.currentTimeMillis();
    while (current <= timestampMillis) {
      current = System.currentTimeMillis();
    }
    return current;
  }

  @Test
  public void testRemoveOrphanFilesWithStatisticFiles() throws Exception {
    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
            tableLocation);

    List<ThreeColumnRecord> records =
        Lists.newArrayList(new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA"));
    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);
    df.select("c1", "c2", "c3").write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    long snapshotSequenceNumber = table.currentSnapshot().sequenceNumber();

    File statsLocation =
        new File(new URI(tableLocation))
            .toPath()
            .resolve("data")
            .resolve("some-stats-file")
            .toFile();
    StatisticsFile statisticsFile;
    try (PuffinWriter puffinWriter = Puffin.write(Files.localOutput(statsLocation)).build()) {
      puffinWriter.add(
          new Blob(
              "some-blob-type",
              ImmutableList.of(1),
              snapshotId,
              snapshotSequenceNumber,
              ByteBuffer.wrap("blob content".getBytes(StandardCharsets.UTF_8))));
      puffinWriter.finish();
      statisticsFile =
          new GenericStatisticsFile(
              snapshotId,
              statsLocation.toString(),
              puffinWriter.fileSize(),
              puffinWriter.footerSize(),
              puffinWriter.writtenBlobsMetadata().stream()
                  .map(GenericBlobMetadata::from)
                  .collect(ImmutableList.toImmutableList()));
    }

    Transaction transaction = table.newTransaction();
    transaction.updateStatistics().setStatistics(snapshotId, statisticsFile).commit();
    transaction.commitTransaction();

    SparkActions.get()
        .deleteOrphanFiles(table)
        .olderThan(System.currentTimeMillis() + 1000)
        .execute();

    Assertions.assertThat(statsLocation.exists()).as("stats file should exist").isTrue();
    Assertions.assertThat(statsLocation.length())
        .as("stats file length")
        .isEqualTo(statisticsFile.fileSizeInBytes());

    transaction = table.newTransaction();
    transaction.updateStatistics().removeStatistics(statisticsFile.snapshotId()).commit();
    transaction.commitTransaction();

    DeleteOrphanFiles.Result result =
        SparkActions.get()
            .deleteOrphanFiles(table)
            .olderThan(System.currentTimeMillis() + 1000)
            .execute();
    Iterable<String> orphanFileLocations = result.orphanFileLocations();
    Assertions.assertThat(orphanFileLocations).as("Should be orphan files").hasSize(1);
    Assertions.assertThat(Iterables.getOnlyElement(orphanFileLocations))
        .as("Deleted file")
        .isEqualTo(statsLocation.toURI().toString());
    Assertions.assertThat(statsLocation.exists()).as("stats file should be deleted").isFalse();
  }

  @Test
  public void testPathsWithExtraSlashes() {
    List<String> validFiles = Lists.newArrayList("file:///dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("file:///dir1/////dir2///file1");
    executeTest(validFiles, actualFiles, Lists.newArrayList());
  }

  @Test
  public void testPathsWithValidFileHavingNoAuthority() {
    List<String> validFiles = Lists.newArrayList("hdfs:///dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename/dir1/dir2/file1");
    executeTest(validFiles, actualFiles, Lists.newArrayList());
  }

  @Test
  public void testPathsWithActualFileHavingNoAuthority() {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs:///dir1/dir2/file1");
    executeTest(validFiles, actualFiles, Lists.newArrayList());
  }

  @Test
  public void testPathsWithEqualSchemes() {
    List<String> validFiles = Lists.newArrayList("scheme1://bucket1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("scheme2://bucket1/dir1/dir2/file1");
    AssertHelpers.assertThrows(
        "Test remove orphan files with equal schemes",
        ValidationException.class,
        "Conflicting authorities/schemes: [(scheme1, scheme2)]",
        () ->
            executeTest(
                validFiles,
                actualFiles,
                Lists.newArrayList(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                DeleteOrphanFiles.PrefixMismatchMode.ERROR));

    Map<String, String> equalSchemes = Maps.newHashMap();
    equalSchemes.put("scheme1", "scheme");
    equalSchemes.put("scheme2", "scheme");
    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList(),
        equalSchemes,
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.ERROR);
  }

  @Test
  public void testPathsWithEqualAuthorities() {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1");
    AssertHelpers.assertThrows(
        "Test remove orphan files with equal authorities",
        ValidationException.class,
        "Conflicting authorities/schemes: [(servicename1, servicename2)]",
        () ->
            executeTest(
                validFiles,
                actualFiles,
                Lists.newArrayList(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                DeleteOrphanFiles.PrefixMismatchMode.ERROR));

    Map<String, String> equalAuthorities = Maps.newHashMap();
    equalAuthorities.put("servicename1", "servicename");
    equalAuthorities.put("servicename2", "servicename");
    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList(),
        ImmutableMap.of(),
        equalAuthorities,
        DeleteOrphanFiles.PrefixMismatchMode.ERROR);
  }

  @Test
  public void testRemoveOrphanFileActionWithDeleteMode() {
    List<String> validFiles = Lists.newArrayList("hdfs://servicename1/dir1/dir2/file1");
    List<String> actualFiles = Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1");

    executeTest(
        validFiles,
        actualFiles,
        Lists.newArrayList("hdfs://servicename2/dir1/dir2/file1"),
        ImmutableMap.of(),
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.DELETE);
  }

  private void executeTest(
      List<String> validFiles, List<String> actualFiles, List<String> expectedOrphanFiles) {
    executeTest(
        validFiles,
        actualFiles,
        expectedOrphanFiles,
        ImmutableMap.of(),
        ImmutableMap.of(),
        DeleteOrphanFiles.PrefixMismatchMode.IGNORE);
  }

  private void executeTest(
      List<String> validFiles,
      List<String> actualFiles,
      List<String> expectedOrphanFiles,
      Map<String, String> equalSchemes,
      Map<String, String> equalAuthorities,
      DeleteOrphanFiles.PrefixMismatchMode mode) {

    StringToFileURI toFileUri = new StringToFileURI(equalSchemes, equalAuthorities);

    Dataset<String> validFileDS = spark.createDataset(validFiles, Encoders.STRING());
    Dataset<String> actualFileDS = spark.createDataset(actualFiles, Encoders.STRING());

    List<String> orphanFiles =
        DeleteOrphanFilesSparkAction.findOrphanFiles(
            spark, toFileUri.apply(actualFileDS), toFileUri.apply(validFileDS), mode);
    Assert.assertEquals(expectedOrphanFiles, orphanFiles);
  }
}
