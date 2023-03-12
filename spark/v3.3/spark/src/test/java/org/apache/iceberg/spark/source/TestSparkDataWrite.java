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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.TableProperties.SPARK_WRITE_PARTITIONED_FANOUT_ENABLED;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkDataWrite {
  private static final Configuration CONF = new Configuration();
  private final FileFormat format;
  private final String branch;
  private static SparkSession spark = null;
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "format = {0}, branch = {1}")
  public static Object[] parameters() {
    return new Object[] {
      new Object[] {"parquet", null},
      new Object[] {"parquet", "main"},
      new Object[] {"parquet", "testBranch"},
      new Object[] {"avro", null},
      new Object[] {"orc", "testBranch"}
    };
  }

  @BeforeClass
  public static void startSpark() {
    TestSparkDataWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @Parameterized.AfterParam
  public static void clearSourceCache() {
    ManualSource.clearTables();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkDataWrite.spark;
    TestSparkDataWrite.spark = null;
    currentSpark.stop();
  }

  public TestSparkDataWrite(String format, String branch) {
    this.format = FileFormat.fromString(format);
    this.branch = branch;
  }

  @Test
  public void testBasicWrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);
    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
    for (ManifestFile manifest :
        SnapshotUtil.latestSnapshot(table, branch).allManifests(table.io())) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        // TODO: avro not support split
        if (!format.equals(FileFormat.AVRO)) {
          Assert.assertNotNull("Split offsets not present", file.splitOffsets());
        }
        Assert.assertEquals("Should have reported record count as 1", 1, file.recordCount());
        // TODO: append more metric info
        if (format.equals(FileFormat.PARQUET)) {
          Assert.assertNotNull("Column sizes metric not present", file.columnSizes());
          Assert.assertNotNull("Counts metric not present", file.valueCounts());
          Assert.assertNotNull("Null value counts metric not present", file.nullValueCounts());
          Assert.assertNotNull("Lower bounds metric not present", file.lowerBounds());
          Assert.assertNotNull("Upper bounds metric not present", file.upperBounds());
        }
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "a"),
            new SimpleRecord(5, "b"),
            new SimpleRecord(6, "c"));

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);

    df.withColumn("id", df.col("id").plus(3))
        .select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(targetLocation);

    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testEmptyOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    List<SimpleRecord> expected = records;
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);

    Dataset<Row> empty = spark.createDataFrame(ImmutableList.of(), SimpleRecord.class);
    empty
        .select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Overwrite)
        .option("overwrite-mode", "dynamic")
        .save(targetLocation);

    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "a"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "b"),
            new SimpleRecord(6, "c"));

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);

    // overwrite with 2*id to replace record 2, append 4 and 6
    df.withColumn("id", df.col("id").multiply(2))
        .select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Overwrite)
        .option("overwrite-mode", "dynamic")
        .save(targetLocation);

    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedOverwrite() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);

    // overwrite with the same data; should not produce two copies
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Overwrite)
        .save(targetLocation);

    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedCreateWithTargetFileSizeViaTableProperties() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    table
        .updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(4000);
    for (int i = 0; i < 4000; i++) {
      expected.add(new SimpleRecord(i, "a"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);
    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest :
        SnapshotUtil.latestSnapshot(table, branch).allManifests(table.io())) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        files.add(file);
      }
    }

    Assert.assertEquals("Should have 4 DataFiles", 4, files.size());
    Assert.assertTrue(
        "All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
  }

  @Test
  public void testPartitionedCreateWithTargetFileSizeViaOption() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.NONE);
  }

  @Test
  public void testPartitionedFanoutCreateWithTargetFileSizeViaOption() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.TABLE);
  }

  @Test
  public void testPartitionedFanoutCreateWithTargetFileSizeViaOption2() throws IOException {
    partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType.JOB);
  }

  @Test
  public void testWriteProjection() throws IOException {
    Assume.assumeTrue(
        "Not supported in Spark 3; analysis requires all columns are present",
        spark.version().startsWith("2"));

    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, null), new SimpleRecord(2, null), new SimpleRecord(3, null));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id")
        .write() // select only id column
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);
    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testWriteProjectionWithMiddle() throws IOException {
    Assume.assumeTrue(
        "Not supported in Spark 3; analysis requires all columns are present",
        spark.version().startsWith("2"));

    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Schema schema =
        new Schema(
            optional(1, "c1", Types.IntegerType.get()),
            optional(2, "c2", Types.StringType.get()),
            optional(3, "c3", Types.StringType.get()));
    Table table = tables.create(schema, spec, location.toString());

    List<ThreeColumnRecord> expected =
        Lists.newArrayList(
            new ThreeColumnRecord(1, null, "hello"),
            new ThreeColumnRecord(2, null, "world"),
            new ThreeColumnRecord(3, null, null));

    Dataset<Row> df = spark.createDataFrame(expected, ThreeColumnRecord.class);

    df.select("c1", "c3")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);
    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<ThreeColumnRecord> actual =
        result.orderBy("c1").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testViewsReturnRecentResults() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    Table table = tables.load(location.toString());
    createBranch(table);

    Dataset<Row> query = spark.read().format("iceberg").load(targetLocation).where("id = 1");
    query.createOrReplaceTempView("tmp");

    List<SimpleRecord> actual1 =
        spark.table("tmp").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expected1 = Lists.newArrayList(new SimpleRecord(1, "a"));
    Assert.assertEquals("Number of rows should match", expected1.size(), actual1.size());
    Assert.assertEquals("Result rows should match", expected1, actual1);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(targetLocation);

    List<SimpleRecord> actual2 =
        spark.table("tmp").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expected2 =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(1, "a"));
    Assert.assertEquals("Number of rows should match", expected2.size(), actual2.size());
    Assert.assertEquals("Result rows should match", expected2, actual2);
  }

  public void partitionedCreateWithTargetFileSizeViaOption(IcebergOptionsType option)
      throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "test");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Map<String, String> properties =
        ImmutableMap.of(
            TableProperties.WRITE_DISTRIBUTION_MODE, TableProperties.WRITE_DISTRIBUTION_MODE_NONE);
    Table table = tables.create(SCHEMA, spec, properties, location.toString());

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      expected.add(new SimpleRecord(i, "a"));
      expected.add(new SimpleRecord(i, "b"));
      expected.add(new SimpleRecord(i, "c"));
      expected.add(new SimpleRecord(i, "d"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    switch (option) {
      case NONE:
        df.select("id", "data")
            .sort("data")
            .write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .save(location.toString());
        break;
      case TABLE:
        table.updateProperties().set(SPARK_WRITE_PARTITIONED_FANOUT_ENABLED, "true").commit();
        df.select("id", "data")
            .write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .save(location.toString());
        break;
      case JOB:
        df.select("id", "data")
            .write()
            .format("iceberg")
            .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
            .mode(SaveMode.Append)
            .option(SparkWriteOptions.TARGET_FILE_SIZE_BYTES, 4) // ~4 bytes; low enough to trigger
            .option(SparkWriteOptions.FANOUT_ENABLED, true)
            .save(location.toString());
        break;
      default:
        break;
    }

    createBranch(table);
    table.refresh();

    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);

    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest :
        SnapshotUtil.latestSnapshot(table, branch).allManifests(table.io())) {
      for (DataFile file : ManifestFiles.read(manifest, table.io())) {
        files.add(file);
      }
    }

    Assert.assertEquals("Should have 8 DataFiles", 8, files.size());
    Assert.assertTrue(
        "All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
  }

  @Test
  public void testCommitUnknownException() throws IOException {
    File parent = temp.newFolder(format.toString());
    File location = new File(parent, "commitunknown");
    String targetLocation = locationWithBranch(location);

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, format.toString())
        .mode(SaveMode.Append)
        .save(location.toString());

    createBranch(table);
    table.refresh();

    List<SimpleRecord> records2 =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));

    Dataset<Row> df2 = spark.createDataFrame(records2, SimpleRecord.class);

    AppendFiles append = table.newFastAppend();
    if (branch != null) {
      append.toBranch(branch);
    }

    AppendFiles spyAppend = spy(append);
    doAnswer(
            invocation -> {
              append.commit();
              throw new CommitStateUnknownException(new RuntimeException("Datacenter on Fire"));
            })
        .when(spyAppend)
        .commit();

    Table spyTable = spy(table);
    when(spyTable.newAppend()).thenReturn(spyAppend);
    SparkTable sparkTable = new SparkTable(spyTable, false);

    String manualTableName = "unknown_exception";
    ManualSource.setTable(manualTableName, sparkTable);

    // Although an exception is thrown here, write and commit have succeeded
    AssertHelpers.assertThrowsWithCause(
        "Should throw a Commit State Unknown Exception",
        SparkException.class,
        "Writing job aborted",
        CommitStateUnknownException.class,
        "Datacenter on Fire",
        () ->
            df2.select("id", "data")
                .sort("data")
                .write()
                .format("org.apache.iceberg.spark.source.ManualSource")
                .option(ManualSource.TABLE_NAME, manualTableName)
                .mode(SaveMode.Append)
                .save(targetLocation));

    // Since write and commit succeeded, the rows should be readable
    Dataset<Row> result = spark.read().format("iceberg").load(targetLocation);
    List<SimpleRecord> actual =
        result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals(
        "Number of rows should match", records.size() + records2.size(), actual.size());
    Assertions.assertThat(actual)
        .describedAs("Result rows should match")
        .containsExactlyInAnyOrder(
            ImmutableList.<SimpleRecord>builder()
                .addAll(records)
                .addAll(records2)
                .build()
                .toArray(new SimpleRecord[0]));
  }

  public enum IcebergOptionsType {
    NONE,
    TABLE,
    JOB
  }

  private String locationWithBranch(File location) {
    if (branch == null) {
      return location.toString();
    }

    return location + "#branch_" + branch;
  }

  private void createBranch(Table table) {
    if (branch != null && !branch.equals(SnapshotRef.MAIN_BRANCH)) {
      table.manageSnapshots().createBranch(branch, table.currentSnapshot().snapshotId()).commit();
    }
  }
}
