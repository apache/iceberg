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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.math.LongMath;
import org.apache.iceberg.spark.CommitMetadata;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestDataSourceOptions extends TestBaseWithCatalog {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static SparkSession spark = null;

  @TempDir private Path temp;

  @BeforeAll
  public static void startSpark() {
    TestDataSourceOptions.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestDataSourceOptions.spark;
    TestDataSourceOptions.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testWriteFormatOptionOverridesTableProperties() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option(SparkWriteOptions.WRITE_FORMAT, "parquet")
        .mode(SaveMode.Append)
        .save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(
          task -> {
            FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
            assertThat(fileFormat).isEqualTo(FileFormat.PARQUET);
          });
    }
  }

  @Test
  public void testNoWriteFormatOption() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.DEFAULT_FILE_FORMAT, "avro");
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> df = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    df.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(
          task -> {
            FileFormat fileFormat = FileFormat.fromFileName(task.file().path());
            assertThat(fileFormat).isEqualTo(FileFormat.AVRO);
          });
    }
  }

  @Test
  public void testHadoopOptions() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    String originalDefaultFS = sparkHadoopConf.get("fs.default.name");

    try {
      HadoopTables tables = new HadoopTables(CONF);
      PartitionSpec spec = PartitionSpec.unpartitioned();
      Map<String, String> options = Maps.newHashMap();
      tables.create(SCHEMA, spec, options, tableLocation);

      // set an invalid value for 'fs.default.name' in Spark Hadoop config
      // to verify that 'hadoop.' data source options are propagated correctly
      sparkHadoopConf.set("fs.default.name", "hdfs://localhost:9000");

      List<SimpleRecord> expectedRecords =
          Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
      Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
      originalDf
          .select("id", "data")
          .write()
          .format("iceberg")
          .mode("append")
          .option("hadoop.fs.default.name", "file:///")
          .save(tableLocation);

      Dataset<Row> resultDf =
          spark
              .read()
              .format("iceberg")
              .option("hadoop.fs.default.name", "file:///")
              .load(tableLocation);
      List<SimpleRecord> resultRecords =
          resultDf.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();

      assertThat(resultRecords).as("Records should match").isEqualTo(expectedRecords);
    } finally {
      sparkHadoopConf.set("fs.default.name", originalDefaultFS);
    }
  }

  @Test
  public void testSplitOptionsOverridesTableProperties() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    options.put(TableProperties.SPLIT_SIZE, String.valueOf(128L * 1024 * 1024)); // 128Mb
    options.put(
        TableProperties.DEFAULT_FILE_FORMAT,
        String.valueOf(FileFormat.AVRO)); // Arbitrarily splittable
    Table icebergTable = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf
        .select("id", "data")
        .repartition(1)
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<DataFile> files =
        Lists.newArrayList(icebergTable.currentSnapshot().addedDataFiles(icebergTable.io()));
    assertThat(files).as("Should have written 1 file").hasSize(1);

    long fileSize = files.get(0).fileSizeInBytes();
    long splitSize = LongMath.divide(fileSize, 2, RoundingMode.CEILING);

    Dataset<Row> resultDf =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SPLIT_SIZE, String.valueOf(splitSize))
            .load(tableLocation);

    assertThat(resultDf.javaRDD().getNumPartitions())
        .as("Spark partitions should match")
        .isEqualTo(2);
  }

  @Test
  public void testIncrementalScanOptions() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    for (SimpleRecord record : expectedRecords) {
      Dataset<Row> originalDf =
          spark.createDataFrame(Lists.newArrayList(record), SimpleRecord.class);
      originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);
    }
    List<Long> snapshotIds = SnapshotUtil.currentAncestorIds(table);

    // start-snapshot-id and snapshot-id are both configured.
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option("snapshot-id", snapshotIds.get(3).toString())
                    .option("start-snapshot-id", snapshotIds.get(3).toString())
                    .load(tableLocation)
                    .explain())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set start-snapshot-id and end-snapshot-id for incremental scans when either snapshot-id or as-of-timestamp is set");

    // end-snapshot-id and as-of-timestamp are both configured.
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(
                        SparkReadOptions.AS_OF_TIMESTAMP,
                        Long.toString(table.snapshot(snapshotIds.get(3)).timestampMillis()))
                    .option("end-snapshot-id", snapshotIds.get(2).toString())
                    .load(tableLocation)
                    .explain())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set start-snapshot-id and end-snapshot-id for incremental scans when either snapshot-id or as-of-timestamp is set");

    // only end-snapshot-id is configured.
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option("end-snapshot-id", snapshotIds.get(2).toString())
                    .load(tableLocation)
                    .explain())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot set only end-snapshot-id for incremental scans. Please, set start-snapshot-id too.");

    // test (1st snapshot, current snapshot] incremental scan.
    List<SimpleRecord> result =
        spark
            .read()
            .format("iceberg")
            .option("start-snapshot-id", snapshotIds.get(3).toString())
            .load(tableLocation)
            .orderBy("id")
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();
    assertThat(result).as("Records should match").isEqualTo(expectedRecords.subList(1, 4));

    // test (2nd snapshot, 3rd snapshot] incremental scan.
    Dataset<Row> resultDf =
        spark
            .read()
            .format("iceberg")
            .option("start-snapshot-id", snapshotIds.get(2).toString())
            .option("end-snapshot-id", snapshotIds.get(1).toString())
            .load(tableLocation);
    List<SimpleRecord> result1 =
        resultDf.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(result1).as("Records should match").isEqualTo(expectedRecords.subList(2, 3));
    assertThat(resultDf.count()).as("Unprocessed count should match record count").isEqualTo(1);
  }

  @Test
  public void testMetadataSplitSizeOptionOverrideTableProperties() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table table = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    // produce 1st manifest
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);
    // produce 2nd manifest
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());

    assertThat(manifests).as("Must be 2 manifests").hasSize(2);

    // set the target metadata split size so each manifest ends up in a separate split
    table
        .updateProperties()
        .set(TableProperties.METADATA_SPLIT_SIZE, String.valueOf(manifests.get(0).length()))
        .commit();

    Dataset<Row> entriesDf = spark.read().format("iceberg").load(tableLocation + "#entries");
    assertThat(entriesDf.javaRDD().getNumPartitions()).as("Num partitions must match").isEqualTo(2);

    // override the table property using options
    entriesDf =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.SPLIT_SIZE, String.valueOf(128 * 1024 * 1024))
            .load(tableLocation + "#entries");
    assertThat(entriesDf.javaRDD().getNumPartitions()).as("Num partitions must match").isEqualTo(1);
  }

  @Test
  public void testDefaultMetadataSplitSize() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Map<String, String> options = Maps.newHashMap();
    Table icebergTable = tables.create(SCHEMA, spec, options, tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    int splitSize = (int) TableProperties.METADATA_SPLIT_SIZE_DEFAULT; // 32MB split size

    int expectedSplits =
        ((int)
                    tables
                        .load(tableLocation + "#entries")
                        .currentSnapshot()
                        .allManifests(icebergTable.io())
                        .get(0)
                        .length()
                + splitSize
                - 1)
            / splitSize;

    Dataset<Row> metadataDf = spark.read().format("iceberg").load(tableLocation + "#entries");

    int partitionNum = metadataDf.javaRDD().getNumPartitions();
    assertThat(partitionNum).as("Spark partitions should match").isEqualTo(expectedSplits);
  }

  @Test
  public void testExtraSnapshotMetadata() throws IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();
    HadoopTables tables = new HadoopTables(CONF);
    tables.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf
        .select("id", "data")
        .write()
        .format("iceberg")
        .mode("append")
        .option(SparkWriteOptions.SNAPSHOT_PROPERTY_PREFIX + ".extra-key", "someValue")
        .option(SparkWriteOptions.SNAPSHOT_PROPERTY_PREFIX + ".another-key", "anotherValue")
        .save(tableLocation);

    Table table = tables.load(tableLocation);

    assertThat(table.currentSnapshot().summary().get("extra-key")).isEqualTo("someValue");
    assertThat(table.currentSnapshot().summary().get("another-key")).isEqualTo("anotherValue");
  }

  @Test
  public void testExtraSnapshotMetadataWithSQL() throws InterruptedException, IOException {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();
    HadoopTables tables = new HadoopTables(CONF);

    Table table =
        tables.create(SCHEMA, PartitionSpec.unpartitioned(), Maps.newHashMap(), tableLocation);

    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(new SimpleRecord(1, "a"), new SimpleRecord(2, "b"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);
    spark.read().format("iceberg").load(tableLocation).createOrReplaceTempView("target");
    Thread writerThread =
        new Thread(
            () -> {
              Map<String, String> properties =
                  ImmutableMap.of(
                      "writer-thread",
                      String.valueOf(Thread.currentThread().getName()),
                      SnapshotSummary.EXTRA_METADATA_PREFIX + "extra-key",
                      "someValue",
                      SnapshotSummary.EXTRA_METADATA_PREFIX + "another-key",
                      "anotherValue");
              CommitMetadata.withCommitProperties(
                  properties,
                  () -> {
                    spark.sql("INSERT INTO target VALUES (3, 'c'), (4, 'd')");
                    return 0;
                  },
                  RuntimeException.class);
            });
    writerThread.setName("test-extra-commit-message-writer-thread");
    writerThread.start();
    writerThread.join();

    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());
    assertThat(snapshots).hasSize(2);
    assertThat(snapshots.get(0).summary().get("writer-thread")).isNull();
    assertThat(snapshots.get(1).summary())
        .containsEntry("writer-thread", "test-extra-commit-message-writer-thread")
        .containsEntry("extra-key", "someValue")
        .containsEntry("another-key", "anotherValue");
  }

  @Test
  public void testExtraSnapshotMetadataWithDelete()
      throws InterruptedException, NoSuchTableException {
    spark.sessionState().conf().setConfString("spark.sql.shuffle.partitions", "1");
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    List<SimpleRecord> expectedRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.repartition(5, new Column("data")).select("id", "data").writeTo(tableName).append();
    Thread writerThread =
        new Thread(
            () -> {
              Map<String, String> properties =
                  ImmutableMap.of(
                      "writer-thread",
                      String.valueOf(Thread.currentThread().getName()),
                      SnapshotSummary.EXTRA_METADATA_PREFIX + "extra-key",
                      "someValue",
                      SnapshotSummary.EXTRA_METADATA_PREFIX + "another-key",
                      "anotherValue");
              CommitMetadata.withCommitProperties(
                  properties,
                  () -> {
                    spark.sql("DELETE FROM " + tableName + " where id = 1");
                    return 0;
                  },
                  RuntimeException.class);
            });
    writerThread.setName("test-extra-commit-message-delete-thread");
    writerThread.start();
    writerThread.join();

    Table table = validationCatalog.loadTable(tableIdent);
    List<Snapshot> snapshots = Lists.newArrayList(table.snapshots());

    assertThat(snapshots).hasSize(2);
    assertThat(snapshots.get(0).summary().get("writer-thread")).isNull();
    assertThat(snapshots.get(1).summary())
        .containsEntry("writer-thread", "test-extra-commit-message-delete-thread")
        .containsEntry("extra-key", "someValue")
        .containsEntry("another-key", "anotherValue");
  }
}
