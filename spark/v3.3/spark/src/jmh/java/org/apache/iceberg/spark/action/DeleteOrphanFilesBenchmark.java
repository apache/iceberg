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
package org.apache.iceberg.spark.action;

import static org.apache.spark.sql.functions.lit;

import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DeleteOrphanFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of remove orphan files action in Spark.
 *
 * <p>To run this benchmark for spark-3.3: <code>
 *   ./gradlew -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3_2.12:jmh
 *       -PjmhIncludeRegex=DeleteOrphanFilesBenchmark
 *       -PjmhOutputPath=benchmark/delete-orphan-files-benchmark-results.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class DeleteOrphanFilesBenchmark {

  private static final String TABLE_NAME = "delete_orphan_perf";
  private static final int NUM_SNAPSHOTS = 1000;
  private static final int NUM_FILES = 1000;

  private SparkSession spark;
  private final List<String> orphanPaths = Lists.newArrayList();
  private Table table;

  @Setup
  public void setupBench() {
    setupSpark();
    initTable();
    appendData();
  }

  @TearDown
  public void teardownBench() {
    tearDownSpark();
  }

  @TearDown(Level.Iteration)
  public void cleanUpIteration() {
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void testDeleteOrphanFiles(Blackhole blackhole) {
    Timestamp timestamp = new Timestamp(10000);
    Dataset<Row> orphanFilesDF =
        spark
            .createDataset(orphanPaths, Encoders.STRING())
            .withColumnRenamed("value", "file_path")
            .withColumn("last_modified", lit(timestamp));

    DeleteOrphanFiles.Result results =
        SparkActions.get(spark)
            .deleteOrphanFiles(table())
            .compareToFileList(orphanFilesDF)
            .execute();
    blackhole.consume(results);
  }

  private void initTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s(id INT, name STRING)"
                + " USING ICEBERG"
                + " TBLPROPERTIES ( 'format-version' = '2',"
                + " 'compatibility.snapshot-id-inheritance.enabled' = 'true')",
            TABLE_NAME));
  }

  private void appendData() {
    String location = table().location();
    PartitionSpec partitionSpec = table().spec();

    for (int i = 0; i < NUM_SNAPSHOTS; i++) {
      AppendFiles appendFiles = table().newFastAppend();
      for (int j = 0; j < NUM_FILES; j++) {
        String path = String.format("%s/path/to/data-%d-%d.parquet", location, i, j);
        orphanPaths.add(path);
        DataFile dataFile =
            DataFiles.builder(partitionSpec)
                .withPath(path)
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build();
        appendFiles.appendFile(dataFile);
      }
      appendFiles.commit();
    }
  }

  private Table table() {
    if (table == null) {
      try {
        table = Spark3Util.loadIcebergTable(spark(), TABLE_NAME);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    return table;
  }

  private SparkSession spark() {
    return spark;
  }

  private String catalogWarehouse() {
    return Files.createTempDir().getAbsolutePath() + "/" + UUID.randomUUID() + "/";
  }

  private void cleanupFiles() {
    spark.sql("DROP TABLE IF EXISTS " + TABLE_NAME);
  }

  private void setupSpark() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", catalogWarehouse())
            .master("local");
    spark = builder.getOrCreate();
  }

  private void tearDownSpark() {
    spark.stop();
  }
}
