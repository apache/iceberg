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

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmark to evaluate performance of spark reader sync (current) vs async
 *
 * <p>To run this benchmark for spark-4.1: <code>
 *   ./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh
 *       -PjmhIncludeRegex=RewriteDataFilesProcedureBenchmark
 *       -PjmhOutputPath=benchmark/rewrite-data-files-procedure-benchmark.txt
 * </code>
 */
@Fork(
    value = 1,
    jvmArgs = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
    })
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class RewriteDataFilesProcedureBenchmark {

  private static final String TABLE_NAME = "test_table";

  @Param({"100", "500", "1000"})
  private int numFiles;

  @Param({"true", "false"})
  private boolean isAsyncEnabled;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;
  private Table table;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException {
    setupSpark();
    createTable();
    insertData(numFiles);
    this.table = Spark3Util.loadIcebergTable(spark, TABLE_NAME);
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
    tearDownSpark();
  }

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles(Blackhole blackhole) {
    long snapshotId = currentSnapshotId();
    blackhole.consume(
        sql("CALL spark_catalog.system.rewrite_data_files(table => '%s')", TABLE_NAME));
    rollback(snapshotId);
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(
                "spark.driver.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .config(
                "spark.executor.extraJavaOptions",
                "--add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", newWarehouseDir())
            .master("local[*]")
            .getOrCreate();
  }

  private void tearDownSpark() {
    spark.stop();
  }

  private void createTable() {
    sql(
        "CREATE TABLE %s (c1 int, c2 string, c3 string) USING iceberg TBLPROPERTIES ('%s' = '%s')",
        TABLE_NAME, TableProperties.ASYNC_READER_ENABLED, isAsyncEnabled);
  }

  private void insertData(int filesCount) {
    //    Making approx 2000 rows per file
    long totalRecords = (long) filesCount * 2000;
    Dataset<Row> df =
        spark
            .range(0, totalRecords)
            .withColumn("c1", col("id").cast("int"))
            .withColumn("c2", concat(lit("foo_"), col("id")))
            .withColumn("c3", lit(null).cast("string"))
            .drop("id")
            .repartition(filesCount);
    try {
      df.writeTo(TABLE_NAME).append();
    } catch (NoSuchTableException e) {
      throw new RuntimeException(e);
    }
  }

  private long currentSnapshotId() {
    table.refresh();
    return table.currentSnapshot().snapshotId();
  }

  private void rollback(long snapshotId) {
    table.manageSnapshots().rollbackTo(snapshotId).commit();
  }

  private void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", TABLE_NAME);
  }

  private String newWarehouseDir() {
    return hadoopConf.get("hadoop.tmp.dir") + UUID.randomUUID();
  }

  @FormatMethod
  private Object sql(@FormatString String query, Object... args) {
    return spark.sql(String.format(query, args));
  }
}
