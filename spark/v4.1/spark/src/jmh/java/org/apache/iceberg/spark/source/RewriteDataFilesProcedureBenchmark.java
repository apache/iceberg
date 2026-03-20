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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
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
 * Benchmark to evaluate performance of spark reader sync (current) vs async.
 *
 * <p>Requires S3 tables to be pre-populated once before running.
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

  private static final String WAREHOUSE = "s3a://iceberg-spark-readers-poc/iceberg-benchmark";
  private static final String CATALOG = "iceberg";

  @Param({"true", "false"})
  private boolean isAsyncEnabled;

  private SparkSession spark;
  private Table table;

  private String tableName;

  private String bareTableName;

  @Setup
  public void setupBenchmark() throws ParseException {
    this.bareTableName = isAsyncEnabled ? "test_table_async_on" : "test_table_async_off";
    this.tableName = CATALOG + "." + bareTableName;

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
            .config("spark.sql.catalog." + CATALOG, SparkCatalog.class.getName())
            .config(
                "spark.sql.catalog." + CATALOG + ".catalog-impl",
                "org.apache.iceberg.jdbc.JdbcCatalog")
            .config(
                "spark.sql.catalog." + CATALOG + ".uri",
                "jdbc:sqlite:/tmp/iceberg-benchmark-catalog.db")
            .config("spark.sql.catalog." + CATALOG + ".warehouse", WAREHOUSE)
            .config(
                "spark.sql.catalog." + CATALOG + ".io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog." + CATALOG + ".http-client.apache.max-connections", "200")
            .master("local[*]")
            .getOrCreate();

    try {
      this.table = Spark3Util.loadIcebergTable(spark, tableName);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchTableException e) {
      throw new RuntimeException(
          "Table not found: " + tableName + ". Run GenerateParquetData first.", e);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    spark.stop();
  }

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles(Blackhole blackhole) {
    long snapshotId = currentSnapshotId();
    blackhole.consume(
        sql("CALL %s.system.rewrite_data_files(table => '%s')", CATALOG, bareTableName));
    rollback(snapshotId);
  }

  private long currentSnapshotId() {
    table.refresh();
    return table.currentSnapshot().snapshotId();
  }

  private void rollback(long snapshotId) {
    table.manageSnapshots().rollbackTo(snapshotId).commit();
  }

  @FormatMethod
  private Object sql(@FormatString String query, Object... args) {
    return spark.sql(String.format(query, args));
  }
}
