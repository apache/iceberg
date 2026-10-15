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
package org.apache.iceberg.spark;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.net.URI;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of the changelog view iterators.
 *
 * <p>To run this benchmark for spark-4.2: <code>
 *   ./gradlew -DsparkVersions=4.2 :iceberg-spark:iceberg-spark-extensions-4.2_2.13:jmh
 *       -PjmhIncludeRegex=ChangelogViewBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-changelog-view-benchmark.txt
 * </code>
 */
@Fork(2)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
public class ChangelogViewBenchmark {
  private static final String TABLE = "bench.default.changelog_benchmark";
  private static final String VIEW = "changelog_benchmark_view";
  private static final int CHANGE_INTERVAL = 10;
  private static final int REVERT_INTERVAL = 20;

  @Param({"100000"})
  public long rowCount;

  @Param({"1", "20"})
  public int payloadColumns;

  @Param({"carryovers", "updates", "net"})
  public String operation;

  private SparkSession spark;
  private URI warehouse;
  private String call;
  private long expectedRows;

  @Setup
  public void setup() throws Exception {
    Preconditions.checkArgument(
        rowCount % REVERT_INTERVAL == 0, "Row count must be divisible by %s", REVERT_INTERVAL);
    this.warehouse = Files.createTempDirectory("changelog-benchmark-").toUri();
    this.spark =
        SparkSession.builder()
            .master("local[2]")
            .config(TestBase.DISABLE_UI)
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.bench", SparkCatalog.class.getName())
            .config("spark.sql.catalog.bench.type", "hadoop")
            .config("spark.sql.catalog.bench.warehouse", warehouse.toString())
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "4")
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    sql("CREATE NAMESPACE IF NOT EXISTS bench.default");
    sql(
        "CREATE TABLE %s USING iceberg TBLPROPERTIES ('format-version'='2') AS %s",
        TABLE, inputQuery("0"));
    long startSnapshot = Spark3Util.loadIcebergTable(spark, TABLE).currentSnapshot().snapshotId();
    sql("INSERT OVERWRITE %s %s", TABLE, inputQuery("IF(id % " + CHANGE_INTERVAL + " = 0, 1, 0)"));
    sql(
        "INSERT OVERWRITE %s %s",
        TABLE, inputQuery("IF(id % " + REVERT_INTERVAL + " = " + CHANGE_INTERVAL + ", 1, 0)"));
    long endSnapshot = Spark3Util.loadIcebergTable(spark, TABLE).currentSnapshot().snapshotId();
    String options =
        switch (operation) {
          case "updates" -> "compute_updates => true, identifier_columns => array('id')";
          case "carryovers" -> "compute_updates => false";
          case "net" -> "compute_updates => false, net_changes => true";
          default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        };
    this.call =
        String.format(
            "CALL bench.system.create_changelog_view(table => '%s', changelog_view => '%s',"
                + " options => map('start-snapshot-id', '%d', 'end-snapshot-id', '%d'), %s)",
            TABLE, VIEW, startSnapshot, endSnapshot, options);
    this.expectedRows =
        operation.equals("net")
            ? 2 * (rowCount / CHANGE_INTERVAL - rowCount / REVERT_INTERVAL)
            : 2 * (rowCount / CHANGE_INTERVAL + rowCount / REVERT_INTERVAL);
  }

  @Benchmark
  public long createAndReadChangelog() {
    spark.sql(call).collectAsList();
    long count = spark.table(VIEW).count();
    Preconditions.checkState(
        count == expectedRows, "Expected %s rows, found %s", expectedRows, count);
    return count;
  }

  @TearDown
  public void tearDown() throws Exception {
    if (spark != null) {
      try {
        sql("DROP VIEW IF EXISTS %s", VIEW);
        sql("DROP TABLE IF EXISTS %s PURGE", TABLE);
        FileSystem.get(warehouse, spark.sparkContext().hadoopConfiguration())
            .delete(new Path(warehouse), true);
      } finally {
        spark.stop();
      }
    }
  }

  private String inputQuery(String revision) {
    String payload =
        IntStream.range(0, payloadColumns)
            .mapToObj(
                index ->
                    String.format(
                        ", sha2(concat(cast(id AS STRING), '%d'), 256) AS payload_%d", index, index))
            .collect(Collectors.joining());
    return String.format(
        "SELECT id, %s AS revision%s FROM range(0, %d, 1, 4)", revision, payload, rowCount);
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
