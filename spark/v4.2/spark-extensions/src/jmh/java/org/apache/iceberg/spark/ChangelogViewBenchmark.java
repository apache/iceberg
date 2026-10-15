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

import java.net.URI;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.SparkSession;
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
        rowCount % REVERT_INTERVAL == 0, "Row count must be divisible by 20");
    this.warehouse = Files.createTempDirectory("changelog-benchmark-").toUri();
    this.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.bench", SparkCatalog.class.getName())
            .config("spark.sql.catalog.bench.type", "hadoop")
            .config("spark.sql.catalog.bench.warehouse", warehouse.toString())
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");
    spark.sql("CREATE NAMESPACE IF NOT EXISTS bench.default");
    spark.sql(
        "CREATE TABLE "
            + TABLE
            + " USING iceberg TBLPROPERTIES ('format-version'='2') AS "
            + inputQuery("0"));
    long startSnapshot = Spark3Util.loadIcebergTable(spark, TABLE).currentSnapshot().snapshotId();
    spark.sql(
        "INSERT OVERWRITE "
            + TABLE
            + " "
            + inputQuery("IF(id % " + CHANGE_INTERVAL + " = 0, 1, 0)"));
    spark.sql(
        "INSERT OVERWRITE "
            + TABLE
            + " "
            + inputQuery("IF(id % " + REVERT_INTERVAL + " = " + CHANGE_INTERVAL + ", 1, 0)"));
    long endSnapshot = Spark3Util.loadIcebergTable(spark, TABLE).currentSnapshot().snapshotId();
    String options =
        switch (operation) {
          case "updates" -> "compute_updates => true, identifier_columns => array('id')";
          case "carryovers" -> "compute_updates => false";
          case "net" -> "compute_updates => false, net_changes => true";
          default -> throw new IllegalArgumentException("Unknown operation: " + operation);
        };
    this.call =
        "CALL bench.system.create_changelog_view(table => '"
            + TABLE
            + "', changelog_view => '"
            + VIEW
            + "', options => map('start-snapshot-id', '"
            + startSnapshot
            + "', 'end-snapshot-id', '"
            + endSnapshot
            + "'), "
            + options
            + ")";
    this.expectedRows =
        operation.equals("net")
            ? 2 * (rowCount / CHANGE_INTERVAL - rowCount / REVERT_INTERVAL)
            : 2 * (rowCount / CHANGE_INTERVAL + rowCount / REVERT_INTERVAL);
    createAndReadChangelog();
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
        spark.sql("DROP VIEW IF EXISTS " + VIEW);
        spark.sql("DROP TABLE IF EXISTS " + TABLE + " PURGE");
        FileSystem.get(warehouse, spark.sparkContext().hadoopConfiguration())
            .delete(new Path(warehouse), true);
      } finally {
        spark.stop();
      }
    }
  }

  private String inputQuery(String revision) {
    StringBuilder query = new StringBuilder("SELECT id, ").append(revision).append(" AS revision");
    for (int index = 0; index < payloadColumns; index++) {
      query
          .append(", sha2(concat(cast(id AS STRING), '")
          .append(index)
          .append("'), 256) AS payload_")
          .append(index);
    }

    return query.append(" FROM range(0, ").append(rowCount).append(", 1, 4)").toString();
  }
}
