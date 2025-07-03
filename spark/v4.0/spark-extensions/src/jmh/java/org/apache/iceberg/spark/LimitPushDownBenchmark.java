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
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
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
 * A benchmark that evaluates the limit push down performance.
 *
 * <p>To run this benchmark for spark-4.0: <code>
 *   ./gradlew -DsparkVersions=4.0 :iceberg-spark:iceberg-spark-extensions-4.0_2.13:jmh
 *       -PjmhIncludeRegex=LimitPushDownBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-limit-push-down-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.AverageTime)
public class LimitPushDownBenchmark {

  private static final String TABLE_NAME = "test_limit_table";

  @Param({"100", "1000", "10000"})
  private int limitValue;

  @Param({"true", "false"})
  private boolean limitPushDownEnabled;

  private SparkSession spark;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException {
    setupSpark();
    setupTable();
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
    tearDownSpark();
  }

  @Benchmark
  @Threads(1)
  public void limitQuery(Blackhole blackhole) {
    spark
        .conf()
        .set("spark.sql.iceberg.limit-push-down.enabled", String.valueOf(limitPushDownEnabled));

    Dataset<Row> result =
        spark.sql(
            String.format(Locale.ROOT, "SELECT * FROM local.%s LIMIT %d", TABLE_NAME, limitValue));

    blackhole.consume(result.count());
  }

  @Benchmark
  @Threads(1)
  public void limitQueryWithPartitionPruning(Blackhole blackhole) {
    spark
        .conf()
        .set("spark.sql.iceberg.limit-push-down.enabled", String.valueOf(limitPushDownEnabled));

    Dataset<Row> result =
        spark.sql(
            String.format(
                Locale.ROOT,
                "SELECT * FROM local.%s WHERE category != '0' LIMIT %d",
                TABLE_NAME,
                limitValue));

    blackhole.consume(result.count());
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .appName("limit-push-down-benchmark")
            .master("local[1]")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", "/tmp/iceberg-benchmark-warehouse")
            .getOrCreate();
  }

  private void setupTable() {
    sql("DROP TABLE IF EXISTS local.%s PURGE", TABLE_NAME);

    sql(
        "CREATE TABLE local.%s (id BIGINT, data STRING, value DOUBLE, category STRING) "
            + "USING iceberg PARTITIONED BY (category)",
        TABLE_NAME);

    // Insert substantial data across multiple partitions
    for (int partition = 0; partition < 100; partition++) {
      StringBuilder values = new StringBuilder();
      for (int row = 0; row < 5000; row++) {
        int id = partition * 5000 + row;
        if (values.length() > 0) {
          values.append(", ");
        }
        values.append(
            String.format(
                Locale.ROOT,
                "(%d, 'data_%d', %f, 'category_%d')",
                id,
                id,
                Math.random() * 1000,
                partition));
      }
      sql("INSERT INTO local.%s VALUES %s", TABLE_NAME, values.toString());
    }
  }

  private void dropTable() {
    sql("DROP TABLE IF EXISTS local.%s PURGE", TABLE_NAME);
  }

  private void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
