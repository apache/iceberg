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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.TestBase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Timeout;

@Fork(1)
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1, timeUnit = TimeUnit.HOURS)
public abstract class IcebergCompactionBenchmark {

  private final Configuration hadoopConf = initHadoopConf();
  private SparkSession spark;

  protected abstract String tableName();

  protected abstract void initTable();

  protected abstract void appendData();

  @Setup
  public void setupBench() {
    setupSpark();
  }

  @TearDown
  public void teardownBench() {
    tearDownSpark();
  }

  @Setup(Level.Iteration)
  public void setupIteration() {
    initTable();
    appendData();
  }

  @TearDown(Level.Iteration)
  public void cleanUpIteration() throws IOException {
    cleanupFiles();
  }

  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected final Table table() {
    try {
      return Spark3Util.loadIcebergTable(spark(), tableName());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected final SparkSession spark() {
    return spark;
  }

  protected String getCatalogWarehouse() {
    try {
      return Files.createTempDirectory("benchmark-").toAbsolutePath()
          + "/"
          + UUID.randomUUID()
          + "/";
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  protected void cleanupFiles() throws IOException {
    spark.sql("DROP TABLE IF EXISTS " + tableName());
  }

  /**
   * Returns extra properties added to the Spark catalog during session setup.
   *
   * <p>The default implementation returns {@code type=hadoop} for a local Hadoop catalog. Override
   * to switch the catalog implementation or FileIO, e.g., with {@code catalog-impl}, {@code
   * io-impl}, and {@code warehouse} for an S3-backed run.
   *
   * @return a map of catalog properties to apply
   */
  protected Map<String, String> extraCatalogProperties() {
    return Map.of("type", "hadoop");
  }

  protected String sparkMaster() {
    return "local[*]";
  }

  protected void setupSpark() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.warehouse", getCatalogWarehouse())
            .config(TestBase.DISABLE_UI)
            .master(sparkMaster());
    extraCatalogProperties()
        .forEach((key, value) -> builder.config("spark.sql.catalog.spark_catalog." + key, value));
    hadoopConf.forEach(entry -> builder.config("spark.hadoop." + entry.getKey(), entry.getValue()));
    spark = builder.getOrCreate();
  }

  protected void tearDownSpark() {
    spark.stop();
  }

  protected void writeData(Dataset<Row> df) {
    df.write().format("iceberg").mode(SaveMode.Append).save(tableName());
  }
}
