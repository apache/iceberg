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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewritePlanner;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
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

/**
 * A benchmark that evaluates the performance of the rewrite data files action in Spark.
 *
 * <p>To run this benchmark for spark-4.1: <code>
 *   ./gradlew :iceberg-spark:iceberg-spark-4.1_2.13:jmh
 *       -PjmhIncludeRegex=IcebergCompactionBenchmark.rewriteDataFiles
 *       -PjmhOutputPath=benchmark/compaction-benchmark-1000files-2000rows-v3.txt
 *       -PjmhJsonOutputPath=benchmark/compaction-benchmark-1000files-2000rows-v3.json
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class IcebergCompactionBenchmark {

  private static final String[] NAMESPACE = new String[] {"default"};
  private static final String NAME = "compactbench";
  private static final Identifier IDENT = Identifier.of(NAMESPACE, NAME);
  private static final int NUM_FILES = 1000;
  private static final long NUM_ROWS = 2000;

  private final Configuration hadoopConf = initHadoopConf();
  private SparkSession spark;

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

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewritePlanner.REWRITE_ALL, "true")
        .execute();
  }

  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected final void initTable() {
    Schema schema =
        new Schema(
            required(1, "intCol", Types.IntegerType.get()),
            required(2, "stringCol", Types.StringType.get()),
            optional(3, "nullCol", Types.StringType.get()));

    SparkSessionCatalog<?> catalog;
    try {
      catalog =
          (SparkSessionCatalog<?>)
              Spark3Util.catalogAndIdentifier(spark(), "spark_catalog").catalog();
      catalog.dropTable(IDENT);
      catalog.createTable(
          IDENT, SparkSchemaUtil.convert(schema), new Transform[0], Collections.emptyMap());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void appendData() {
    Dataset<Row> df =
        spark()
            .range(0, NUM_FILES * NUM_ROWS)
            .withColumn("intCol", col("id").cast("int"))
            .withColumn("stringCol", concat(lit("foo_"), col("id")))
            .withColumn("nullCol", lit(null).cast("string"))
            .drop("id")
            .repartition(NUM_FILES);
    writeData(df);
  }

  private void writeData(Dataset<Row> df) {
    df.write().format("iceberg").mode(SaveMode.Append).save(NAME);
  }

  protected final Table table() {
    try {
      return Spark3Util.loadIcebergTable(spark(), NAME);
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
    spark.sql("DROP TABLE IF EXISTS " + NAME);
  }

  protected void setupSpark() {
    SparkSession.Builder builder =
        SparkSession.builder()
            .config(
                "spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", getCatalogWarehouse())
            .config(TestBase.DISABLE_UI)
            .master("local[*]");
    spark = builder.getOrCreate();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.forEach(entry -> sparkHadoopConf.set(entry.getKey(), entry.getValue()));
  }

  protected void tearDownSpark() {
    spark.stop();
  }
}
