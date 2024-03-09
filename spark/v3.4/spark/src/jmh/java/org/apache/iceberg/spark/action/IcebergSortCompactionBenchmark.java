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
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.NullOrder;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.SizeBasedFileRewriter;
import org.apache.iceberg.relocated.com.google.common.io.Files;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
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

@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
public class IcebergSortCompactionBenchmark {

  private static final String[] NAMESPACE = new String[] {"default"};
  private static final String NAME = "sortbench";
  private static final Identifier IDENT = Identifier.of(NAMESPACE, NAME);
  private static final int NUM_FILES = 8;
  private static final long NUM_ROWS = 7500000L;
  private static final long UNIQUE_VALUES = NUM_ROWS / 4;

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
  public void sortInt() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortInt2() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol2", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortInt3() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol2", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol3", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol4", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortInt4() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol2", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol3", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol4", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortString() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("stringCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortFourColumns() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("stringCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("dateCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .sortBy("doubleCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void sortSixColumns() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .sort(
            SortOrder.builderFor(table().schema())
                .sortBy("stringCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("intCol", SortDirection.ASC, NullOrder.NULLS_FIRST)
                .sortBy("dateCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .sortBy("timestampCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .sortBy("doubleCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .sortBy("longCol", SortDirection.DESC, NullOrder.NULLS_FIRST)
                .build())
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortInt() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("intCol")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortInt2() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("intCol", "intCol2")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortInt3() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("intCol", "intCol2", "intCol3")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortInt4() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("intCol", "intCol2", "intCol3", "intCol4")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortString() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("stringCol")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortFourColumns() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("stringCol", "intCol", "dateCol", "doubleCol")
        .execute();
  }

  @Benchmark
  @Threads(1)
  public void zSortSixColumns() {
    SparkActions.get()
        .rewriteDataFiles(table())
        .option(SizeBasedFileRewriter.REWRITE_ALL, "true")
        .zOrder("stringCol", "intCol", "dateCol", "timestampCol", "doubleCol", "longCol")
        .execute();
  }

  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected final void initTable() {
    Schema schema =
        new Schema(
            required(1, "longCol", Types.LongType.get()),
            required(2, "intCol", Types.IntegerType.get()),
            required(3, "intCol2", Types.IntegerType.get()),
            required(4, "intCol3", Types.IntegerType.get()),
            required(5, "intCol4", Types.IntegerType.get()),
            required(6, "floatCol", Types.FloatType.get()),
            optional(7, "doubleCol", Types.DoubleType.get()),
            optional(8, "dateCol", Types.DateType.get()),
            optional(9, "timestampCol", Types.TimestampType.withZone()),
            optional(10, "stringCol", Types.StringType.get()));

    SparkSessionCatalog catalog;
    try {
      catalog =
          (SparkSessionCatalog) Spark3Util.catalogAndIdentifier(spark(), "spark_catalog").catalog();
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
            .range(0, NUM_ROWS * NUM_FILES, 1, NUM_FILES)
            .drop("id")
            .withColumn("longCol", new RandomGeneratingUDF(UNIQUE_VALUES).randomLongUDF().apply())
            .withColumn(
                "intCol",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.IntegerType))
            .withColumn(
                "intCol2",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.IntegerType))
            .withColumn(
                "intCol3",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.IntegerType))
            .withColumn(
                "intCol4",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.IntegerType))
            .withColumn(
                "floatCol",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.FloatType))
            .withColumn(
                "doubleCol",
                new RandomGeneratingUDF(UNIQUE_VALUES)
                    .randomLongUDF()
                    .apply()
                    .cast(DataTypes.DoubleType))
            .withColumn("dateCol", date_add(current_date(), col("intCol").mod(NUM_FILES)))
            .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
            .withColumn("stringCol", new RandomGeneratingUDF(UNIQUE_VALUES).randomString().apply());
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
    String location = Files.createTempDir().getAbsolutePath() + "/" + UUID.randomUUID() + "/";
    return location;
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
            .master("local[*]");
    spark = builder.getOrCreate();
    Configuration sparkHadoopConf = spark.sessionState().newHadoopConf();
    hadoopConf.forEach(entry -> sparkHadoopConf.set(entry.getKey(), entry.getValue()));
  }

  protected void tearDownSpark() {
    spark.stop();
  }
}
