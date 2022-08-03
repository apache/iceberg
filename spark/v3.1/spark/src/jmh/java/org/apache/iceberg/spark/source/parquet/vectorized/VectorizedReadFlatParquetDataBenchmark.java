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
package org.apache.iceberg.spark.source.parquet.vectorized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.when;

import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * Benchmark to compare performance of reading Parquet data with a flat schema using vectorized
 * Iceberg read path and the built-in file source in Spark.
 *
 * <p>To run this benchmark for spark-3.1: <code>
 *   ./gradlew -DsparkVersions=3.1 :iceberg-spark:iceberg-spark-3.1_2.12:jmh
 *       -PjmhIncludeRegex=VectorizedReadFlatParquetDataBenchmark
 *       -PjmhOutputPath=benchmark/results.txt
 * </code>
 */
public class VectorizedReadFlatParquetDataBenchmark extends IcebergSourceBenchmark {

  static final int NUM_FILES = 5;
  static final int NUM_ROWS_PER_FILE = 10_000_000;

  @Setup
  public void setupBenchmark() {
    setupSpark();
    appendData();
    // Allow unsafe memory access to avoid the costly check arrow does to check if index is within
    // bounds
    System.setProperty("arrow.enable_unsafe_memory_access", "true");
    // Disable expensive null check for every get(index) call.
    // Iceberg manages nullability checks itself instead of relying on arrow.
    System.setProperty("arrow.enable_null_check_for_get", "false");
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  @Override
  protected Table initTable() {
    Schema schema =
        new Schema(
            optional(1, "longCol", Types.LongType.get()),
            optional(2, "intCol", Types.IntegerType.get()),
            optional(3, "floatCol", Types.FloatType.get()),
            optional(4, "doubleCol", Types.DoubleType.get()),
            optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
            optional(6, "dateCol", Types.DateType.get()),
            optional(7, "timestampCol", Types.TimestampType.withZone()),
            optional(8, "stringCol", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = parquetWriteProps();
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }

  Map<String, String> parquetWriteProps() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    properties.put(TableProperties.PARQUET_DICT_SIZE_BYTES, "1");
    return properties;
  }

  void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df =
          spark()
              .range(NUM_ROWS_PER_FILE)
              .withColumn(
                  "longCol",
                  when(pmod(col("id"), lit(10)).equalTo(lit(0)), lit(null)).otherwise(col("id")))
              .drop("id")
              .withColumn("intCol", expr("CAST(longCol AS INT)"))
              .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
              .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
              .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(20, 5))"))
              .withColumn("dateCol", date_add(current_date(), fileNum))
              .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
              .withColumn("stringCol", expr("CAST(longCol AS STRING)"));
      appendAsFile(df);
    }
  }

  @Benchmark
  @Threads(1)
  public void readIntegersIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("intCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readIntegersSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("intCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readLongsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("longCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readLongsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("longCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readFloatsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("floatCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readFloatsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("floatCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDoublesIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("doubleCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDoublesSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("doubleCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDecimalsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("decimalCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDecimalsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("decimalCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDatesIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("dateCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDatesSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("dateCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readTimestampsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("timestampCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readTimestampsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("timestampCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readStringsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("stringCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readStringsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("stringCol");
          materialize(df);
        });
  }

  private static Map<String, String> tablePropsWithVectorizationEnabled(int batchSize) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true");
    tableProperties.put(TableProperties.PARQUET_BATCH_SIZE, String.valueOf(batchSize));
    return tableProperties;
  }

  private static Map<String, String> sparkConfWithVectorizationEnabled(int batchSize) {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_BATCH_SIZE().key(), String.valueOf(batchSize));
    return conf;
  }
}
