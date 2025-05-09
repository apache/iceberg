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
 * Benchmark to compare performance of reading Parquet decimal data using vectorized Iceberg read
 * path and the built-in file source in Spark.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:jmh \
 *       -PjmhIncludeRegex=VectorizedReadParquetDecimalBenchmark \
 *       -PjmhOutputPath=benchmark/results.txt
 * </code>
 */
public class VectorizedReadParquetDecimalBenchmark extends IcebergSourceBenchmark {

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
            optional(1, "decimalCol1", Types.DecimalType.of(7, 2)),
            optional(2, "decimalCol2", Types.DecimalType.of(15, 2)),
            optional(3, "decimalCol3", Types.DecimalType.of(20, 2)));
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
              .withColumn("decimalCol1", expr("CAST(longCol AS DECIMAL(7, 2))"))
              .withColumn("decimalCol2", expr("CAST(longCol AS DECIMAL(15, 2))"))
              .withColumn("decimalCol3", expr("CAST(longCol AS DECIMAL(20, 2))"))
              .drop("longCol");
      appendAsFile(df);
    }
  }

  @Benchmark
  @Threads(1)
  public void readIntBackedDecimalsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("decimalCol1");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readIntBackedDecimalsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("decimalCol1");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readLongBackedDecimalsIcebergVectorized5k() {
    withTableProperties(
        tablePropsWithVectorizationEnabled(5000),
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).select("decimalCol2");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readLongBackedDecimalsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("decimalCol2");
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
              spark().read().format("iceberg").load(tableLocation).select("decimalCol3");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDecimalsSparkVectorized5k() {
    withSQLConf(
        sparkConfWithVectorizationEnabled(5000),
        () -> {
          Dataset<Row> df = spark().read().parquet(dataLocation()).select("decimalCol3");
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
