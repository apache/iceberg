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

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.spark.source.IcebergSourceBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;

/**
 * Parent class of the benchmarks that compare performance of performance of reading Parquet data with a
 * flat schema using vectorized Iceberg read path and the built-in file source in Spark.
 * <p>
 * To run all the the benchmarks that extend this class:
 * <code>
   * ./gradlew :iceberg-spark:jmh
 * -PjmhIncludeRegex=VectorizedRead*Benchmark
 * -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-read-benchmark-result.txt
 * </code>
 */

public abstract class VectorizedIcebergSourceBenchmark extends IcebergSourceBenchmark {
  static final int NUM_FILES = 10;
  static final int NUM_ROWS = 10000000;

  @Setup
  public void setupBenchmark() {
    setupSpark();
    appendData();
    // Allow unsafe memory access to avoid the costly check arrow does to check if index is within bounds
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

  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected abstract void appendData();

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized100() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "100")
          .load(tableLocation);
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized1k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "1000")
          .load(tableLocation);
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized5k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "5000")
          .load(tableLocation);
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized10k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "10000")
          .load(tableLocation);
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readFileSourceVectorized() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(conf, () -> {
      Dataset<Row> df = spark().read().parquet(dataLocation());
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readFileSourceNonVectorized() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "false");
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(conf, () -> {
      Dataset<Row> df = spark().read().parquet(dataLocation());
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionIcebergVectorized1k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "1000")
          .load(tableLocation).select("longCol");
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionIcebergVectorized5k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "5000")
          .load(tableLocation).select("longCol");
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionIcebergVectorized10k() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg")
          .option("iceberg.read.numrecordsperbatch", "10000")
          .load(tableLocation).select("longCol");
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionFileSourceVectorized() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "true");
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(conf, () -> {
      Dataset<Row> df = spark().read().parquet(dataLocation()).select("longCol");
      materialize(df);
    });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionFileSourceNonVectorized() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_VECTORIZED_READER_ENABLED().key(), "false");
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(conf, () -> {
      Dataset<Row> df = spark().read().parquet(dataLocation()).select("longCol");
      materialize(df);
    });
  }
}
