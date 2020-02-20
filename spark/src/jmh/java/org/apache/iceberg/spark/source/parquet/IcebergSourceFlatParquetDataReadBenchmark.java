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

package org.apache.iceberg.spark.source.parquet;

import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.source.IcebergSourceFlatDataBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * A benchmark that evaluates the performance of reading Parquet data with a flat schema
 * using Iceberg and the built-in file source in Spark.
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :iceberg-spark:jmh
 *       -PjmhIncludeRegex=IcebergSourceFlatParquetDataReadBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-read-benchmark-result.txt
 * </code>
 */
public class IcebergSourceFlatParquetDataReadBenchmark extends IcebergSourceFlatDataBenchmark {

  private static final int NUM_FILES = 10;
  private static final int NUM_ROWS = 1000000;

  @Setup
  public void setupBenchmark() {
    setupSpark();
    appendData();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void readIceberg() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TableProperties.SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
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
  public void readWithProjectionIceberg() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TableProperties.SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("longCol");
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

  private void appendData() {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> df = spark().range(NUM_ROWS)
          .withColumnRenamed("id", "longCol")
          .withColumn("intCol", functions.expr("CAST(longCol AS INT)"))
          .withColumn("floatCol", functions.expr("CAST(longCol AS FLOAT)"))
          .withColumn("doubleCol", functions.expr("CAST(longCol AS DOUBLE)"))
          .withColumn("decimalCol", functions.expr("CAST(longCol AS DECIMAL(20, 5))"))
          .withColumn("dateCol", functions.date_add(functions.current_date(), fileNum))
          .withColumn("timestampCol", functions.expr("TO_TIMESTAMP(dateCol)"))
          .withColumn("stringCol", functions.expr("CAST(dateCol AS STRING)"));
      appendAsFile(df);
    }
  }
}
