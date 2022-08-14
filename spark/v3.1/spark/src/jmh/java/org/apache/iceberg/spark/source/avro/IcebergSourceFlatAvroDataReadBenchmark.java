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
package org.apache.iceberg.spark.source.avro;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceFlatDataBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * A benchmark that evaluates the performance of reading Avro data with a flat schema using Iceberg
 * and the built-in file source in Spark.
 *
 * <p>To run this benchmark for spark-3.1: <code>
 *   ./gradlew -DsparkVersions=3.1 :iceberg-spark:iceberg-spark-3.1_2.12:jmh
 *       -PjmhIncludeRegex=IcebergSourceFlatAvroDataReadBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-flat-avro-data-read-benchmark-result.txt
 * </code>
 */
public class IcebergSourceFlatAvroDataReadBenchmark extends IcebergSourceFlatDataBenchmark {

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
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readFileSource() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(
        conf,
        () -> {
          Dataset<Row> df = spark().read().format("avro").load(dataLocation());
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionIceberg() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation).select("longCol");
          materialize(df);
        });
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionFileSource() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.FILES_OPEN_COST_IN_BYTES().key(), Integer.toString(128 * 1024 * 1024));
    withSQLConf(
        conf,
        () -> {
          Dataset<Row> df = spark().read().format("avro").load(dataLocation()).select("longCol");
          materialize(df);
        });
  }

  private void appendData() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(DEFAULT_FILE_FORMAT, "avro");
    withTableProperties(
        tableProperties,
        () -> {
          for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
            Dataset<Row> df =
                spark()
                    .range(NUM_ROWS)
                    .withColumnRenamed("id", "longCol")
                    .withColumn("intCol", expr("CAST(longCol AS INT)"))
                    .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
                    .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
                    .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(20, 5))"))
                    .withColumn("dateCol", date_add(current_date(), fileNum))
                    .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
                    .withColumn("stringCol", expr("CAST(dateCol AS STRING)"));
            appendAsFile(df);
          }
        });
  }
}
