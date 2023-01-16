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

import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceFlatDataBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * A benchmark that evaluates the performance of writing Parquet data with a flat schema using
 * Iceberg and the built-in file source in Spark.
 *
 * <p>To run this benchmark for spark-3.1: <code>
 *   ./gradlew -DsparkVersions=3.1 :iceberg-spark:iceberg-spark-3.1_2.12:jmh
 *       -PjmhIncludeRegex=IcebergSourceFlatParquetDataWriteBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-flat-parquet-data-write-benchmark-result.txt
 * </code>
 */
public class IcebergSourceFlatParquetDataWriteBenchmark extends IcebergSourceFlatDataBenchmark {

  private static final int NUM_ROWS = 5000000;

  @Setup
  public void setupBenchmark() {
    setupSpark();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg() {
    String tableLocation = table().location();
    benchmarkData().write().format("iceberg").mode(SaveMode.Append).save(tableLocation);
  }

  @Benchmark
  @Threads(1)
  public void writeFileSource() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_COMPRESSION().key(), "gzip");
    withSQLConf(conf, () -> benchmarkData().write().mode(SaveMode.Append).parquet(dataLocation()));
  }

  private Dataset<Row> benchmarkData() {
    return spark()
        .range(NUM_ROWS)
        .withColumnRenamed("id", "longCol")
        .withColumn("intCol", expr("CAST(longCol AS INT)"))
        .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
        .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
        .withColumn("decimalCol", expr("CAST(longCol AS DECIMAL(20, 5))"))
        .withColumn("dateCol", expr("DATE_ADD(CURRENT_DATE(), (intCol % 20))"))
        .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
        .withColumn("stringCol", expr("CAST(dateCol AS STRING)"))
        .coalesce(1);
  }
}
