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

package org.apache.iceberg.spark.source.orc;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.source.IcebergSourceNestedListDataBenchmark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.internal.SQLConf;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import static org.apache.spark.sql.functions.array_repeat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.struct;

/**
 * A benchmark that evaluates the performance of writing nested Parquet data using Iceberg
 * and the built-in file source in Spark.
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :iceberg-spark2:jmh
 *       -PjmhIncludeRegex=IcebergSourceNestedListORCDataWriteBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-nested-list-orc-data-write-benchmark-result.txt
 * </code>
 */
public class IcebergSourceNestedListORCDataWriteBenchmark extends IcebergSourceNestedListDataBenchmark {

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
  public void writeIceberg2000() {
    String tableLocation = table().location();
    benchmarkData(2000).write().format("iceberg").option("write-format", "orc")
        .mode(SaveMode.Append).save(tableLocation);
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg20000() {
    String tableLocation = table().location();
    benchmarkData(20000).write().format("iceberg").option("write-format", "orc")
        .mode(SaveMode.Append).save(tableLocation);
  }

  @Benchmark
  @Threads(1)
  public void writeIceberg20000DictionaryOff() {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put("orc.dictionary.key.threshold", "0");
    withTableProperties(tableProperties, () -> {
      String tableLocation = table().location();
      benchmarkData(20000).write().format("iceberg").option("write-format", "orc")
          .mode(SaveMode.Append).save(tableLocation);
    });
  }

  @Benchmark
  @Threads(1)
  public void writeFileSource2000() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_COMPRESSION().key(), "gzip");
    withSQLConf(conf, () -> benchmarkData(2000).write().mode(SaveMode.Append).orc(dataLocation()));
  }

  @Benchmark
  @Threads(1)
  public void writeFileSource20000() {
    Map<String, String> conf = Maps.newHashMap();
    conf.put(SQLConf.PARQUET_COMPRESSION().key(), "gzip");
    withSQLConf(conf, () -> benchmarkData(20000).write().mode(SaveMode.Append).orc(dataLocation()));
  }

  private Dataset<Row> benchmarkData(int numRows) {
    return spark().range(numRows)
        .withColumn("outerlist", array_repeat(struct(
            expr("array_repeat(CAST(id AS string), 1000) AS innerlist")),
            10))
        .coalesce(1);
  }
}
