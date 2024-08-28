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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of appending files to the table.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=AppendBenchmark
 *       -PjmhOutputPath=benchmark/append-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class AppendBenchmark {

  private static final String TABLE_IDENT = "tbl";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "int_col", Types.IntegerType.get()),
          required(2, "long_col", Types.LongType.get()),
          required(3, "decimal_col", Types.DecimalType.of(10, 10)),
          required(4, "date_col", Types.DateType.get()),
          required(5, "timestamp_col", Types.TimestampType.withoutZone()),
          required(6, "timestamp_tz_col", Types.TimestampType.withZone()),
          required(7, "str_col", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final HadoopTables TABLES = new HadoopTables();

  private Table table;
  private List<DataFile> dataFiles;

  @Param({"500000", "1000000", "2500000"})
  private int numFiles;

  @Param({"true", "false"})
  private boolean fast;

  @Setup
  public void setupBenchmark() {
    initTable();
    initDataFiles();
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
  }

  @Benchmark
  @Threads(1)
  public void appendFiles(Blackhole blackhole) {
    AppendFiles append = fast ? table.newFastAppend() : table.newAppend();

    for (DataFile dataFile : dataFiles) {
      append.appendFile(dataFile);
    }

    append.commit();
  }

  private void initTable() {
    this.table = TABLES.create(SCHEMA, SPEC, TABLE_IDENT);
  }

  private void dropTable() {
    TABLES.dropTable(TABLE_IDENT);
  }

  private void initDataFiles() {
    List<DataFile> generatedDataFiles = Lists.newArrayListWithExpectedSize(numFiles);

    for (int ordinal = 0; ordinal < numFiles; ordinal++) {
      DataFile dataFile = FileGenerationUtil.generateDataFile(table, null);
      generatedDataFiles.add(dataFile);
    }

    this.dataFiles = generatedDataFiles;
  }
}
