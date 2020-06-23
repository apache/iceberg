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

package org.apache.iceberg.spark.data.parquet;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.datasources.parquet.ParquetWriteSupport;
import org.apache.spark.sql.types.StructType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

/**
 * A benchmark that evaluates the performance of writing Parquet data with a flat schema using
 * Iceberg and Spark Parquet writers.
 *
 * To run this benchmark:
 * <code>
 *   ./gradlew :iceberg-spark:jmh
 *       -PjmhIncludeRegex=SparkParquetWritersFlatDataBenchmark
 *       -PjmhOutputPath=benchmark/spark-parquet-writers-flat-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkParquetWritersFlatDataBenchmark {

  private static final Schema SCHEMA = new Schema(
      required(1, "longCol", Types.LongType.get()),
      required(2, "intCol", Types.IntegerType.get()),
      required(3, "floatCol", Types.FloatType.get()),
      optional(4, "doubleCol", Types.DoubleType.get()),
      optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
      optional(6, "dateCol", Types.DateType.get()),
      optional(7, "timestampCol", Types.TimestampType.withZone()),
      optional(8, "stringCol", Types.StringType.get()));
  private static final int NUM_RECORDS = 1000000;
  private Iterable<InternalRow> rows;
  private File dataFile;

  @Setup
  public void setupBenchmark() throws IOException {
    rows = RandomData.generateSpark(SCHEMA, NUM_RECORDS, 0L);
    dataFile = File.createTempFile("parquet-flat-data-benchmark", ".parquet");
  }

  @TearDown
  public void tearDownBenchmark() {
    if (dataFile != null) {
      dataFile.delete();
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingIcebergWriter() throws IOException {
    try (FileAppender<InternalRow> writer = Parquet.write(Files.localOutput(dataFile))
        .createWriterFunc(msgType -> SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(SCHEMA), msgType))
        .schema(SCHEMA)
        .build()) {

      writer.addAll(rows);
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingSparkWriter() throws IOException {
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    try (FileAppender<InternalRow> writer = Parquet.write(Files.localOutput(dataFile))
        .writeSupport(new ParquetWriteSupport())
        .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
        .set("spark.sql.parquet.writeLegacyFormat", "false")
        .set("spark.sql.parquet.binaryAsString", "false")
        .set("spark.sql.parquet.int96AsTimestamp", "false")
        .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .schema(SCHEMA)
        .build()) {

      writer.addAll(rows);
    }
  }
}
