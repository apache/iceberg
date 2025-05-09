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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

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
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of writing nested Parquet data using Iceberg and Spark
 * Parquet writers.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 *       -PjmhIncludeRegex=SparkParquetWritersNestedDataBenchmark
 *       -PjmhOutputPath=benchmark/spark-parquet-writers-nested-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkParquetWritersNestedDataBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          required(0, "id", Types.LongType.get()),
          optional(
              4,
              "nested",
              Types.StructType.of(
                  required(1, "col1", Types.StringType.get()),
                  required(2, "col2", Types.DoubleType.get()),
                  required(3, "col3", Types.LongType.get()))));
  private static final int NUM_RECORDS = 1000000;
  private Iterable<InternalRow> rows;
  private File dataFile;

  @Setup
  public void setupBenchmark() throws IOException {
    rows = RandomData.generateSpark(SCHEMA, NUM_RECORDS, 0L);
    dataFile = File.createTempFile("parquet-nested-data-benchmark", ".parquet");
    dataFile.delete();
  }

  @TearDown(Level.Iteration)
  public void tearDownBenchmark() {
    if (dataFile != null) {
      dataFile.delete();
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingIcebergWriter() throws IOException {
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(dataFile))
            .createWriterFunc(
                msgType ->
                    SparkParquetWriters.buildWriter(SparkSchemaUtil.convert(SCHEMA), msgType))
            .schema(SCHEMA)
            .build()) {

      writer.addAll(rows);
    }
  }

  @Benchmark
  @Threads(1)
  public void writeUsingSparkWriter() throws IOException {
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(dataFile))
            .writeSupport(new ParquetWriteSupport())
            .set("org.apache.spark.sql.parquet.row.attributes", sparkSchema.json())
            .set("spark.sql.parquet.writeLegacyFormat", "false")
            .set("spark.sql.parquet.binaryAsString", "false")
            .set("spark.sql.parquet.int96AsTimestamp", "false")
            .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
            .set("spark.sql.caseSensitive", "false")
            .set("spark.sql.parquet.fieldId.write.enabled", "false")
            .schema(SCHEMA)
            .build()) {

      writer.addAll(rows);
    }
  }
}
