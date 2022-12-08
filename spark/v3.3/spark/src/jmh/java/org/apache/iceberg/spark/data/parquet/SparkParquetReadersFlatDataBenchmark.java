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
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkBenchmarkUtil;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkParquetReaders;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.execution.datasources.parquet.ParquetReadSupport;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of reading Parquet data with a flat schema using
 * Iceberg and Spark Parquet readers.
 *
 * <p>To run this benchmark for spark-3.3: <code>
 *   ./gradlew -DsparkVersions=3.3 :iceberg-spark:iceberg-spark-3.3_2.12:jmh
 *       -PjmhIncludeRegex=SparkParquetReadersFlatDataBenchmark
 *       -PjmhOutputPath=benchmark/spark-parquet-readers-flat-data-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class SparkParquetReadersFlatDataBenchmark {

  private static final DynMethods.UnboundMethod APPLY_PROJECTION =
      DynMethods.builder("apply").impl(UnsafeProjection.class, InternalRow.class).build();
  private static final Schema SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "floatCol", Types.FloatType.get()),
          optional(4, "doubleCol", Types.DoubleType.get()),
          optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
          optional(6, "dateCol", Types.DateType.get()),
          optional(7, "timestampCol", Types.TimestampType.withZone()),
          optional(8, "stringCol", Types.StringType.get()));
  private static final Schema PROJECTED_SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
          optional(8, "stringCol", Types.StringType.get()));
  private static final int NUM_RECORDS = 1000000;
  private File dataFile;

  @Setup
  public void setupBenchmark() throws IOException {
    dataFile = File.createTempFile("parquet-flat-data-benchmark", ".parquet");
    dataFile.delete();
    List<GenericData.Record> records = RandomData.generateList(SCHEMA, NUM_RECORDS, 0L);
    try (FileAppender<GenericData.Record> writer =
        Parquet.write(Files.localOutput(dataFile)).schema(SCHEMA).named("benchmark").build()) {
      writer.addAll(records);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    if (dataFile != null) {
      dataFile.delete();
    }
  }

  @Benchmark
  @Threads(1)
  public void readUsingIcebergReader(Blackhole blackHole) throws IOException {
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
            .build()) {

      for (InternalRow row : rows) {
        blackHole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readUsingIcebergReaderUnsafe(Blackhole blackhole) throws IOException {
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(SCHEMA, type))
            .build()) {

      Iterable<InternalRow> unsafeRows =
          Iterables.transform(
              rows, APPLY_PROJECTION.bind(SparkBenchmarkUtil.projection(SCHEMA, SCHEMA))::invoke);

      for (InternalRow row : unsafeRows) {
        blackhole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readUsingSparkReader(Blackhole blackhole) throws IOException {
    StructType sparkSchema = SparkSchemaUtil.convert(SCHEMA);
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(SCHEMA)
            .readSupport(new ParquetReadSupport())
            .set("org.apache.spark.sql.parquet.row.requested_schema", sparkSchema.json())
            .set("spark.sql.parquet.binaryAsString", "false")
            .set("spark.sql.parquet.int96AsTimestamp", "false")
            .set("spark.sql.caseSensitive", "false")
            .set("spark.sql.parquet.fieldId.write.enabled", "false")
            .callInit()
            .build()) {

      for (InternalRow row : rows) {
        blackhole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionUsingIcebergReader(Blackhole blackhole) throws IOException {
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(PROJECTED_SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(PROJECTED_SCHEMA, type))
            .build()) {

      for (InternalRow row : rows) {
        blackhole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionUsingIcebergReaderUnsafe(Blackhole blackhole) throws IOException {
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(PROJECTED_SCHEMA)
            .createReaderFunc(type -> SparkParquetReaders.buildReader(PROJECTED_SCHEMA, type))
            .build()) {

      Iterable<InternalRow> unsafeRows =
          Iterables.transform(
              rows,
              APPLY_PROJECTION.bind(
                      SparkBenchmarkUtil.projection(PROJECTED_SCHEMA, PROJECTED_SCHEMA))
                  ::invoke);

      for (InternalRow row : unsafeRows) {
        blackhole.consume(row);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readWithProjectionUsingSparkReader(Blackhole blackhole) throws IOException {
    StructType sparkSchema = SparkSchemaUtil.convert(PROJECTED_SCHEMA);
    try (CloseableIterable<InternalRow> rows =
        Parquet.read(Files.localInput(dataFile))
            .project(PROJECTED_SCHEMA)
            .readSupport(new ParquetReadSupport())
            .set("org.apache.spark.sql.parquet.row.requested_schema", sparkSchema.json())
            .set("spark.sql.parquet.binaryAsString", "false")
            .set("spark.sql.parquet.int96AsTimestamp", "false")
            .set("spark.sql.caseSensitive", "false")
            .callInit()
            .build()) {

      for (InternalRow row : rows) {
        blackhole.consume(row);
      }
    }
  }
}
