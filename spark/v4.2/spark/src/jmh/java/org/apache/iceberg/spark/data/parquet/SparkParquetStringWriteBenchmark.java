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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.data.SparkParquetWriters;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Measures Iceberg Parquet writes of Spark strings backed by slices of larger arrays.
 *
 * <p>Run the same benchmark on the original and patched writers in separate JVMs. Use JMH's GC
 * profiler to compare allocated bytes as well as elapsed time.
 *
 * <p>To run for Spark 4.2:
 *
 * <pre>
 * ./gradlew -DsparkVersions=4.2 :iceberg-spark:iceberg-spark-4.2_2.13:jmh \
 *     -PjmhIncludeRegex=SparkParquetStringWriteBenchmark
 * </pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Threads(1)
public class SparkParquetStringWriteBenchmark {

  private static final int PADDING = 13;
  private static final int LOW_CARDINALITY = 16;

  @Param({"short", "wide", "long", "low-cardinality"})
  public String workload;

  private Schema schema;
  private StructType sparkSchema;
  private List<InternalRow> rows;
  private File dataFile;

  @Setup(Level.Trial)
  public void setup() throws IOException {
    int numRows =
        switch (workload) {
          case "wide" -> 40000;
          case "long" -> 12000;
          default -> 120000;
        };
    int numColumns = workload.equals("wide") ? 32 : 8;
    int length = workload.equals("long") ? 1024 : 32;
    List<Types.NestedField> fields = new ArrayList<>();
    for (int column = 0; column < numColumns; column += 1) {
      fields.add(Types.NestedField.optional(column + 1, "s" + column, Types.StringType.get()));
    }
    this.schema = new Schema(fields);
    this.sparkSchema = SparkSchemaUtil.convert(schema);
    this.rows = new ArrayList<>(numRows);
    Random random = new Random(0);
    for (int id = 0; id < numRows; id += 1) {
      if (workload.equals("low-cardinality")) {
        random.setSeed(id % LOW_CARDINALITY);
      }
      byte[] backing = new byte[PADDING + numColumns * length + PADDING];
      Object[] values = new Object[numColumns];
      for (int column = 0; column < numColumns; column += 1) {
        int offset = PADDING + column * length;
        for (int index = 0; index < length; index += 1) {
          backing[offset + index] = (byte) ('a' + random.nextInt(26));
        }
        values[column] = id % 10 == 0 ? null : UTF8String.fromBytes(backing, offset, length);
      }
      rows.add(new GenericInternalRow(values));
    }
    this.dataFile = File.createTempFile("iceberg-parquet-strings", ".parquet");
    if (!dataFile.delete()) {
      throw new IOException("Cannot delete " + dataFile);
    }
  }

  @TearDown(Level.Iteration)
  public void cleanup() throws IOException {
    if (dataFile.exists() && !dataFile.delete()) {
      throw new IOException("Cannot delete " + dataFile);
    }
  }

  @Benchmark
  public void writeStrings() throws IOException {
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(dataFile))
            .schema(schema)
            .set("write.parquet.compression-codec", "snappy")
            .createWriterFunc(type -> SparkParquetWriters.buildWriter(sparkSchema, type))
            .build()) {
      writer.addAll(rows);
    }
  }
}
