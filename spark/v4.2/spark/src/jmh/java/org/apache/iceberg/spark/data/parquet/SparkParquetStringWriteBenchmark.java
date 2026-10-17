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
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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
 * Measures Iceberg Parquet writes of Spark strings, grouped by how the bytes of each value are
 * stored.
 *
 * <p>{@link Backing#ARRAY} values are exactly sized arrays that the writer can always hand to
 * Parquet as-is, so they act as the baseline. {@link Backing#SLICE} values are windows into a
 * larger array, which is what Spark produces when reading an {@code UnsafeRow}. Comparing the two
 * in one run shows whether slices still pay for a defensive copy.
 *
 * <p>Compression is disabled so the measurement is not dominated by the codec. Use JMH's GC
 * profiler ({@code -prof gc}) to compare allocated bytes alongside elapsed time.
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

  /** Leading bytes before the first value, so a slice never starts at the head of its array. */
  private static final int PADDING = 13;

  /** How the bytes of each value are stored. */
  public enum Backing {
    ARRAY,
    SLICE
  }

  /** Row shapes worth measuring: column count, value size, and how often rows repeat. */
  public enum Workload {
    SHORT(120_000, 8, 32, 120_000),
    WIDE(40_000, 32, 32, 40_000),
    LONG(12_000, 8, 1024, 12_000),
    LOW_CARDINALITY(120_000, 8, 32, 16);

    private final int numRows;
    private final int numColumns;
    private final int length;
    private final int distinctRows;

    Workload(int numRows, int numColumns, int length, int distinctRows) {
      this.numRows = numRows;
      this.numColumns = numColumns;
      this.length = length;
      this.distinctRows = distinctRows;
    }
  }

  @Param private Workload workload;

  @Param private Backing backing;

  private Schema schema;
  private StructType sparkSchema;
  private List<InternalRow> rows;
  private File dataFile;

  @Setup(Level.Trial)
  public void setupBenchmark() throws IOException {
    List<Types.NestedField> fields = Lists.newArrayListWithExpectedSize(workload.numColumns);
    for (int column = 0; column < workload.numColumns; column += 1) {
      fields.add(Types.NestedField.optional(column + 1, "s" + column, Types.StringType.get()));
    }

    this.schema = new Schema(fields);
    this.sparkSchema = SparkSchemaUtil.convert(schema);

    Random random = new Random(0);
    List<InternalRow> distinct = Lists.newArrayListWithExpectedSize(workload.distinctRows);
    for (int id = 0; id < workload.distinctRows; id += 1) {
      distinct.add(newRow(random, id));
    }

    if (workload.distinctRows == workload.numRows) {
      this.rows = distinct;
    } else {
      // Repeat the distinct rows up to the row count, which is how low cardinality is expressed.
      this.rows = Lists.newArrayListWithExpectedSize(workload.numRows);
      for (int id = 0; id < workload.numRows; id += 1) {
        rows.add(distinct.get(id % workload.distinctRows));
      }
    }

    this.dataFile = File.createTempFile("iceberg-parquet-strings", ".parquet");
    dataFile.delete();
  }

  @TearDown(Level.Iteration)
  public void cleanup() {
    dataFile.delete();
  }

  @Benchmark
  public void writeStrings() throws IOException {
    try (FileAppender<InternalRow> writer =
        Parquet.write(Files.localOutput(dataFile))
            .schema(schema)
            .set(TableProperties.PARQUET_COMPRESSION, "uncompressed")
            .createWriterFunc(type -> SparkParquetWriters.buildWriter(sparkSchema, type))
            .build()) {
      writer.addAll(rows);
    }
  }

  private InternalRow newRow(Random random, int id) {
    // All values of a row share one backing array, which is the layout an UnsafeRow has.
    byte[] shared = new byte[PADDING + workload.numColumns * workload.length];
    Object[] values = new Object[workload.numColumns];
    for (int column = 0; column < workload.numColumns; column += 1) {
      int offset = PADDING + column * workload.length;
      fillLetters(random, shared, offset, workload.length);
      values[column] = id % 10 == 0 ? null : newValue(shared, offset, workload.length);
    }

    return new GenericInternalRow(values);
  }

  private UTF8String newValue(byte[] shared, int offset, int length) {
    return switch (backing) {
      case ARRAY -> {
        // Lift the window into an exactly sized array, which the writer passes through as-is.
        byte[] exact = new byte[length];
        System.arraycopy(shared, offset, exact, 0, length);
        yield UTF8String.fromBytes(exact);
      }
      case SLICE -> UTF8String.fromBytes(shared, offset, length);
    };
  }

  private static void fillLetters(Random random, byte[] bytes, int offset, int length) {
    for (int index = 0; index < length; index += 1) {
      bytes[offset + index] = (byte) ('a' + random.nextInt(26));
    }
  }
}
