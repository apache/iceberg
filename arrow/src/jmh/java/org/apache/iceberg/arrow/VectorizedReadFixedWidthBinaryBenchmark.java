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
package org.apache.iceberg.arrow;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.arrow.vectorized.VectorizedTableScanIterable;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Benchmarks the vectorized Arrow read path for fixed-width binary columns (Iceberg {@code UUID},
 * {@code fixed[N]} and high-precision {@code decimal(p>18)}, all stored as Parquet {@code
 * FIXED_LEN_BYTE_ARRAY} and materialized into Arrow {@code FixedSizeBinaryVector}s).
 *
 * <p>The columns are required and high-cardinality (unique random values), so Parquet encodes them
 * as plain {@code FIXED_LEN_BYTE_ARRAY}. That is the path exercised by {@code
 * FixedSizeBinaryReader} in {@code VectorizedParquetDefinitionLevelReader}.
 *
 * <p>The benchmark consumes minimally (the column vectors are fully materialized during batch
 * production, before any accessor call), so {@code -prof gc} reports the allocation cost of
 * materialization itself rather than accessor-side copies.
 *
 * <p>Run with:
 *
 * <pre>
 *   ./gradlew :iceberg-arrow:jmh \
 *     -PjmhIncludeRegex=VectorizedReadFixedWidthBinaryBenchmark \
 *     -PjmhOutputPath=benchmark/arrow-fixedbinary.txt
 * </pre>
 */
@Fork(
    value = 1,
    jvmArgsAppend = {
      // Arrow accesses JDK internals (java.nio.Buffer.address) reflectively; the read path fails
      // without these in the forked JVM. Mirrors the module-open flags the Gradle test tasks use.
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true"
    })
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class VectorizedReadFixedWidthBinaryBenchmark {

  private static final int NUM_ROWS = 1_500_000;
  private static final int BATCH_SIZE = 10_000;
  private static final long SEED = 1729L;

  // "uncompressed" isolates the read/materialization path; "gzip" reflects a realistic on-disk file
  // where Parquet page decompression dominates heap allocation.
  @Param({"uncompressed", "gzip"})
  private String compression;

  private static final Schema SCHEMA =
      new Schema(
          required(1, "uuid", Types.UUIDType.get()),
          required(2, "decimal38", Types.DecimalType.of(38, 0)),
          required(3, "fixed16", Types.FixedType.ofLength(16)));

  private File tableDir;
  private Table table;
  private byte[] firstFixed16;

  @Setup
  public void setupBenchmark() throws IOException {
    this.tableDir = java.nio.file.Files.createTempDirectory("arrow-fixedbinary-bench").toFile();
    File tableLocation = new File(tableDir, "table");
    HadoopTables tables = new HadoopTables();
    this.table = tables.create(SCHEMA, tableLocation.toString());

    File dataFile = new File(tableDir, "data.parquet");
    writeData(dataFile);

    DataFile appended =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withInputFile(Files.localInput(dataFile))
            .withFormat(FileFormat.PARQUET)
            .withRecordCount(NUM_ROWS)
            .withFileSizeInBytes(dataFile.length())
            .build();
    table.newAppend().appendFile(appended).commit();

    sanityCheck();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    try (Stream<java.nio.file.Path> paths = java.nio.file.Files.walk(tableDir.toPath())) {
      paths
          .sorted(Comparator.reverseOrder())
          .forEach(
              path -> {
                try {
                  java.nio.file.Files.deleteIfExists(path);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              });
    }
  }

  private void writeData(File dataFile) throws IOException {
    Random random = new Random(SEED);
    try (FileAppender<GenericRecord> appender =
        Parquet.write(Files.localOutput(dataFile))
            .schema(SCHEMA)
            .set("write.parquet.compression-codec", compression)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      for (int i = 0; i < NUM_ROWS; i++) {
        GenericRecord record = GenericRecord.create(SCHEMA);
        record.setField("uuid", new UUID(random.nextLong(), random.nextLong()));

        // 15 random bytes -> unscaled magnitude < 10^38, so it fits decimal(38, 0) (16-byte fixed)
        byte[] unscaled = new byte[15];
        random.nextBytes(unscaled);
        record.setField("decimal38", new BigDecimal(new BigInteger(1, unscaled)));

        byte[] fixed = new byte[16];
        random.nextBytes(fixed);
        record.setField("fixed16", fixed);

        if (i == 0) {
          this.firstFixed16 = fixed.clone();
        }
        appender.add(record);
      }
    }
  }

  /**
   * Fail fast in @Setup if materialization is wrong, so a regression never measures as "faster".
   */
  private void sanityCheck() throws IOException {
    try (VectorizedTableScanIterable scan =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      ColumnarBatch first = scan.iterator().next();
      byte[] read = first.column(2).getBinary(0);
      if (!Arrays.equals(read, firstFixed16)) {
        throw new IllegalStateException(
            "fixed16 row 0 mismatch: expected "
                + Arrays.toString(firstFixed16)
                + " but was "
                + Arrays.toString(read));
      }
    }
  }

  @Benchmark
  public void scanFixedWidthBinary(Blackhole blackhole) throws IOException {
    try (VectorizedTableScanIterable scan =
        new VectorizedTableScanIterable(table.newScan(), BATCH_SIZE, false)) {
      for (ColumnarBatch batch : scan) {
        // Vectors are already materialized here; reading one value keeps the batch live and proves
        // the data was decoded, while adding only negligible (per-batch, not per-row) allocation.
        blackhole.consume(batch.column(2).getBinary(0));
        blackhole.consume(batch.numRows());
      }
    }
  }
}
