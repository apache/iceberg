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
package org.apache.iceberg.arrow.vectorized;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.formats.FormatModelRegistry;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that compares reading list columns with the Arrow vectorized reader against the
 * row-by-row generic reader.
 *
 * <p>Both lists have the same total number of elements for each {@code listShape}: {@code RANDOM}
 * uses random list lengths, {@code FIXED_768} uses lists of exactly 768 elements.
 *
 * <p>To run this benchmark:
 *
 * <pre>{@code
 * # both list shapes
 * ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=VectorizedListReadBenchmark \
 *     -PjmhOutputPath=benchmark/list-read-results.txt
 *
 * # a single list shape
 * ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=VectorizedListReadBenchmark \
 *     -PjmhParams="listShape=FIXED_768" \
 *     -PjmhOutputPath=benchmark/list-read-results.txt
 * }</pre>
 *
 * <p>Results are written relative to the {@code data} module directory.
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
public class VectorizedListReadBenchmark {
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "intList", Types.ListType.ofOptional(2, Types.IntegerType.get())),
          optional(3, "strList", Types.ListType.ofOptional(4, Types.StringType.get())));

  private static final int BATCH_SIZE = 5_000;
  private static final long SEED = -1L;

  private static final int NUM_LIST_ELEMENTS = 2_500_000;
  private static final int FIXED_LIST_LENGTH = 768;

  // Based on RandomGenericData.RandomDataGenerator MAX_ENTRIES
  private static final float RANDOM_LIST_MEAN_LENGTH = 9.5f;

  @Param({"RANDOM", "FIXED_768"})
  private String listShape;

  private File testFile;

  @Setup
  public void setupBenchmark() throws IOException {
    testFile = Files.createTempFile("vec-list-bench", ".parquet").toFile();
    testFile.delete();

    Iterable<Record> records;
    if ("RANDOM".equals(listShape)) {
      int numRows = (int) (NUM_LIST_ELEMENTS / RANDOM_LIST_MEAN_LENGTH);
      records = RandomGenericData.generate(SCHEMA, numRows, SEED);
    } else {
      int numRows = NUM_LIST_ELEMENTS / FIXED_LIST_LENGTH;
      records =
          RandomGenericData.generateWithFixedLengthList(SCHEMA, numRows, SEED, FIXED_LIST_LENGTH);
    }

    try (FileAppender<Record> writer =
        Parquet.write(org.apache.iceberg.Files.localOutput(testFile))
            .schema(SCHEMA)
            .createWriterFunc(GenericParquetWriter::create)
            .build()) {
      writer.addAll(records);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    testFile.delete();
  }

  @Benchmark
  @Threads(1)
  public void readVectorized(Blackhole blackhole) throws IOException {
    try (CloseableIterable<ColumnarBatch> batches =
        FormatModelRegistry.readBuilder(
                FileFormat.PARQUET,
                ColumnarBatch.class,
                org.apache.iceberg.Files.localInput(testFile))
            .recordsPerBatch(BATCH_SIZE)
            .build()) {
      for (ColumnarBatch batch : batches) {
        org.apache.arrow.vector.complex.ListVector intListVector =
            (org.apache.arrow.vector.complex.ListVector) batch.column(0).getFieldVector();
        int lastRow = intListVector.getValueCount() - 1;
        if (lastRow >= 0) {
          blackhole.consume(intListVector.getElementEndIndex(lastRow));
        }
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readRowByRow(Blackhole blackhole) throws IOException {
    try (CloseableIterable<Record> records =
        Parquet.read(org.apache.iceberg.Files.localInput(testFile))
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(SCHEMA, fileSchema))
            .build()) {
      for (Record record : records) {
        List<?> intList = (List<?>) record.get(0);
        blackhole.consume(intList);
      }
    }
  }
}
