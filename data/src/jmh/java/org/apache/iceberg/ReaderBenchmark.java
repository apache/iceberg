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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.SingleShotTime)
public abstract class ReaderBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(ReaderBenchmark.class);

  private static final Schema TEST_SCHEMA =
      new Schema(
          required(1, "longCol", Types.LongType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "floatCol", Types.FloatType.get()),
          optional(4, "doubleCol", Types.DoubleType.get()),
          optional(5, "decimalCol", Types.DecimalType.of(20, 5)),
          optional(6, "dateCol", Types.DateType.get()),
          optional(7, "timestampCol", Types.TimestampType.withZone()),
          optional(8, "stringCol", Types.StringType.get()));

  private static final int NUM_ROWS = 2500000;
  private static final int SEED = -1;

  private File testFile;

  @Setup
  public void setupBenchmark() throws IOException {
    testFile = Files.createTempFile("perf-bench", null).toFile();
    testFile.delete();

    try (FileAppender<Record> writer = writer(testFile, TEST_SCHEMA)) {
      writer.addAll(RandomGenericData.generate(TEST_SCHEMA, NUM_ROWS, SEED));
    }
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    testFile.delete();
  }

  @Benchmark
  @Threads(1)
  public void readIceberg() throws IOException {
    try (CloseableIterable<Record> reader = reader(testFile, TEST_SCHEMA)) {
      long val = 0;
      for (Record record : reader) {
        // access something to ensure the compiler doesn't optimize this away
        val ^= (Long) record.get(0);
      }
      LOG.info("XOR val: {}", val);
    }
  }

  protected abstract CloseableIterable<Record> reader(File file, Schema schema);

  protected abstract FileAppender<Record> writer(File file, Schema schema) throws IOException;
}
