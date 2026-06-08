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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.api.Binary;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Microbenchmark isolating the eager dictionary-encoded fixed-width-binary decode (the {@code
 * FixedSizeBinaryDictEncodedReader} per-value path). End-to-end this path only fires when a
 * column's dictionary overflows to plain mid-chunk, so its cost is masked by page reading; this
 * drives the decode directly to measure per-value time and allocation ({@code -prof gc}).
 *
 * <pre>
 *   ./gradlew :iceberg-arrow:jmh \
 *     -PjmhIncludeRegex=VectorizedDictFixedBinaryDecodeBenchmark \
 *     -PjmhOutputPath=benchmark/arrow-dict-fixedbinary.txt
 * </pre>
 */
@Fork(
    value = 1,
    jvmArgsAppend = {
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true"
    })
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 2)
@Measurement(iterations = 5, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class VectorizedDictFixedBinaryDecodeBenchmark {

  private static final int TYPE_WIDTH = 16;
  private static final int NUM_VALUES = 4096;
  private static final int DICT_SIZE = 256;
  private static final long SEED = 1729L;

  private RootAllocator allocator;
  private FixedSizeBinaryVector vector;
  private Dictionary dictionary;
  private VectorizedDictionaryEncodedParquetValuesReader.FixedSizeBinaryDictEncodedReader reader;
  private int[] ids;

  @Setup
  public void setupBenchmark() {
    this.allocator = new RootAllocator();
    this.vector = new FixedSizeBinaryVector("fixed", allocator, TYPE_WIDTH);
    vector.allocateNew(NUM_VALUES);

    Random random = new Random(SEED);
    Binary[] entries = new Binary[DICT_SIZE];
    for (int i = 0; i < DICT_SIZE; i++) {
      byte[] bytes = new byte[TYPE_WIDTH];
      random.nextBytes(bytes);
      // Match the Binary type a real Parquet dictionary produces for FIXED_LEN_BYTE_ARRAY.
      entries[i] = Binary.fromConstantByteBuffer(ByteBuffer.wrap(bytes), 0, TYPE_WIDTH);
    }
    this.dictionary =
        new Dictionary(Encoding.RLE_DICTIONARY) {
          @Override
          public int getMaxId() {
            return DICT_SIZE - 1;
          }

          @Override
          public Binary decodeToBinary(int id) {
            return entries[id];
          }
        };

    this.reader =
        new VectorizedDictionaryEncodedParquetValuesReader(0, false)
            .fixedSizeBinaryDictEncodedReader();

    this.ids = new int[NUM_VALUES];
    for (int i = 0; i < NUM_VALUES; i++) {
      ids[i] = random.nextInt(DICT_SIZE);
    }
  }

  @TearDown
  public void tearDownBenchmark() {
    vector.close();
    allocator.close();
  }

  @Benchmark
  public void decodeDictFixedBinary(Blackhole blackhole) {
    for (int i = 0; i < NUM_VALUES; i++) {
      reader.nextVal(vector, dictionary, i, ids[i], TYPE_WIDTH);
    }
    blackhole.consume(vector);
  }
}
