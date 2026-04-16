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
package org.apache.iceberg.mumbling;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import me.lemire.integercompression.FastPFOR128;
import me.lemire.integercompression.IntWrapper;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the performance of {@link PFOREncoding} and compares it with
 * JavaFastPFOR.
 *
 * <p>Two data shapes are exercised:
 *
 * <ul>
 *   <li><b>Descriptor</b>: 256 values in {@code [0, 31]} with 5% full-range outliers, typical for
 *       Mumbling bitmap descriptor arrays.
 *   <li><b>Uniform byte</b>: 256 values drawn uniformly from {@code [0, 255]}, the worst case for
 *       PFOR (no compression benefit; exercises the fallback path).
 * </ul>
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=PFOREncodingBenchmark
 *       -PjmhOutputPath=benchmark/pfor-encoding-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class PFOREncodingBenchmark {

  // Fixed seeds for reproducibility
  private static final long DESCRIPTOR_SEED = 0x5a5a5a5a5a5a5a5aL;
  private static final long UNIFORM_SEED = 0xa1b2c3d4e5f60708L;

  // Iceberg PFOR input arrays
  private int[] descriptorValues;
  private int[] uniformValues;

  // Pre-encoded buffers for decode benchmarks
  private ByteBuffer descriptorEncoded;
  private ByteBuffer uniformEncoded;

  // Reusable encode buffer (avoids allocation in the encode hot path)
  private ByteBuffer encodeBuffer;

  // JavaFastPFOR compressed arrays (pre-encoded for decode benchmarks)
  private int[] descriptorFastPFOREncoded;
  private int[] uniformFastPFOREncoded;

  // Reusable output buffers for JavaFastPFOR (avoids allocation in hot path)
  private int[] fastPFOROutputBuffer;

  @Setup
  public void setupBenchmark() {
    // 256-value descriptor-like data: mostly [0,31] with ~5% [0,255] outliers
    descriptorValues = PFOREncodingTestUtils.sparse(256, DESCRIPTOR_SEED, 5);

    // 256-value uniform byte data
    uniformValues = PFOREncodingTestUtils.uniform(256, UNIFORM_SEED, 255);

    // Reusable encode buffer: worst-case for a single 256-value chunk
    encodeBuffer = ByteBuffer.allocate(3 + 256);

    // Pre-encode for decode benchmarks
    descriptorEncoded = PFOREncoding.encode(descriptorValues);
    uniformEncoded = PFOREncoding.encode(uniformValues);

    // Pre-encode with JavaFastPFOR for decode benchmarks
    FastPFOR128 codec = new FastPFOR128();
    descriptorFastPFOREncoded = fastPFOREncode(codec, descriptorValues);
    uniformFastPFOREncoded = fastPFOREncode(codec, uniformValues);

    // Output buffer large enough for any 256-value decoded result
    fastPFOROutputBuffer = new int[256 + 1024];
  }

  // ---------------------------------------------------------------------------
  // Iceberg PFOR — descriptor data shape
  // ---------------------------------------------------------------------------

  @Benchmark
  @Threads(1)
  public void encodeDescriptorIceberg(Blackhole blackhole) {
    blackhole.consume(PFOREncoding.encode(descriptorValues, descriptorValues.length, encodeBuffer));
  }

  @Benchmark
  @Threads(1)
  public void decodeDescriptorIceberg(Blackhole blackhole) {
    blackhole.consume(PFOREncoding.decode(descriptorEncoded, descriptorValues.length));
  }

  // ---------------------------------------------------------------------------
  // Iceberg PFOR — uniform byte data shape
  // ---------------------------------------------------------------------------

  @Benchmark
  @Threads(1)
  public void encodeUniformIceberg(Blackhole blackhole) {
    blackhole.consume(PFOREncoding.encode(uniformValues, uniformValues.length, encodeBuffer));
  }

  @Benchmark
  @Threads(1)
  public void decodeUniformIceberg(Blackhole blackhole) {
    blackhole.consume(PFOREncoding.decode(uniformEncoded, uniformValues.length));
  }

  // ---------------------------------------------------------------------------
  // JavaFastPFOR — descriptor data shape
  // ---------------------------------------------------------------------------

  @Benchmark
  @Threads(1)
  public void encodeDescriptorFastPFOR(Blackhole blackhole) {
    FastPFOR128 codec = new FastPFOR128();
    blackhole.consume(fastPFOREncode(codec, descriptorValues));
  }

  @Benchmark
  @Threads(1)
  public void decodeDescriptorFastPFOR(Blackhole blackhole) {
    FastPFOR128 codec = new FastPFOR128();
    blackhole.consume(fastPFORDecode(codec, descriptorFastPFOREncoded, descriptorValues.length));
  }

  // ---------------------------------------------------------------------------
  // JavaFastPFOR — uniform byte data shape
  // ---------------------------------------------------------------------------

  @Benchmark
  @Threads(1)
  public void encodeUniformFastPFOR(Blackhole blackhole) {
    FastPFOR128 codec = new FastPFOR128();
    blackhole.consume(fastPFOREncode(codec, uniformValues));
  }

  @Benchmark
  @Threads(1)
  public void decodeUniformFastPFOR(Blackhole blackhole) {
    FastPFOR128 codec = new FastPFOR128();
    blackhole.consume(fastPFORDecode(codec, uniformFastPFOREncoded, uniformValues.length));
  }

  // ---------------------------------------------------------------------------
  // JavaFastPFOR helpers
  // ---------------------------------------------------------------------------

  private static int[] fastPFOREncode(FastPFOR128 codec, int[] values) {
    int[] output = new int[values.length + 1024];
    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);
    codec.compress(values, inPos, values.length, output, outPos);
    int[] result = new int[outPos.get()];
    System.arraycopy(output, 0, result, 0, outPos.get());
    return result;
  }

  private static int[] fastPFORDecode(FastPFOR128 codec, int[] encoded, int count) {
    int[] output = new int[count];
    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);
    codec.uncompress(encoded, inPos, encoded.length, output, outPos);
    return output;
  }
}
