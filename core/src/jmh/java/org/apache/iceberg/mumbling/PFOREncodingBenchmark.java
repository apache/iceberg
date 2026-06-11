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
import java.util.Random;
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

  // Iceberg PFOR input arrays
  private int[] descriptorValues;
  private int[] uniformValues;

  // Pre-encoded buffers for decode benchmarks
  private ByteBuffer descriptorEncoded;
  private ByteBuffer uniformEncoded;

  // Reusable encode buffer (avoids allocation in the encode hot path)
  private ByteBuffer encodeBuffer;

  // Reusable decode output (avoids allocation in the decode hot path)
  private int[] decodeOutput;

  // JavaFastPFOR compressed arrays (pre-encoded for decode benchmarks)
  private int[] descriptorFastPFOREncoded;
  private int[] uniformFastPFOREncoded;

  // Reusable output buffers for JavaFastPFOR (avoids allocation in hot path)
  private int[] fastPFOROutputBuffer;

  @Setup
  public void setupBenchmark() {
    Random random = new Random(1938745);

    // 256-value descriptor-like data: mostly [0,31] with ~5% [0,255] outliers
    descriptorValues = PFORRandomData.exceptions(random, 256, 0.05f);

    // 256-value uniform byte data
    uniformValues = PFORRandomData.uniform(random, 256, 255);

    // Reusable encode buffer: worst-case for a single 256-value chunk
    encodeBuffer = ByteBuffer.allocate(3 + 256);

    // Reusable decode output: one entry per value in a chunk
    decodeOutput = new int[256];

    // Pre-encode for decode benchmarks
    descriptorEncoded = PFOREncoding.encode(descriptorValues, descriptorValues.length);
    uniformEncoded = PFOREncoding.encode(uniformValues, uniformValues.length);

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
    blackhole.consume(
        PFOREncoding.encode(
            descriptorValues, 0, encodeBuffer, encodeBuffer.position(), descriptorValues.length));
  }

  @Benchmark
  @Threads(1)
  public void decodeDescriptorIceberg(Blackhole blackhole) {
    PFOREncoding.decode(
        descriptorEncoded, descriptorEncoded.position(), decodeOutput, 0, descriptorValues.length);
    blackhole.consume(decodeOutput);
  }

  // ---------------------------------------------------------------------------
  // Iceberg PFOR — uniform byte data shape
  // ---------------------------------------------------------------------------

  @Benchmark
  @Threads(1)
  public void encodeUniformIceberg(Blackhole blackhole) {
    blackhole.consume(
        PFOREncoding.encode(
            uniformValues, 0, encodeBuffer, encodeBuffer.position(), uniformValues.length));
  }

  @Benchmark
  @Threads(1)
  public void decodeUniformIceberg(Blackhole blackhole) {
    PFOREncoding.decode(
        uniformEncoded, uniformEncoded.position(), decodeOutput, 0, uniformValues.length);
    blackhole.consume(decodeOutput);
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
    blackhole.consume(fastPFORDecode(codec, descriptorFastPFOREncoded, fastPFOROutputBuffer));
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
    blackhole.consume(fastPFORDecode(codec, uniformFastPFOREncoded, fastPFOROutputBuffer));
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

  private static int[] fastPFORDecode(FastPFOR128 codec, int[] encoded, int[] output) {
    IntWrapper inPos = new IntWrapper(0);
    IntWrapper outPos = new IntWrapper(0);
    codec.uncompress(encoded, inPos, encoded.length, output, outPos);
    return output;
  }
}
