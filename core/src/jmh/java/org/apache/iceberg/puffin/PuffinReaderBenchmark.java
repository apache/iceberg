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
package org.apache.iceberg.puffin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.inmemory.InMemoryInputFile;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.util.Pair;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * Compares coalesced Puffin blob reads with reading each blob independently.
 *
 * <p>Run with:
 *
 * <pre>{@code
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=PuffinReaderBenchmark
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class PuffinReaderBenchmark {
  @Param({"16", "64"})
  private int blobCount;

  @Param({"1024", "65536"})
  private int blobSize;

  @Param({"0", "4096", "1048577"})
  private int gapSize;

  private InputFile inputFile;
  private List<BlobMetadata> blobs;

  @Setup
  public void setup() {
    int fileSize =
        Math.toIntExact((long) blobCount * blobSize + (long) (blobCount - 1) * gapSize);
    this.inputFile = new InMemoryInputFile(new byte[fileSize]);
    this.blobs = new ArrayList<>(blobCount);

    long offset = 0;
    for (int index = 0; index < blobCount; index++) {
      blobs.add(
          new BlobMetadata(
              "benchmark",
              List.of(),
              1,
              1,
              offset,
              blobSize,
              PuffinCompressionCodec.NONE.codecName(),
              Map.of()));
      offset += blobSize + gapSize;
    }
  }

  @Benchmark
  public void readCoalesced(Blackhole blackhole) throws IOException {
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      consume(reader.readAll(blobs), blackhole);
    }
  }

  @Benchmark
  public void readIndividually(Blackhole blackhole) throws IOException {
    try (PuffinReader reader = Puffin.read(inputFile).build()) {
      for (BlobMetadata blob : blobs) {
        consume(reader.readAll(List.of(blob)), blackhole);
      }
    }
  }

  private static void consume(
      Iterable<Pair<BlobMetadata, ByteBuffer>> read, Blackhole blackhole) {
    for (Pair<BlobMetadata, ByteBuffer> blob : read) {
      blackhole.consume(blob.second());
    }
  }
}
