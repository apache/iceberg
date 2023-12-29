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

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.Files;
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
import org.openjdk.jmh.annotations.Timeout;

/**
 * A benchmark that evaluates the performance of writing manifest files
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestWriteBenchmark
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class ManifestWriteBenchmark {

  private static final int NUM_FILES = 10;
  private static final int NUM_ROWS = 100000;
  private static final int NUM_COLS = 100;

  private String baseDir;
  private String manifestListFile;

  private Metrics metrics;

  @Setup
  public void before() {
    Random random = new Random(System.currentTimeMillis());
    // Pre-create the metrics to avoid doing this in the benchmark itself
    metrics = randomMetrics(random);
  }

  @TearDown
  public void after() {
    if (baseDir != null) {
      FileUtils.deleteQuietly(new File(baseDir));
      baseDir = null;
    }

    manifestListFile = null;
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"1", "2"})
    public int formatVersion;
  }

  @Benchmark
  @Threads(1)
  public void writeManifestFile(BenchmarkState state) throws IOException {
    this.baseDir = Files.createTempDir().getAbsolutePath();
    this.manifestListFile = String.format("%s/%s.avro", baseDir, UUID.randomUUID());

    try (ManifestListWriter listWriter =
        ManifestLists.write(
            state.formatVersion,
            org.apache.iceberg.Files.localOutput(manifestListFile),
            0,
            1L,
            0)) {
      for (int i = 0; i < NUM_FILES; i++) {
        OutputFile manifestFile =
            org.apache.iceberg.Files.localOutput(
                String.format("%s/%s.avro", baseDir, UUID.randomUUID()));

        ManifestWriter<DataFile> writer =
            ManifestFiles.write(
                state.formatVersion, PartitionSpec.unpartitioned(), manifestFile, 1L);
        try (ManifestWriter<DataFile> finalWriter = writer) {
          for (int j = 0; j < NUM_ROWS; j++) {
            DataFile dataFile =
                DataFiles.builder(PartitionSpec.unpartitioned())
                    .withFormat(FileFormat.PARQUET)
                    .withPath(String.format("/path/to/data-%s-%s.parquet", i, j))
                    .withFileSizeInBytes(j)
                    .withRecordCount(j)
                    .withMetrics(metrics)
                    .build();
            finalWriter.add(dataFile);
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        listWriter.add(writer.toManifestFile());
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Metrics randomMetrics(Random random) {
    long rowCount = 100000L + random.nextInt(1000);
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    for (int i = 0; i < NUM_COLS; i++) {
      columnSizes.put(i, 1000000L + random.nextInt(100000));
      valueCounts.put(i, 100000L + random.nextInt(100));
      nullValueCounts.put(i, (long) random.nextInt(5));
      nanValueCounts.put(i, (long) random.nextInt(5));
      byte[] lower = new byte[8];
      random.nextBytes(lower);
      lowerBounds.put(i, ByteBuffer.wrap(lower));
      byte[] upper = new byte[8];
      random.nextBytes(upper);
      upperBounds.put(i, ByteBuffer.wrap(upper));
    }

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }
}
