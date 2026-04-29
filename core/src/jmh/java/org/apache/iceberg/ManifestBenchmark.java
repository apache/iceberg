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
import java.nio.file.Files;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that measures manifest read/write performance across compression codecs.
 *
 * <p>Entry counts are calibrated per column count via {@link #ENTRY_BASE}. Set to 300_000 for ~8 MB
 * manifests (matching the default {@code commit.manifest.target-size-bytes}) or 15_000 for ~400 KB.
 *
 * <p>To run this benchmark:
 *
 * <pre>{@code
 * # all combinations
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestBenchmark
 *
 * # single codec
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestBenchmark \
 *     -PjmhParams="codec=gzip"
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 6)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class ManifestBenchmark {

  static final int ENTRY_BASE = 300_000;

  private static final int FORMAT_VERSION = 4;

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "data", Types.StringType.get()),
          Types.NestedField.required(3, "customer", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("id").identity("data").identity("customer").build();

  @Param({"gzip", "snappy", "zstd", "uncompressed"})
  private String codec;

  @Param({"true", "false"})
  private String partitioned;

  @Param({"10", "50", "100"})
  private int numCols;

  private PartitionSpec spec;
  private Map<Integer, PartitionSpec> specsById;
  private Map<String, String> writerProperties;
  private List<DataFile> dataFiles;
  private int numEntries;

  private String writeBaseDir;
  private OutputFile writeOutputFile;

  private String readBaseDir;
  private ManifestFile readManifest;

  @Setup(Level.Trial)
  public void setupTrial() {
    this.spec = Boolean.parseBoolean(partitioned) ? SPEC : PartitionSpec.unpartitioned();
    this.specsById = Map.of(spec.specId(), spec);
    this.writerProperties = Map.of(TableProperties.AVRO_COMPRESSION, codec);
    // ENTRY_BASE / cols: empirically calibrated — 300_000 → ~8 MB, 15_000 → ~400 KB manifests
    this.numEntries = ENTRY_BASE / numCols;
    this.dataFiles = generateDataFiles();
    setupReadManifest();
  }

  @Setup(Level.Invocation)
  public void setupWriteInvocation() throws IOException {
    this.writeBaseDir = Files.createTempDirectory("bench-write-").toAbsolutePath().toString();
    this.writeOutputFile =
        org.apache.iceberg.Files.localOutput(
            String.format(Locale.ROOT, "%s/manifest.avro", writeBaseDir));

    for (DataFile file : dataFiles) {
      file.path();
      file.fileSizeInBytes();
      file.recordCount();
    }
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    cleanDir(readBaseDir);
    readBaseDir = null;
    readManifest = null;
    dataFiles = null;
  }

  @TearDown(Level.Invocation)
  public void tearDownInvocation() {
    cleanDir(writeBaseDir);
    writeBaseDir = null;
    writeOutputFile = null;
  }

  @AuxCounters(AuxCounters.Type.EVENTS)
  @State(Scope.Thread)
  @SuppressWarnings("checkstyle:VisibilityModifier")
  public static class FileSizeCounters {
    public double manifestSizeMB;

    @Setup(Level.Invocation)
    public void reset() {
      manifestSizeMB = 0;
    }
  }

  @Benchmark
  @Threads(1)
  public ManifestFile writeManifest(FileSizeCounters counters) throws IOException {
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(FORMAT_VERSION, spec, writeOutputFile, 1L, writerProperties);

    try (ManifestWriter<DataFile> w = writer) {
      for (DataFile file : dataFiles) {
        w.add(file);
      }
    }

    ManifestFile manifest = writer.toManifestFile();
    counters.manifestSizeMB = manifest.length() / (1024.0 * 1024.0);
    return manifest;
  }

  @Benchmark
  @Threads(1)
  public void readManifest(Blackhole blackhole) throws IOException {
    TestTables.LocalFileIO fileIO = new TestTables.LocalFileIO();
    try (CloseableIterator<DataFile> it =
        ManifestFiles.read(readManifest, fileIO, specsById).iterator()) {
      while (it.hasNext()) {
        blackhole.consume(it.next());
      }
    }
  }

  private void setupReadManifest() {
    try {
      this.readBaseDir = Files.createTempDirectory("bench-read-").toAbsolutePath().toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    OutputFile manifestFile =
        org.apache.iceberg.Files.localOutput(
            String.format(Locale.ROOT, "%s/manifest.avro", readBaseDir));

    ManifestWriter<DataFile> writer =
        ManifestFiles.write(FORMAT_VERSION, spec, manifestFile, 1L, writerProperties);

    try (ManifestWriter<DataFile> w = writer) {
      for (DataFile file : dataFiles) {
        w.add(file);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    this.readManifest = writer.toManifestFile();
  }

  private List<DataFile> generateDataFiles() {
    Random random = new Random(42);
    List<DataFile> files = Lists.newArrayListWithCapacity(numEntries);
    for (int i = 0; i < numEntries; i++) {
      DataFiles.Builder builder =
          DataFiles.builder(spec)
              .withFormat(FileFormat.PARQUET)
              .withPath(String.format(Locale.ROOT, "/path/to/data-%d.parquet", i))
              .withFileSizeInBytes(1024 + i)
              .withRecordCount(1000 + i)
              .withMetrics(randomMetrics(random, numCols));

      if (!spec.isUnpartitioned()) {
        builder.withPartitionPath(
            String.format(
                Locale.ROOT, "id=%d/data=val-%d/customer=cust-%d", i % 100, i % 50, i % 200));
      }

      files.add(builder.build());
    }

    return files;
  }

  static Metrics randomMetrics(Random random, int cols) {
    long rowCount = 100_000L + random.nextInt(1000);
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    for (int i = 0; i < cols; i++) {
      columnSizes.put(i, 1_000_000L + random.nextInt(100_000));
      valueCounts.put(i, 100_000L + random.nextInt(100));
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

  private static void cleanDir(String dir) {
    if (dir != null) {
      FileUtils.deleteQuietly(new File(dir));
    }
  }
}
