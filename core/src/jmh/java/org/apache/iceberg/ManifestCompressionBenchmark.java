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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFile;
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
 * <p>Entry counts are calibrated per column count via {@link ManifestBenchmarkUtil#ENTRY_BASE}. Set
 * to 300_000 for ~8 MB manifests (matching the default {@code commit.manifest.target-size-bytes})
 * or 15_000 for ~400 KB.
 *
 * <p>To run this benchmark:
 *
 * <pre>{@code
 * # all combinations
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestCompressionBenchmark
 *
 * # single codec
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestCompressionBenchmark \
 *     -PjmhParams="codec=gzip"
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 6)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class ManifestCompressionBenchmark {

  private static final int FORMAT_VERSION = 4;

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

  private String writeBaseDir;
  private OutputFile writeOutputFile;

  private String readBaseDir;
  private ManifestFile readManifest;

  @Setup(Level.Trial)
  public void setupTrial() {
    this.spec =
        Boolean.parseBoolean(partitioned)
            ? ManifestBenchmarkUtil.SPEC
            : PartitionSpec.unpartitioned();
    this.specsById = Map.of(spec.specId(), spec);
    this.writerProperties = Map.of(TableProperties.AVRO_COMPRESSION, codec);
    int numEntries = ManifestBenchmarkUtil.entriesForColumnCount(numCols);
    this.dataFiles = ManifestBenchmarkUtil.generateDataFiles(spec, numEntries, numCols);
    setupReadManifest();
  }

  @Setup(Level.Invocation)
  public void setupWriteInvocation() throws IOException {
    this.writeBaseDir =
        java.nio.file.Files.createTempDirectory("bench-write-").toAbsolutePath().toString();
    this.writeOutputFile =
        Files.localOutput(String.format(Locale.ROOT, "%s/manifest.avro", writeBaseDir));

    for (DataFile file : dataFiles) {
      file.path();
      file.fileSizeInBytes();
      file.recordCount();
    }
  }

  @TearDown(Level.Trial)
  public void tearDownTrial() {
    ManifestBenchmarkUtil.cleanDir(readBaseDir);
    readBaseDir = null;
    readManifest = null;
    dataFiles = null;
  }

  @TearDown(Level.Invocation)
  public void tearDownInvocation() {
    ManifestBenchmarkUtil.cleanDir(writeBaseDir);
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
      this.readBaseDir =
          java.nio.file.Files.createTempDirectory("bench-read-").toAbsolutePath().toString();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    OutputFile manifestFile =
        Files.localOutput(String.format(Locale.ROOT, "%s/manifest.avro", readBaseDir));

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
}
