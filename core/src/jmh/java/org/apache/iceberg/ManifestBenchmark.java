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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
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
 * A benchmark that measures manifest read/write performance across format versions and file
 * formats.
 *
 * <p>V1-V3 only support Avro manifests. V4 supports both Avro and Parquet. The {@code
 * versionFormat} parameter encodes valid combinations as {@code "<version>_<format>"} (e.g. {@code
 * "4_PARQUET"}) so that only meaningful pairings are benchmarked.
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
 * # V4-only (Avro vs Parquet)
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestBenchmark \
 *     -PjmhParams="versionFormat=4_AVRO|4_PARQUET"
 *
 * # all versions, single column count
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestBenchmark \
 *     -PjmhParams="numCols=50"
 *
 * # single version
 * ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestBenchmark \
 *     -PjmhParams="versionFormat=3_AVRO"
 * }</pre>
 */
@Fork(1)
@State(Scope.Benchmark)
// Parquet's columnar write path has a deep call graph (per-column encoders, page assembly,
// dictionary management) that requires more warmup iterations than Avro for the JIT compiler to
// fully optimize. Profiling shows ~650ms of JIT compilation spread across the first 3-4
// iterations, so 6 warmups ensure measurement begins after JIT has stabilized.
@Warmup(iterations = 6)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class ManifestBenchmark {

  static final int ENTRY_BASE = 300_000;

  @Param({"1_AVRO", "2_AVRO", "3_AVRO", "4_AVRO", "4_PARQUET"})
  private String versionFormat;

  @Param({"true", "false"})
  private String partitioned;

  @Param({"10", "50", "100"})
  private int numCols;

  private int formatVersion;
  private FileFormat fileFormat;
  private PartitionSpec spec;
  private Map<Integer, PartitionSpec> specsById;
  private List<DataFile> dataFiles;

  private String writeBaseDir;
  private OutputFile writeOutputFile;

  private String readBaseDir;
  private ManifestFile readManifest;

  @Setup(Level.Trial)
  public void setupTrial() {
    String[] parts = versionFormat.split("_", 2);
    this.formatVersion = Integer.parseInt(parts[0]);
    this.fileFormat = FileFormat.fromString(parts[1]);
    this.spec =
        Boolean.parseBoolean(partitioned)
            ? ManifestBenchmarkUtil.SPEC
            : PartitionSpec.unpartitioned();
    this.specsById = ImmutableMap.of(spec.specId(), spec);
    int numEntries = ManifestBenchmarkUtil.entriesForColumnCount(ENTRY_BASE, numCols);
    this.dataFiles = ManifestBenchmarkUtil.generateDataFiles(spec, numEntries, numCols);
    setupReadManifest();
  }

  @Setup(Level.Invocation)
  public void setupWriteInvocation() throws IOException {
    this.writeBaseDir =
        java.nio.file.Files.createTempDirectory("bench-write-").toAbsolutePath().toString();
    this.writeOutputFile =
        Files.localOutput(
            String.format(Locale.ROOT, "%s/%s", writeBaseDir, fileFormat.addExtension("manifest")));

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

  @Benchmark
  @Threads(1)
  public ManifestFile writeManifest() throws IOException {
    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, writeOutputFile, 1L);

    try (ManifestWriter<DataFile> w = writer) {
      for (DataFile file : dataFiles) {
        w.add(file);
      }
    }

    return writer.toManifestFile();
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
        Files.localOutput(
            String.format(Locale.ROOT, "%s/%s", readBaseDir, fileFormat.addExtension("manifest")));

    ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, manifestFile, 1L);

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
