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
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
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

/**
 * A benchmark that compares manifest write performance between v3 (metrics-in-maps) and v4
 * (content_stats structs).
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestWriteStatsBenchmark -PjmhOutputPath=benchmark/manifest-write-stats.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class ManifestWriteStatsBenchmark {
  private static final int NUM_ROWS = 100_000;

  private String baseDir;
  private PartitionSpec spec;
  private Schema schema;
  private DataFileSet dataFiles;

  @Param({"32", "64", "100", "250"})
  private int numberOfColumns;

  @Param({"3", "4"})
  private int formatVersion;

  @Setup
  public void before() {
    Metrics metrics = randomMetrics();

    List<Types.NestedField> fields = Lists.newArrayList();
    for (int j = 0; j < numberOfColumns; j++) {
      fields.add(Types.NestedField.optional(j, "id" + j, Types.LongType.get()));
    }

    this.schema = new Schema(fields);
    this.spec = PartitionSpec.builderFor(schema).build();
    this.dataFiles = DataFileSet.create();

    for (int j = 0; j < NUM_ROWS; j++) {
      DataFiles.Builder builder =
          DataFiles.builder(spec)
              .withFormat(FileFormat.PARQUET)
              .withPath(String.format("/path/to/data-%s.parquet", j))
              .withFileSizeInBytes(j)
              .withRecordCount(j);

      if (formatVersion < 4) {
        builder.withMetrics(metrics);
      } else {
        builder.withContentStats(MetricsUtil.fromMetrics(schema, metrics));
      }

      dataFiles.add(builder.build());
    }
  }

  @TearDown(Level.Invocation)
  public void after() {
    if (baseDir != null) {
      FileUtils.deleteQuietly(new File(baseDir));
      baseDir = null;
    }
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
  public void writeSingleManifestFile(FileSizeCounters counters) throws IOException {
    this.baseDir =
        java.nio.file.Files.createTempDirectory("benchmark-").toAbsolutePath().toString();
    String manifestListFile = String.format("%s/%s.avro", baseDir, UUID.randomUUID());

    try (ManifestListWriter listWriter =
        ManifestLists.write(
            formatVersion,
            Files.localOutput(manifestListFile),
            PlaintextEncryptionManager.instance(),
            1L,
            null,
            0,
            0L)) {
      OutputFile manifestFile =
          Files.localOutput(String.format("%s/%s.avro", baseDir, UUID.randomUUID()));

      ManifestWriter<DataFile> writer = ManifestFiles.write(formatVersion, spec, manifestFile, 1L);
      try (ManifestWriter<DataFile> finalWriter = writer) {
        dataFiles.forEach(finalWriter::add);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      ManifestFile file = writer.toManifestFile();
      counters.manifestSizeMB = file.length() / (1024.0 * 1024.0);

      listWriter.add(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private Metrics randomMetrics() {
    Random random = new Random(42);
    long rowCount = 100_000L + random.nextInt(1000);
    Map<Integer, Long> columnSizes = Maps.newHashMap();
    Map<Integer, Long> valueCounts = Maps.newHashMap();
    Map<Integer, Long> nullValueCounts = Maps.newHashMap();
    Map<Integer, Long> nanValueCounts = Maps.newHashMap();
    Map<Integer, ByteBuffer> lowerBounds = Maps.newHashMap();
    Map<Integer, ByteBuffer> upperBounds = Maps.newHashMap();
    Map<Integer, Type> originalTypes = Maps.newHashMap();
    for (int i = 0; i < numberOfColumns; i++) {
      columnSizes.put(i, 1_000_000L + random.nextInt(100_000));
      valueCounts.put(i, 100_000L + random.nextInt(100));
      nullValueCounts.put(i, (long) random.nextInt(5));
      nanValueCounts.put(i, (long) random.nextInt(5));

      long bound = random.nextLong();
      lowerBounds.put(i, Conversions.toByteBuffer(Types.LongType.get(), bound));
      upperBounds.put(i, Conversions.toByteBuffer(Types.LongType.get(), bound));
      originalTypes.put(i, Types.LongType.get());
    }

    return new Metrics(
        rowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds,
        originalTypes);
  }
}
