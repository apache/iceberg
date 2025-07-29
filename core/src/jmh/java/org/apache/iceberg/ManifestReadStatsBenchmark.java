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

import static org.assertj.core.api.Assertions.assertThat;

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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.stats.FieldStats;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DataFileSet;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that compares manifest read performance between v3 (metrics-in-maps) and v4
 * (content_stats structs) for different read patterns: single stat, bounds, and all stats for a
 * column.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh -PjmhIncludeRegex=ManifestReadStatsBenchmark -PjmhOutputPath=benchmark/manifest-read-stats.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class ManifestReadStatsBenchmark {
  private static final int NUM_ROWS = 100_000;

  private String baseDir;
  private String manifestListFile;
  private PartitionSpec spec;
  private Schema schema;
  private DataFileSet dataFiles;

  @Param({"32", "64", "100", "250"})
  private int numberOfColumns;

  @Param({"3", "4"})
  private int formatVersion;

  @Setup
  public void before() throws IOException {
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

    writeSingleManifestFile();
  }

  @TearDown
  public void after() {
    if (baseDir != null) {
      FileUtils.deleteQuietly(new File(baseDir));
      baseDir = null;
    }

    manifestListFile = null;
  }

  private void writeSingleManifestFile() throws IOException {
    this.baseDir =
        java.nio.file.Files.createTempDirectory("benchmark-").toAbsolutePath().toString();
    this.manifestListFile = String.format("%s/%s.avro", baseDir, UUID.randomUUID());

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

      ManifestFile manifest = writer.toManifestFile();
      listWriter.add(manifest);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Benchmark
  @Threads(1)
  public void readSingleStatForColumn(Blackhole blackhole) {
    List<ManifestFile> manifests =
        ManifestLists.read(org.apache.iceberg.Files.localInput(manifestListFile));
    TestTables.LocalFileIO fileIO = new TestTables.LocalFileIO();
    if (formatVersion <= 3) {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(ImmutableList.of("value_counts"));
      DataFile dataFile = reader.iterator().next();
      Long valueCount = dataFile.valueCounts().get(10);
      assertThat(valueCount).isNotNull();
      blackhole.consume(valueCount);
    } else {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(ImmutableList.of("content_stats.10.value_count"));
      DataFile dataFile = reader.iterator().next();
      FieldStats<?> stat = dataFile.contentStats().statsFor(10);
      assertThat(stat).isNotNull();
      blackhole.consume(stat.valueCount());
    }
  }

  @Benchmark
  public void readUpperLowerBoundsForColumn(Blackhole blackhole) {
    List<ManifestFile> manifests =
        ManifestLists.read(org.apache.iceberg.Files.localInput(manifestListFile));
    TestTables.LocalFileIO fileIO = new TestTables.LocalFileIO();
    if (formatVersion <= 3) {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(ImmutableList.of("lower_bounds", "upper_bounds"));
      DataFile dataFile = reader.iterator().next();
      ByteBuffer lower = dataFile.lowerBounds().get(10);
      assertThat(lower).isNotNull();
      blackhole.consume(lower);
      ByteBuffer upper = dataFile.upperBounds().get(10);
      assertThat(upper).isNotNull();
      blackhole.consume(upper);
    } else {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(
                  ImmutableList.of("content_stats.10.lower_bound", "content_stats.10.upper_bound"));
      DataFile dataFile = reader.iterator().next();
      FieldStats<?> stat = dataFile.contentStats().statsFor(10);
      assertThat(stat).isNotNull();
      assertThat(stat.lowerBound()).isNotNull();
      assertThat(stat.upperBound()).isNotNull();
      blackhole.consume(stat.lowerBound());
      blackhole.consume(stat.upperBound());
    }
  }

  @Benchmark
  public void readAllStatsForSingleColumn(Blackhole blackhole) {
    List<ManifestFile> manifests =
        ManifestLists.read(org.apache.iceberg.Files.localInput(manifestListFile));
    TestTables.LocalFileIO fileIO = new TestTables.LocalFileIO();
    if (formatVersion <= 3) {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(
                  ImmutableList.of(
                      "column_sizes",
                      "value_counts",
                      "null_value_counts",
                      "nan_value_counts",
                      "lower_bounds",
                      "upper_bounds"));
      DataFile dataFile = reader.iterator().next();
      Long columnSizes = dataFile.columnSizes().get(10);
      Long valueCounts = dataFile.valueCounts().get(10);
      Long nullValueCounts = dataFile.nullValueCounts().get(10);
      Long nanValueCounts = dataFile.nanValueCounts().get(10);
      ByteBuffer lower = dataFile.lowerBounds().get(10);
      assertThat(lower).isNotNull();
      ByteBuffer upper = dataFile.upperBounds().get(10);
      assertThat(upper).isNotNull();
      blackhole.consume(columnSizes);
      blackhole.consume(valueCounts);
      blackhole.consume(nullValueCounts);
      blackhole.consume(nanValueCounts);
      blackhole.consume(lower);
      blackhole.consume(upper);
    } else {
      ManifestReader<DataFile> reader =
          ManifestFiles.read(manifests.get(0), fileIO, null)
              .select(ImmutableList.of("content_stats.10"));
      DataFile dataFile = reader.iterator().next();
      FieldStats<?> stat = dataFile.contentStats().statsFor(10);
      assertThat(stat).isNotNull();
      assertThat(stat.valueCount()).isNotNull();
      assertThat(stat.lowerBound()).isNotNull();
      assertThat(stat.upperBound()).isNotNull();
      blackhole.consume(stat.valueCount());
      blackhole.consume(stat.nullValueCount());
      blackhole.consume(stat.nanValueCount());
      blackhole.consume(stat.lowerBound());
      blackhole.consume(stat.upperBound());
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
