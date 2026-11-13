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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

/** Measures manifest planning for IN predicates on bucketed files with broad or narrow bounds. */
@Fork(
    value = 1,
    jvmArgsAppend = {"-Xms512m", "-Xmx1500m"})
@State(Scope.Benchmark)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Timeout(time = 5, timeUnit = TimeUnit.MINUTES)
public class BucketInManifestPlanningBenchmark {
  private static final int NUM_BUCKETS = 1_000;
  private static final int FILES_PER_BUCKET = 10;
  private static final int VALUES_PER_BUCKET = 100;
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "key", Types.IntegerType.get()));
  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("key", NUM_BUCKETS).build();
  private static final Map<Integer, PartitionSpec> SPECS = ImmutableMap.of(SPEC.specId(), SPEC);

  @Param({"broad", "narrow"})
  private String layout;

  @Param({"1", "50", "200", "400"})
  private int keyCount;

  private String tempDir;
  private ManifestFile manifest;
  private Expression filter;

  @Setup(Level.Trial)
  public void setup() {
    Function<Integer, Integer> bucket =
        Transforms.<Integer>bucket(NUM_BUCKETS).bind(Types.IntegerType.get());
    List<List<Integer>> valuesByBucket = new ArrayList<>(NUM_BUCKETS);
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId += 1) {
      valuesByBucket.add(new ArrayList<>());
    }

    int candidate = 0;
    int completeBuckets = 0;
    while (completeBuckets < NUM_BUCKETS) {
      int bucketId = bucket.apply(candidate);
      List<Integer> values = valuesByBucket.get(bucketId);
      if (values.size() < VALUES_PER_BUCKET) {
        values.add(candidate);
        if (values.size() == VALUES_PER_BUCKET) {
          completeBuckets += 1;
        }
      }
      candidate += 1;
    }

    List<Integer> storedValues = new ArrayList<>(NUM_BUCKETS * VALUES_PER_BUCKET);
    for (List<Integer> values : valuesByBucket) {
      storedValues.addAll(values);
    }
    storedValues.sort(Integer::compareTo);

    List<Integer> queryValues = new ArrayList<>(keyCount);
    for (int index = 0; index < keyCount; index += 1) {
      queryValues.add(storedValues.get(index * (storedValues.size() - 1) / keyCount));
    }
    this.filter = Expressions.in("key", queryValues);

    List<DataFile> files = new ArrayList<>(NUM_BUCKETS * FILES_PER_BUCKET);
    for (int bucketId = 0; bucketId < NUM_BUCKETS; bucketId += 1) {
      List<Integer> values = valuesByBucket.get(bucketId);
      for (int fileIndex = 0; fileIndex < FILES_PER_BUCKET; fileIndex += 1) {
        int lower;
        int upper;
        if ("broad".equals(layout)) {
          lower = values.get(0);
          upper = values.get(values.size() - 1);
        } else {
          int valuesPerFile = values.size() / FILES_PER_BUCKET;
          lower = values.get(fileIndex * valuesPerFile);
          upper = values.get((fileIndex + 1) * valuesPerFile - 1);
        }
        files.add(dataFile(bucketId, fileIndex, lower, upper));
      }
    }

    try {
      this.tempDir =
          java.nio.file.Files.createTempDirectory("bucket-in-planning-")
              .toAbsolutePath()
              .toString();
      OutputFile output = Files.localOutput(tempDir + "/manifest.avro");
      ManifestWriter<DataFile> writer = ManifestFiles.write(2, SPEC, output, 1L);
      try (ManifestWriter<DataFile> closeableWriter = writer) {
        for (DataFile file : files) {
          closeableWriter.add(file);
        }
      }
      this.manifest = writer.toManifestFile();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    System.err.printf(
        Locale.ROOT,
        "fixture layout=%s keys=%d selectedFiles=%d%n",
        layout,
        keyCount,
        selectedFiles());
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    ManifestBenchmarkUtil.cleanDir(tempDir);
  }

  @Benchmark
  @Threads(1)
  public int planManifestFiles() {
    return selectedFiles();
  }

  private int selectedFiles() {
    int selected = 0;
    TestTables.LocalFileIO fileIO = new TestTables.LocalFileIO();
    try (CloseableIterator<DataFile> files =
        ManifestFiles.read(manifest, fileIO, SPECS).filterRows(filter).iterator()) {
      while (files.hasNext()) {
        files.next();
        selected += 1;
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return selected;
  }

  private static DataFile dataFile(int bucket, int file, int lower, int upper) {
    PartitionData partition = new PartitionData(SPEC.partitionType());
    partition.set(0, bucket);
    return DataFiles.builder(SPEC)
        .withPath(
            String.format(Locale.ROOT, "/path/to/bucket-%04d-file-%02d.parquet", bucket, file))
        .withFileSizeInBytes(128L * 1024 * 1024)
        .withPartition(partition)
        .withMetrics(
            new Metrics(
                1_000L,
                null,
                ImmutableMap.of(1, 1_000L),
                ImmutableMap.of(1, 0L),
                null,
                ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), lower)),
                ImmutableMap.of(1, Conversions.toByteBuffer(Types.IntegerType.get(), upper))))
        .build();
  }
}
