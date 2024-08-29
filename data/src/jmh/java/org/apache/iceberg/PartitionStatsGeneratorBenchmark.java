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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.data.PartitionStatsHandler;
import org.apache.iceberg.data.PartitionStatsRecord;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Timeout;
import org.openjdk.jmh.annotations.Warmup;

@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 2)
@Measurement(iterations = 5)
@Timeout(time = 1000, timeUnit = TimeUnit.HOURS)
@BenchmarkMode(Mode.SingleShotTime)
public class PartitionStatsGeneratorBenchmark {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "c1", Types.IntegerType.get()),
          optional(2, "c2", Types.StringType.get()),
          optional(3, "c3", Types.StringType.get()));

  protected static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("c1").build();

  private String baseDir;

  // Create 10k manifests
  private static final int MANIFEST_COUNTER = 10000;

  // each manifest with 100 partition values
  private static final int PARTITION_PER_MANIFEST = 100;

  // 20 data files per partition, which results in 2k data files per manifest
  private static final int DATA_FILES_PER_PARTITION_COUNT = 20;

  private Table table;

  @Setup
  public void setupBenchmark() {
    baseDir =
        Paths.get(new File(System.getProperty("java.io.tmpdir")).getAbsolutePath()).toString();
    table = TestTables.create(new File(baseDir), "foo", SCHEMA, SPEC, SortOrder.unsorted(), 2);

    IntStream.range(0, MANIFEST_COUNTER)
        .forEach(
            manifestCount -> {
              AppendFiles appendFiles = table.newAppend();

              IntStream.range(0, PARTITION_PER_MANIFEST)
                  .forEach(
                      partitionOrdinal -> {
                        StructLike partition = TestHelpers.Row.of(partitionOrdinal);
                        IntStream.range(0, DATA_FILES_PER_PARTITION_COUNT)
                            .forEach(
                                fileOrdinal ->
                                    appendFiles.appendFile(
                                        FileGenerationUtil.generateDataFile(table, partition)));
                      });

              appendFiles.commit();
            });
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    if (baseDir != null) {
      try (Stream<Path> walk = java.nio.file.Files.walk(Paths.get(baseDir))) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
      baseDir = null;
    }
  }

  @Benchmark
  @Threads(1)
  public void benchmarkPartitionStats() throws IOException {
    Snapshot currentSnapshot = table.currentSnapshot();

    PartitionStatisticsFile result = PartitionStatsHandler.computeAndWritePartitionStatsFile(table);
    table.updatePartitionStatistics().setPartitionStatistics(result).commit();
    assertThat(result.snapshotId()).isEqualTo(currentSnapshot.snapshotId());

    // validate row count
    try (CloseableIterable<PartitionStatsRecord> recordIterator =
        PartitionStatsHandler.readPartitionStatsFile(
            PartitionStatsHandler.schema(Partitioning.partitionType(table)),
            Files.localInput(result.path()))) {
      assertThat(recordIterator).hasSize(PARTITION_PER_MANIFEST);
    }
  }
}
