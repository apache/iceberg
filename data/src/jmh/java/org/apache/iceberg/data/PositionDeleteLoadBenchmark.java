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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Loads one position delete file the way an engine with an executor cache does on a miss: the whole
 * file is read and indexed per data file ({@code BaseDeleteLoader.readPosDeletes}). The file holds
 * 64 data files x 30,000 positions, sorted by path then position as the spec requires, with
 * object-store-length paths. Time per load.
 *
 * <p>To run: {@code ./gradlew :iceberg-data:jmh -PjmhIncludeRegex=PositionDeleteLoadBenchmark
 * -PjmhOutputPath=benchmark/position-delete-load.txt}
 */
@Fork(3)
@State(Scope.Benchmark)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 10, time = 2)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class PositionDeleteLoadBenchmark {

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));
  private static final int DATA_FILES = 64;
  private static final int DELETES_PER_FILE = 30_000;

  private final HadoopTables tables = new HadoopTables();

  private File tableDir;
  private DeleteFile deleteFile;
  private String firstPath;
  private BaseDeleteLoader loader;

  @Setup
  public void setupBenchmark() throws IOException {
    this.tableDir = java.nio.file.Files.createTempDirectory("pos-delete-load-bench").toFile();
    Table table =
        tables.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
            tableDir.toURI().toString());

    List<Pair<CharSequence, Long>> deletes = Lists.newArrayList();
    for (int f = 0; f < DATA_FILES; f += 1) {
      String path =
          String.format(
              Locale.ROOT,
              "s3://warehouse-bucket/warehouse/db.db/store_sales/data/ss_sold_date_sk=%05d/"
                  + "00%03d-1234-5678-9abc-def012345678-0-00001.parquet",
              f,
              f);
      for (long pos = 0; pos < DELETES_PER_FILE; pos += 1) {
        deletes.add(Pair.of(path, pos * 5));
      }
    }

    this.firstPath = deletes.get(0).first().toString();
    this.deleteFile =
        FileHelpers.writeDeleteFile(
                table, Files.localOutput(new File(tableDir, "pos-deletes.parquet")), deletes)
            .first();

    // caching allowed but never kept: every load reads the whole file, as an executor-cache miss
    this.loader =
        new BaseDeleteLoader(
            file -> Files.localInput(file.location()), MoreExecutors.newDirectExecutorService()) {
          @Override
          protected boolean canCache(long size) {
            return true;
          }

          @Override
          protected <V> V getOrLoad(String key, Supplier<V> valueSupplier, long valueSize) {
            return valueSupplier.get();
          }
        };
  }

  @TearDown
  public void tearDownBenchmark() {
    tables.dropTable(tableDir.toURI().toString());
  }

  @Benchmark
  public PositionDeleteIndex loadWholeFile() {
    return loader.loadPositionDeletes(ImmutableList.of(deleteFile), firstPath);
  }
}
