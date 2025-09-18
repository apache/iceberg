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

import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of rewriting data files in the table.
 *
 * <p>To run this benchmark: <code>
 *   ./gradlew :iceberg-core:jmh
 *       -PjmhIncludeRegex=RewriteDataFilesBenchmark
 *       -PjmhOutputPath=benchmark/rewrite-data-files-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
@Timeout(time = 10, timeUnit = TimeUnit.MINUTES)
public class RewriteDataFilesBenchmark {

  private static final String TABLE_IDENT = "tblX";
  private static final Schema SCHEMA =
      new Schema(
          required(1, "int_col", Types.IntegerType.get()),
          required(2, "long_col", Types.LongType.get()),
          required(3, "decimal_col", Types.DecimalType.of(10, 10)),
          required(4, "date_col", Types.DateType.get()),
          required(5, "timestamp_col", Types.TimestampType.withoutZone()),
          required(6, "timestamp_tz_col", Types.TimestampType.withZone()),
          required(7, "str_col", Types.StringType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.unpartitioned();
  private static final HadoopTables TABLES = new HadoopTables();

  private Table table;
  private DataFileSet dataFilesToRemove;
  private DataFileSet dataFilesToAdd;

  @Param({"50000", "100000", "500000", "1000000", "2000000"})
  private int numFiles;

  @Param({"5", "25", "50", "100"})
  private int percentDataFilesRewritten;

  @Setup
  public void setupBenchmark() throws IOException {
    initTable();
    initFiles();
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
  }

  @Benchmark
  @Threads(1)
  public void rewriteDataFiles() {
    Snapshot currentSnapshot = table.currentSnapshot();
    RewriteFiles rewriteFiles = table.newRewrite();
    rewriteFiles.validateFromSnapshot(currentSnapshot.snapshotId());
    dataFilesToAdd.forEach(rewriteFiles::addFile);
    dataFilesToRemove.forEach(rewriteFiles::deleteFile);
    rewriteFiles.commit();
    table.manageSnapshots().rollbackTo(currentSnapshot.snapshotId()).commit();
  }

  private void initTable() {
    if (TABLES.exists(TABLE_IDENT)) {
      TABLES.dropTable(TABLE_IDENT);
    }

    this.table =
        TABLES.create(
            SCHEMA, SPEC, ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"), TABLE_IDENT);
  }

  private void dropTable() {
    TABLES.dropTable(TABLE_IDENT);
  }

  private void initFiles() throws IOException {
    List<DataFile> pendingDataFiles = Lists.newArrayListWithExpectedSize(numFiles);
    int numDataFilesToRewrite = (int) Math.ceil(numFiles * (percentDataFilesRewritten / 100.0));
    Map<String, DataFile> filesToReplace = Maps.newHashMapWithExpectedSize(numDataFilesToRewrite);
    RowDelta rowDelta = table.newRowDelta();
    for (int ordinal = 0; ordinal < numFiles; ordinal++) {
      DataFile dataFile = generateDataFile();
      rowDelta.addRows(dataFile);
      DeleteFile deleteFile = FileGenerationUtil.generateDV(table, dataFile);
      rowDelta.addDeletes(deleteFile);
      if (numDataFilesToRewrite > 0) {
        filesToReplace.put(dataFile.location(), dataFile);
        DataFile pendingDataFile = generateDataFile(dataFile.recordCount());
        rowDelta.addRows(pendingDataFile);
        pendingDataFiles.add(pendingDataFile);
        numDataFilesToRewrite--;
      }
    }

    rowDelta.commit();

    List<DataFile> dataFilesReadFromManifests = Lists.newArrayList();
    for (ManifestFile dataManifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> manifestReader = ManifestFiles.read(dataManifest, table.io())) {
        manifestReader
            .iterator()
            .forEachRemaining(
                file -> {
                  if (filesToReplace.containsKey(file.location())) {
                    dataFilesReadFromManifests.add(file);
                  }
                });
      }
    }

    this.dataFilesToRemove = DataFileSet.of(dataFilesReadFromManifests);
    this.dataFilesToAdd = DataFileSet.of(pendingDataFiles);
  }

  private DataFile generateDataFile() {
    return generateDataFile(-1L);
  }

  private DataFile generateDataFile(long recordCount) {
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();
    String path = locations.newDataLocation(spec, null, FileGenerationUtil.generateFileName());
    long fileSize = ThreadLocalRandom.current().nextLong(50_000L);
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    Metrics metrics =
        FileGenerationUtil.generateRandomMetrics(
            schema, metricsConfig, ImmutableMap.of(), ImmutableMap.of());
    if (recordCount > 0) {
      metrics =
          new Metrics(
              recordCount,
              metrics.columnSizes(),
              metrics.valueCounts(),
              metrics.nullValueCounts(),
              metrics.nanValueCounts());
    }

    return DataFiles.builder(spec)
        .withPath(path)
        .withFileSizeInBytes(fileSize)
        .withFormat(FileFormat.PARQUET)
        .withMetrics(metrics)
        .build();
  }
}
