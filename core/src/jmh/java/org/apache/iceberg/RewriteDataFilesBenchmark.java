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
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
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
  private static final HadoopTables TABLES = new HadoopTables();

  private Table table;
  private DataFileSet dataFilesToRemove;
  private DataFileSet dataFilesToAdd;

  @Param({"50000", "100000", "500000"})
  private int numFiles;

  @Param({"1", "5", "50", "100"})
  private int percentDataFilesRewritten;

  /** Number of partitions to spread the data files over, or 0 for an unpartitioned table. */
  @Param({"0", "1000"})
  private int numPartitions;

  /**
   * Latency charged to every metadata file read and write, or 0 for no latency. Helps simulate
   * object storage like latency even though using local disk.
   */
  @Param({"0", "50"})
  private int metadataFileLatencyMs;

  /**
   * Target manifest size, which controls how many manifests the files are spread over. Helps
   * simulate larger tables with many manifests without generating extremely large table metadata.
   */
  @Param({"65536", "8388608"})
  private long manifestTargetSizeBytes;

  @Setup
  public void setupBenchmark() throws IOException {
    initTable();
    initFiles();

    this.table =
        new BaseTable(
            new LatencyInjectingTableOperations(
                ((HasTableOperations) table).operations(), metadataFileLatencyMs),
            TABLE_IDENT);
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
            SCHEMA,
            spec(),
            ImmutableMap.of(
                TableProperties.FORMAT_VERSION,
                "3",
                TableProperties.MANIFEST_TARGET_SIZE_BYTES,
                String.valueOf(manifestTargetSizeBytes)),
            TABLE_IDENT);
  }

  private PartitionSpec spec() {
    return numPartitions > 0
        ? PartitionSpec.builderFor(SCHEMA).identity("int_col").build()
        : PartitionSpec.unpartitioned();
  }

  private int partitionValue(int ordinal) {
    // Assign files to partitions in contiguous blocks to pack them by partition value.
    // This allows partition pruning optimization, as manifests are clustered by partition value.
    int filesPerPartition = (numFiles + numPartitions - 1) / numPartitions;
    return ordinal / filesPerPartition;
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
      DataFile dataFile = generateDataFile(ordinal);
      rowDelta.addRows(dataFile);
      DeleteFile deleteFile = FileGenerationUtil.generateDV(table, dataFile);
      rowDelta.addDeletes(deleteFile);
      if (numDataFilesToRewrite > 0) {
        filesToReplace.put(dataFile.location(), dataFile);
        // the replacement lands in the same partition as the file it replaces
        DataFile pendingDataFile = generateDataFile(ordinal, dataFile.recordCount());
        rowDelta.addRows(pendingDataFile);
        pendingDataFiles.add(pendingDataFile);
        numDataFilesToRewrite--;
      }
    }

    rowDelta.commit();

    List<DataFile> dataFilesReadFromManifests = Lists.newArrayList();
    for (ManifestFile dataManifest : table.currentSnapshot().dataManifests(table.io())) {
      try (ManifestReader<DataFile> manifestReader =
          ManifestFiles.read(dataManifest, table.io(), table.specs())) {
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

  /** Delegating operations whose FileIO charges a fixed latency for each open. */
  private static class LatencyInjectingTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final int latencyMs;

    LatencyInjectingTableOperations(TableOperations delegate, int latencyMs) {
      this.delegate = delegate;
      this.latencyMs = latencyMs;
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      delegate.commit(base, metadata);
    }

    @Override
    public FileIO io() {
      return new LatencyInjectingFileIO(delegate.io(), latencyMs);
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      return new LatencyInjectingTableOperations(delegate.temp(uncommittedMetadata), latencyMs);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }

  private static class LatencyInjectingFileIO implements FileIO {
    private final FileIO delegate;
    private final int latencyMs;

    LatencyInjectingFileIO(FileIO delegate, int latencyMs) {
      this.delegate = delegate;
      this.latencyMs = latencyMs;
    }

    @Override
    public InputFile newInputFile(String path) {
      return new LatencyInjectingInputFile(delegate.newInputFile(path), latencyMs);
    }

    @Override
    public InputFile newInputFile(String path, long length) {
      return new LatencyInjectingInputFile(delegate.newInputFile(path, length), latencyMs);
    }

    @Override
    public OutputFile newOutputFile(String path) {
      return new LatencyInjectingOutputFile(delegate.newOutputFile(path), latencyMs);
    }

    @Override
    public void deleteFile(String path) {
      delegate.deleteFile(path);
    }
  }

  /**
   * An object store pays for a write when the object is completed rather than when the handle is
   * created, so the latency is charged on close.
   */
  private static class LatencyInjectingOutputFile implements OutputFile {
    private final OutputFile delegate;
    private final int latencyMs;

    LatencyInjectingOutputFile(OutputFile delegate, int latencyMs) {
      this.delegate = delegate;
      this.latencyMs = latencyMs;
    }

    @Override
    public PositionOutputStream create() {
      return new LatencyInjectingPositionOutputStream(delegate.create(), latencyMs);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new LatencyInjectingPositionOutputStream(delegate.createOrOverwrite(), latencyMs);
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public InputFile toInputFile() {
      return new LatencyInjectingInputFile(delegate.toInputFile(), latencyMs);
    }
  }

  private static class LatencyInjectingPositionOutputStream extends PositionOutputStream {
    private final PositionOutputStream delegate;
    private final int latencyMs;

    LatencyInjectingPositionOutputStream(PositionOutputStream delegate, int latencyMs) {
      this.delegate = delegate;
      this.latencyMs = latencyMs;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      delegate.write(b);
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
      delegate.write(buffer, offset, length);
    }

    @Override
    public void flush() throws IOException {
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      sleep(latencyMs);
      delegate.close();
    }
  }

  private static class LatencyInjectingInputFile implements InputFile {
    private final InputFile delegate;
    private final int latencyMs;

    LatencyInjectingInputFile(InputFile delegate, int latencyMs) {
      this.delegate = delegate;
      this.latencyMs = latencyMs;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      sleep(latencyMs);
      return delegate.newStream();
    }

    @Override
    public String location() {
      return delegate.location();
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }

  private static void sleep(int latencyMs) {
    if (latencyMs <= 0) {
      return;
    }

    try {
      Thread.sleep(latencyMs);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while simulating latency", e);
    }
  }

  private DataFile generateDataFile(int ordinal) {
    return generateDataFile(ordinal, -1L);
  }

  private DataFile generateDataFile(int ordinal, long recordCount) {
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();
    String fileName = FileGenerationUtil.generateFileName();
    String path =
        numPartitions > 0
            ? locations.newDataLocation(fileName)
            : locations.newDataLocation(spec, null, fileName);
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

    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withPath(path)
            .withFileSizeInBytes(fileSize)
            .withFormat(FileFormat.PARQUET)
            .withMetrics(metrics);

    if (numPartitions > 0) {
      builder.withPartitionPath("int_col=" + partitionValue(ordinal));
    }

    return builder.build();
  }
}
