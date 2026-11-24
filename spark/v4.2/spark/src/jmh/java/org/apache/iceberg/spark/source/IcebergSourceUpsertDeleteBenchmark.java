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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.iceberg.util.SerializableFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.unsafe.hash.Murmur3_x86_32;
import org.apache.spark.unsafe.types.UTF8String;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * A benchmark that evaluates reads of a table after many streaming upsert commits, with the deletes
 * stored either as equality delete files or as deletion vectors (DVs).
 *
 * <p>Each commit models one snapshot in upsert mode: it adds a data file with new versions of
 * random existing keys and, for equality deletes, an equality delete file for these keys. An
 * equality delete file applies to all older data files in its partition, so the number of delete
 * files per data file grows with every commit. The partitioned table is bucketed by key and gets
 * one file of each kind per bucket and commit.
 *
 * <p>The DV variant contains the same data files with the same rows deleted, expressed as one DV
 * per data file. This is the table state after converting the equality deletes to DVs.
 *
 * <p>All scans run with the executor cache of parsed delete files warm, so the cost of equality
 * deletes is a lower bound for a cluster.
 *
 * <p>To run this benchmark for spark-4.2: <code>
 *   ./gradlew -DsparkVersions=4.2 :iceberg-spark:iceberg-spark-4.2_2.13:jmh
 *       -PjmhIncludeRegex=IcebergSourceUpsertDeleteBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-source-upsert-delete-benchmark-result.txt
 * </code>
 */
@Fork(value = 3, jvmArgsAppend = "-Xmx12g")
@State(Scope.Benchmark)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@Timeout(time = 30, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.SingleShotTime)
public class IcebergSourceUpsertDeleteBenchmark {

  private static final Logger LOG =
      LoggerFactory.getLogger(IcebergSourceUpsertDeleteBenchmark.class);
  private static final int NUM_KEYS = 10_000_000;
  private static final int NUM_INITIAL_FILES = 10;
  private static final int NUM_BUCKETS = 5;
  private static final int UPSERTS_PER_COMMIT = 500;
  private static final long RANDOM_SEED = 42L;
  private static final int INITIAL_VERSION = 0;
  // the seed used by Spark's hash function
  private static final int SPARK_HASH_SEED = 42;
  private static final int FIRST_DAY = 20_000;
  private static final long MICROS_PER_DAY = TimeUnit.DAYS.toMicros(1);
  private static final String CATALOG_NAME = "local";
  private static final TableIdentifier TABLE_IDENT = TableIdentifier.of("default", "upserts");
  private static final String TABLE_NAME = CATALOG_NAME + "." + TABLE_IDENT;
  // commits on the default worker pools wait for their tasks in 10 ms polling steps, which adds up
  // over thousands of commits
  private static final ExecutorService COMMIT_EXECUTOR = MoreExecutors.newDirectExecutorService();
  private static final int COMMIT_PARALLELISM = 1;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          required(2, "version", Types.IntegerType.get()),
          required(3, "floatCol", Types.FloatType.get()),
          optional(4, "doubleCol", Types.DoubleType.get()),
          optional(5, "dateCol", Types.DateType.get()),
          optional(6, "timestampCol", Types.TimestampType.withZone()),
          optional(7, "stringCol", Types.StringType.get()));

  @Param({"100", "200", "500", "1000"})
  private int numCommits;

  @Param({"EQUALITY", "DV"})
  private DeleteType deleteType;

  @Param({"false", "true"})
  private boolean partitioned;

  private final Configuration hadoopConf = new Configuration();
  private final List<TrackedDataFile> dataFiles = Lists.newArrayList();
  // the data file index and position of the live row of each key
  private final int[] liveFileIndexes = new int[NUM_KEYS];
  private final int[] livePositions = new int[NUM_KEYS];
  private String warehouse;
  private SparkSession spark;
  private Table table;
  private SparkFileWriterFactory writerFactory;
  private OutputFileFactory fileFactory;
  private SerializableFunction<Long, Integer> bucketFunc;
  private int[] eqDeleteFileCounts;

  public enum DeleteType {
    EQUALITY,
    DV
  }

  @Setup
  public void setupBenchmark() throws IOException {
    setupSpark();
    initTable();
    writeInitialFiles();
    writeUpsertCommits();
    verifyDeleteFiles();
    verifyLiveRows();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    spark.stop();
    Path warehousePath = new Path(warehouse);
    warehousePath.getFileSystem(hadoopConf).delete(warehousePath, true /* recursive */);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  @Warmup(iterations = 5, time = 1)
  @Measurement(iterations = 5, time = 1)
  @Threads(1)
  public void planFiles(Blackhole blackhole) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        blackhole.consume(task);
      }
    }
  }

  @Benchmark
  @Threads(1)
  public void readIceberg(Blackhole blackhole) {
    Dataset<Row> df = spark.table(TABLE_NAME);
    blackhole.consume(df.queryExecution().toRdd().toJavaRDD().count());
  }

  private void setupSpark() {
    this.warehouse = hadoopConf.get("hadoop.tmp.dir") + "/iceberg-warehouse-" + UUID.randomUUID();
    String catalog = "spark.sql.catalog." + CATALOG_NAME;
    this.spark =
        SparkSession.builder()
            .config(TestBase.DISABLE_UI)
            .config(catalog, SparkCatalog.class.getName())
            .config(catalog + ".type", "hadoop")
            .config(catalog + ".warehouse", warehouse)
            // Hadoop's local file system forks a chmod process for each new file without native
            // libraries, which dominates the time to create tables with thousands of commits
            .config(
                catalog + "." + CatalogProperties.FILE_IO_IMPL,
                TestTables.LocalFileIO.class.getName())
            // every scan loads the table metadata, like a new query
            .config(catalog + "." + CatalogProperties.CACHE_ENABLED, "false")
            .master("local[*]")
            .getOrCreate();
  }

  private void initTable() {
    PartitionSpec spec =
        partitioned
            ? PartitionSpec.builderFor(SCHEMA).bucket("id", NUM_BUCKETS).build()
            : PartitionSpec.unpartitioned();
    Map<String, String> properties = ImmutableMap.of(TableProperties.FORMAT_VERSION, "3");
    Catalog catalog = Spark3Util.loadIcebergCatalog(spark, CATALOG_NAME);
    this.table = catalog.createTable(TABLE_IDENT, SCHEMA, spec, properties);
    this.writerFactory =
        SparkFileWriterFactory.builderFor(table)
            .dataFileFormat(FileFormat.PARQUET)
            .dataSchema(table.schema())
            .equalityFieldIds(new int[] {SCHEMA.findField("id").fieldId()})
            .equalityDeleteRowSchema(SCHEMA.select("id"))
            .build();
    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PARQUET).build();
    Transform<Long, Integer> bucket = Transforms.bucket(NUM_BUCKETS);
    this.bucketFunc = bucket.bind(Types.LongType.get());
    this.eqDeleteFileCounts = new int[numBuckets()];
  }

  private void writeInitialFiles() throws IOException {
    int filesPerBucket = NUM_INITIAL_FILES / numBuckets();
    int keyRangePerFile = NUM_KEYS / filesPerBucket;
    AppendFiles append = table.newAppend();
    for (int fileIndex = 0; fileIndex < NUM_INITIAL_FILES; fileIndex++) {
      int bucket = fileIndex / filesPerBucket;
      int firstKey = (fileIndex % filesPerBucket) * keyRangePerFile;
      int[] keys = keysInBucket(IntStream.range(firstKey, firstKey + keyRangePerFile), bucket);
      DataFile dataFile = writeDataFile(bucket, keys, INITIAL_VERSION);
      trackDataFile(dataFile, bucket, INITIAL_VERSION, keys);
      append.appendFile(dataFile);
    }

    append.commit();
  }

  private void writeUpsertCommits() throws IOException {
    Random random = new Random(RANDOM_SEED);
    for (int commit = 1; commit <= numCommits; commit++) {
      int[] upsertedKeys = random.ints(0, NUM_KEYS).distinct().limit(UPSERTS_PER_COMMIT).toArray();
      RowDelta rowDelta =
          table
              .newRowDelta()
              .scanManifestsWith(COMMIT_EXECUTOR)
              .writeManifestsWith(COMMIT_EXECUTOR, COMMIT_PARALLELISM);
      for (int bucket = 0; bucket < numBuckets(); bucket++) {
        int[] keys = keysInBucket(Arrays.stream(upsertedKeys), bucket);
        if (deleteType == DeleteType.EQUALITY) {
          rowDelta.addDeletes(writeEqualityDeleteFile(bucket, keys));
          eqDeleteFileCounts[bucket] += 1;
        }

        DataFile dataFile = writeDataFile(bucket, keys, commit);
        trackDataFile(dataFile, bucket, commit, keys);
        rowDelta.addRows(dataFile);
      }

      rowDelta.commit();
    }

    if (deleteType == DeleteType.DV) {
      commitDVs();
    }
  }

  private int[] keysInBucket(IntStream keys, int bucket) {
    return keys.filter(key -> bucket(key) == bucket).toArray();
  }

  /** Makes the rows of the data file the live rows of their keys and deletes the replaced rows. */
  private void trackDataFile(DataFile dataFile, int bucket, int version, int[] keys) {
    int fileIndex = dataFiles.size();
    dataFiles.add(new TrackedDataFile(dataFile, bucket, version, eqDeleteFileCounts[bucket]));
    for (int pos = 0; pos < keys.length; pos++) {
      int key = keys[pos];
      if (version > INITIAL_VERSION) {
        dataFiles.get(liveFileIndexes[key]).deletedPositions.set(livePositions[key]);
      }

      liveFileIndexes[key] = fileIndex;
      livePositions[key] = pos;
    }
  }

  private DataFile writeDataFile(int bucket, int[] keys, int version) throws IOException {
    StructLike partition = partition(bucket);
    DataWriter<InternalRow> writer =
        writerFactory.newDataWriter(newOutputFile(partition), table.spec(), partition);
    GenericInternalRow row = new GenericInternalRow(SCHEMA.columns().size());
    int day = FIRST_DAY + version;
    try (DataWriter<InternalRow> closeableWriter = writer) {
      for (int key : keys) {
        row.setLong(0, key);
        row.setInt(1, version);
        row.setFloat(2, (float) key);
        row.setDouble(3, key + version);
        row.setInt(4, day);
        row.setLong(5, day * MICROS_PER_DAY + key);
        row.update(6, UTF8String.fromString("key-" + key + "-version-" + version));
        closeableWriter.write(row);
      }
    }

    return writer.toDataFile();
  }

  private DeleteFile writeEqualityDeleteFile(int bucket, int[] keys) throws IOException {
    StructLike partition = partition(bucket);
    EqualityDeleteWriter<InternalRow> writer =
        writerFactory.newEqualityDeleteWriter(newOutputFile(partition), table.spec(), partition);
    GenericInternalRow row = new GenericInternalRow(1);
    try (EqualityDeleteWriter<InternalRow> closeableWriter = writer) {
      for (int key : keys) {
        row.setLong(0, key);
        closeableWriter.write(row);
      }
    }

    return writer.toDeleteFile();
  }

  private void commitDVs() throws IOException {
    OutputFileFactory dvFileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    DVFileWriter writer = new BaseDVFileWriter(dvFileFactory, path -> null);
    try (DVFileWriter closeableWriter = writer) {
      for (TrackedDataFile dataFile : dataFiles) {
        BitSet positions = dataFile.deletedPositions;
        for (int pos = positions.nextSetBit(0); pos >= 0; pos = positions.nextSetBit(pos + 1)) {
          closeableWriter.delete(
              dataFile.file.location(), pos, table.spec(), dataFile.file.partition());
        }
      }
    }

    RowDelta rowDelta = table.newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();
  }

  private void verifyDeleteFiles() throws IOException {
    Map<String, TrackedDataFile> dataFilesByLocation = Maps.newHashMap();
    dataFiles.forEach(dataFile -> dataFilesByLocation.put(dataFile.file.location(), dataFile));

    int numPlannedFiles = 0;
    long numAttachedDeleteFiles = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        TrackedDataFile dataFile = dataFilesByLocation.get(task.file().location());
        Preconditions.checkState(dataFile != null, "Unknown data file: %s", task.file());
        verifyAttachedDeleteFiles(dataFile, task.deletes());
        numPlannedFiles += 1;
        numAttachedDeleteFiles += task.deletes().size();
      }
    }

    Preconditions.checkState(
        numPlannedFiles == dataFiles.size(),
        "Expected %s data files, planned %s",
        dataFiles.size(),
        numPlannedFiles);

    long numDeletedRows =
        dataFiles.stream().mapToLong(dataFile -> dataFile.deletedPositions.cardinality()).sum();
    LOG.info(
        "Planned {} data files with {} attached delete files and {} deleted rows",
        numPlannedFiles,
        numAttachedDeleteFiles,
        numDeletedRows);
  }

  private void verifyAttachedDeleteFiles(TrackedDataFile dataFile, List<DeleteFile> deleteFiles) {
    switch (deleteType) {
      case EQUALITY -> {
        int expectedCount =
            eqDeleteFileCounts[dataFile.bucket] - dataFile.committedEqDeleteFileCount;
        Preconditions.checkState(
            deleteFiles.size() == expectedCount
                && deleteFiles.stream().allMatch(f -> f.content() == FileContent.EQUALITY_DELETES),
            "Expected %s equality delete files for %s: %s",
            expectedCount,
            dataFile.file.location(),
            deleteFiles);
      }
      case DV -> {
        int expectedCount = dataFile.deletedPositions.isEmpty() ? 0 : 1;
        Preconditions.checkState(
            deleteFiles.size() == expectedCount
                && deleteFiles.stream()
                    .allMatch(
                        f ->
                            ContentFileUtil.isDV(f)
                                && f.recordCount() == dataFile.deletedPositions.cardinality()),
            "Expected %s DV with %s positions for %s: %s",
            expectedCount,
            dataFile.deletedPositions.cardinality(),
            dataFile.file.location(),
            deleteFiles);
      }
    }
  }

  private void verifyLiveRows() {
    long expectedChecksum = 0L;
    for (int key = 0; key < NUM_KEYS; key++) {
      int version = dataFiles.get(liveFileIndexes[key]).version;
      int hash = Murmur3_x86_32.hashLong(key, SPARK_HASH_SEED);
      expectedChecksum += Murmur3_x86_32.hashInt(version, hash);
    }

    Dataset<Row> df = spark.table(TABLE_NAME);
    List<SparkPlan> scans =
        JavaConverters.seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves())
            .asJava();
    Preconditions.checkState(
        scans.stream().allMatch(SparkPlan::supportsColumnar),
        "Expected vectorized reads: %s",
        scans);

    Row result = df.selectExpr("count(*)", "sum(hash(id, version))").first();
    Preconditions.checkState(
        result.getLong(0) == NUM_KEYS, "Expected %s rows, got %s", NUM_KEYS, result.getLong(0));
    Preconditions.checkState(
        result.getLong(1) == expectedChecksum,
        "Expected checksum %s, got %s",
        expectedChecksum,
        result.getLong(1));
  }

  private int numBuckets() {
    return partitioned ? NUM_BUCKETS : 1;
  }

  private int bucket(int key) {
    return partitioned ? bucketFunc.apply((long) key) : 0;
  }

  private StructLike partition(int bucket) {
    if (!partitioned) {
      return null;
    }

    GenericRecord partition = GenericRecord.create(table.spec().partitionType());
    partition.set(0, bucket);
    return partition;
  }

  private EncryptedOutputFile newOutputFile(StructLike partition) {
    return partition == null
        ? fileFactory.newOutputFile()
        : fileFactory.newOutputFile(table.spec(), partition);
  }

  private static class TrackedDataFile {
    private final DataFile file;
    private final int bucket;
    private final int version;
    // equality delete files in the bucket committed up to the data file's commit, which don't apply
    private final int committedEqDeleteFileCount;
    private final BitSet deletedPositions = new BitSet();

    private TrackedDataFile(
        DataFile file, int bucket, int version, int committedEqDeleteFileCount) {
      this.file = file;
      this.bucket = bucket;
      this.version = version;
      this.committedEqDeleteFileCount = committedEqDeleteFileCount;
    }
  }
}
