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

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.DeleteLoader;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.DeleteGranularity;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FanoutPositionOnlyDeleteWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.util.ContentFileUtil;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.unsafe.types.UTF8String;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that compares the performance of DV and position delete readers.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-3.5_2.12:jmh
 *       -PjmhIncludeRegex=DVReaderBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-dv-reader-benchmark-result.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 15)
@Timeout(time = 20, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.SingleShotTime)
public class DVReaderBenchmark {

  private static final String TABLE_NAME = "test_table";
  private static final int DATA_FILE_RECORD_COUNT = 2_000_000;
  private static final long TARGET_FILE_SIZE = Long.MAX_VALUE;

  @Param({"5", "10"})
  private int referencedDataFileCount;

  @Param({"0.01", "0.03", "0.05", "0.10", "0.2"})
  private double deletedRowsRatio;

  private final Configuration hadoopConf = new Configuration();
  private final Random random = ThreadLocalRandom.current();
  private SparkSession spark;
  private Table table;
  private DeleteWriteResult dvsResult;
  private DeleteWriteResult fileDeletesResult;
  private DeleteWriteResult partitionDeletesResult;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException, IOException {
    setupSpark();
    initTable();
    List<InternalRow> deletes = generatePositionDeletes();
    this.dvsResult = writeDVs(deletes);
    this.fileDeletesResult = writePositionDeletes(deletes, DeleteGranularity.FILE);
    this.partitionDeletesResult = writePositionDeletes(deletes, DeleteGranularity.PARTITION);
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
    tearDownSpark();
  }

  @Benchmark
  @Threads(1)
  public void dv(Blackhole blackhole) {
    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file), null);
    DeleteFile dv = dvsResult.deleteFiles().get(0);
    CharSequence dataFile = dv.referencedDataFile();
    PositionDeleteIndex index = loader.loadPositionDeletes(ImmutableList.of(dv), dataFile);
    blackhole.consume(index);
  }

  @Benchmark
  @Threads(1)
  public void fileScopedParquetDeletes(Blackhole blackhole) {
    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file), null);
    DeleteFile deleteFile = fileDeletesResult.deleteFiles().get(0);
    CharSequence dataFile = ContentFileUtil.referencedDataFile(deleteFile);
    PositionDeleteIndex index = loader.loadPositionDeletes(ImmutableList.of(deleteFile), dataFile);
    blackhole.consume(index);
  }

  @Benchmark
  @Threads(1)
  public void partitionScopedParquetDeletes(Blackhole blackhole) {
    DeleteLoader loader = new BaseDeleteLoader(file -> table.io().newInputFile(file), null);
    DeleteFile deleteFile = Iterables.getOnlyElement(partitionDeletesResult.deleteFiles());
    CharSequence dataFile = Iterables.getLast(partitionDeletesResult.referencedDataFiles());
    PositionDeleteIndex index = loader.loadPositionDeletes(ImmutableList.of(deleteFile), dataFile);
    blackhole.consume(index);
  }

  private FanoutPositionOnlyDeleteWriter<InternalRow> newWriter(DeleteGranularity granularity) {
    return new FanoutPositionOnlyDeleteWriter<>(
        newWriterFactory(),
        newFileFactory(FileFormat.PARQUET),
        table.io(),
        TARGET_FILE_SIZE,
        granularity);
  }

  private SparkFileWriterFactory newWriterFactory() {
    return SparkFileWriterFactory.builderFor(table).dataFileFormat(FileFormat.PARQUET).build();
  }

  private OutputFileFactory newFileFactory(FileFormat format) {
    return OutputFileFactory.builderFor(table, 1, 1).format(format).build();
  }

  private List<InternalRow> generatePositionDeletes() {
    int numDeletesPerFile = (int) (DATA_FILE_RECORD_COUNT * deletedRowsRatio);
    int numDeletes = referencedDataFileCount * numDeletesPerFile;
    List<InternalRow> deletes = Lists.newArrayListWithExpectedSize(numDeletes);

    for (int pathIndex = 0; pathIndex < referencedDataFileCount; pathIndex++) {
      UTF8String dataFilePath = UTF8String.fromString(generateDataFilePath());
      Set<Long> positions = generatePositions(numDeletesPerFile);
      for (long pos : positions) {
        deletes.add(new GenericInternalRow(new Object[] {dataFilePath, pos}));
      }
    }

    Collections.shuffle(deletes);

    return deletes;
  }

  private DeleteWriteResult writeDVs(Iterable<InternalRow> rows) throws IOException {
    OutputFileFactory fileFactory = newFileFactory(FileFormat.PUFFIN);
    DVFileWriter writer = new BaseDVFileWriter(fileFactory, path -> null);
    try (DVFileWriter closableWriter = writer) {
      for (InternalRow row : rows) {
        String path = row.getString(0);
        long pos = row.getLong(1);
        closableWriter.delete(path, pos, table.spec(), null);
      }
    }
    return writer.result();
  }

  private DeleteWriteResult writePositionDeletes(
      Iterable<InternalRow> rows, DeleteGranularity granularity) throws IOException {
    FanoutPositionOnlyDeleteWriter<InternalRow> writer = newWriter(granularity);
    try (FanoutPositionOnlyDeleteWriter<InternalRow> closableWriter = writer) {
      PositionDelete<InternalRow> positionDelete = PositionDelete.create();
      for (InternalRow row : rows) {
        String path = row.getString(0);
        long pos = row.getLong(1);
        positionDelete.set(path, pos, null /* no row */);
        closableWriter.write(positionDelete, table.spec(), null);
      }
    }
    return writer.result();
  }

  public Set<Long> generatePositions(int numPositions) {
    Set<Long> positions = Sets.newHashSet();

    while (positions.size() < numPositions) {
      long pos = random.nextInt(DATA_FILE_RECORD_COUNT);
      positions.add(pos);
    }

    return positions;
  }

  private String generateDataFilePath() {
    String fileName = FileGenerationUtil.generateFileName();
    return table.locationProvider().newDataLocation(table.spec(), null, fileName);
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", newWarehouseDir())
            .master("local[*]")
            .getOrCreate();
  }

  private void tearDownSpark() {
    spark.stop();
  }

  private void initTable() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (c1 INT, c2 INT, c3 STRING) USING iceberg", TABLE_NAME);
    this.table = Spark3Util.loadIcebergTable(spark, TABLE_NAME);
  }

  private void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", TABLE_NAME);
  }

  private String newWarehouseDir() {
    return hadoopConf.get("hadoop.tmp.dir") + UUID.randomUUID();
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
