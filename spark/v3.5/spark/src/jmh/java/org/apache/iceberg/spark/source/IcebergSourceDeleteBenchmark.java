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

import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.SPLIT_OPEN_FILE_COST;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.ClusteredEqualityDeleteWriter;
import org.apache.iceberg.io.ClusteredPositionDeleteWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.infra.Blackhole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class IcebergSourceDeleteBenchmark extends IcebergSourceBenchmark {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergSourceDeleteBenchmark.class);
  private static final long TARGET_FILE_SIZE_IN_BYTES = 512L * 1024 * 1024;

  protected static final int NUM_FILES = 1;
  protected static final int NUM_ROWS = 10 * 1000 * 1000;

  @Setup
  public void setupBenchmark() throws IOException {
    setupSpark();
    appendData();
  }

  @TearDown
  public void tearDownBenchmark() throws IOException {
    tearDownSpark();
    cleanupFiles();
  }

  @Benchmark
  @Threads(1)
  public void readIceberg(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "false");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
          materialize(df, blackhole);
        });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergWithIsDeletedColumn(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "false");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).filter("_deleted = false");
          materialize(df, blackhole);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDeletedRows(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "false");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).filter("_deleted = true");
          materialize(df, blackhole);
        });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergVectorized(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "true");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df = spark().read().format("iceberg").load(tableLocation);
          materialize(df, blackhole);
        });
  }

  @Benchmark
  @Threads(1)
  public void readIcebergWithIsDeletedColumnVectorized(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "true");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).filter("_deleted = false");
          materialize(df, blackhole);
        });
  }

  @Benchmark
  @Threads(1)
  public void readDeletedRowsVectorized(Blackhole blackhole) {
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(SPLIT_OPEN_FILE_COST, Integer.toString(128 * 1024 * 1024));
    tableProperties.put(PARQUET_VECTORIZATION_ENABLED, "true");
    withTableProperties(
        tableProperties,
        () -> {
          String tableLocation = table().location();
          Dataset<Row> df =
              spark().read().format("iceberg").load(tableLocation).filter("_deleted = true");
          materialize(df, blackhole);
        });
  }

  protected abstract void appendData() throws IOException;

  protected void writeData(int fileNum) {
    Dataset<Row> df =
        spark()
            .range(NUM_ROWS)
            .withColumnRenamed("id", "longCol")
            .withColumn("intCol", expr("CAST(MOD(longCol, 2147483647) AS INT)"))
            .withColumn("floatCol", expr("CAST(longCol AS FLOAT)"))
            .withColumn("doubleCol", expr("CAST(longCol AS DOUBLE)"))
            .withColumn("dateCol", date_add(current_date(), fileNum))
            .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
            .withColumn("stringCol", expr("CAST(dateCol AS STRING)"));
    appendAsFile(df);
  }

  @Override
  protected Table initTable() {
    Schema schema =
        new Schema(
            required(1, "longCol", Types.LongType.get()),
            required(2, "intCol", Types.IntegerType.get()),
            required(3, "floatCol", Types.FloatType.get()),
            optional(4, "doubleCol", Types.DoubleType.get()),
            optional(6, "dateCol", Types.DateType.get()),
            optional(7, "timestampCol", Types.TimestampType.withZone()),
            optional(8, "stringCol", Types.StringType.get()));
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    HadoopTables tables = new HadoopTables(hadoopConf());
    Map<String, String> properties = Maps.newHashMap();
    properties.put(TableProperties.METADATA_COMPRESSION, "gzip");
    properties.put(TableProperties.FORMAT_VERSION, "2");
    return tables.create(schema, partitionSpec, properties, newTableLocation());
  }

  @Override
  protected Configuration initHadoopConf() {
    return new Configuration();
  }

  protected void writePosDeletes(CharSequence path, long numRows, double percentage)
      throws IOException {
    writePosDeletes(path, numRows, percentage, 1);
  }

  protected void writePosDeletes(
      CharSequence path, long numRows, double percentage, int numDeleteFile) throws IOException {
    writePosDeletesWithNoise(path, numRows, percentage, 0, numDeleteFile);
  }

  protected void writePosDeletesWithNoise(
      CharSequence path, long numRows, double percentage, int numNoise, int numDeleteFile)
      throws IOException {
    Set<Long> deletedPos = Sets.newHashSet();
    while (deletedPos.size() < numRows * percentage) {
      deletedPos.add(ThreadLocalRandom.current().nextLong(numRows));
    }
    LOG.info("pos delete row count: {}, num of delete files: {}", deletedPos.size(), numDeleteFile);

    int partitionSize = (int) (numRows * percentage) / numDeleteFile;
    Iterable<List<Long>> sets = Iterables.partition(deletedPos, partitionSize);
    for (List<Long> item : sets) {
      writePosDeletes(ImmutableList.of(Pair.of(path, item)), numNoise);
    }
  }

  protected void writePosDeletes(
      List<CharSequence> filePaths, long numRowsPerFile, double deletePercentagePerFile)
      throws IOException {

    List<Pair<CharSequence, List<Long>>> pathAndDeletedPosList = Lists.newArrayList();

    for (CharSequence path : filePaths) {
      Set<Long> deletedPos = Sets.newHashSet();
      while (deletedPos.size() < numRowsPerFile * deletePercentagePerFile) {
        deletedPos.add(ThreadLocalRandom.current().nextLong(numRowsPerFile));
      }

      pathAndDeletedPosList.add(Pair.of(path, Lists.newArrayList(deletedPos)));
    }

    writePosDeletes(pathAndDeletedPosList, 0);
  }

  protected void writePosDeletes(
      List<Pair<CharSequence, List<Long>>> pathAndDeletedPosList, int numNoise) throws IOException {
    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table()).dataFileFormat(fileFormat()).build();

    ClusteredPositionDeleteWriter<InternalRow> writer =
        new ClusteredPositionDeleteWriter<>(
            writerFactory, fileFactory, table().io(), TARGET_FILE_SIZE_IN_BYTES);

    PartitionSpec unpartitionedSpec = table().specs().get(0);

    PositionDelete<InternalRow> positionDelete = PositionDelete.create();
    try (ClusteredPositionDeleteWriter<InternalRow> closeableWriter = writer) {
      for (Pair<CharSequence, List<Long>> pathAndDeletedPos : pathAndDeletedPosList) {
        CharSequence path = pathAndDeletedPos.first();
        List<Long> deletedPos = pathAndDeletedPos.second();
        for (Long pos : deletedPos) {
          positionDelete.set(path, pos, null);
          closeableWriter.write(positionDelete, unpartitionedSpec, null);
          for (int i = 0; i < numNoise; i++) {
            positionDelete.set(noisePath(path), pos, null);
            closeableWriter.write(positionDelete, unpartitionedSpec, null);
          }
        }
      }
    }

    RowDelta rowDelta = table().newRowDelta();
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.validateDeletedFiles().commit();
  }

  protected void writeEqDeletes(long numRows, double percentage) throws IOException {
    Set<Long> deletedValues = Sets.newHashSet();
    while (deletedValues.size() < numRows * percentage) {
      deletedValues.add(ThreadLocalRandom.current().nextLong(numRows));
    }

    List<InternalRow> rows = Lists.newArrayList();
    for (Long value : deletedValues) {
      GenericInternalRow genericInternalRow = new GenericInternalRow(7);
      genericInternalRow.setLong(0, value);
      genericInternalRow.setInt(1, (int) (value % Integer.MAX_VALUE));
      genericInternalRow.setFloat(2, (float) value);
      genericInternalRow.setNullAt(3);
      genericInternalRow.setNullAt(4);
      genericInternalRow.setNullAt(5);
      genericInternalRow.setNullAt(6);
      rows.add(genericInternalRow);
    }
    LOG.info("Num of equality deleted rows: {}", rows.size());

    writeEqDeletes(rows);
  }

  private void writeEqDeletes(List<InternalRow> rows) throws IOException {
    int equalityFieldId = table().schema().findField("longCol").fieldId();

    OutputFileFactory fileFactory = newFileFactory();
    SparkFileWriterFactory writerFactory =
        SparkFileWriterFactory.builderFor(table())
            .dataFileFormat(fileFormat())
            .equalityDeleteRowSchema(table().schema())
            .equalityFieldIds(new int[] {equalityFieldId})
            .build();

    ClusteredEqualityDeleteWriter<InternalRow> writer =
        new ClusteredEqualityDeleteWriter<>(
            writerFactory, fileFactory, table().io(), TARGET_FILE_SIZE_IN_BYTES);

    PartitionSpec unpartitionedSpec = table().specs().get(0);
    try (ClusteredEqualityDeleteWriter<InternalRow> closeableWriter = writer) {
      for (InternalRow row : rows) {
        closeableWriter.write(row, unpartitionedSpec, null);
      }
    }

    RowDelta rowDelta = table().newRowDelta();
    LOG.info("Num of Delete File: {}", writer.result().deleteFiles().size());
    writer.result().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.validateDeletedFiles().commit();
  }

  private OutputFileFactory newFileFactory() {
    return OutputFileFactory.builderFor(table(), 1, 1).format(fileFormat()).build();
  }

  private CharSequence noisePath(CharSequence path) {
    // assume the data file name would be something like
    // "00000-0-30da64e0-56b5-4743-a11b-3188a1695bf7-00001.parquet"
    // so the dataFileSuffixLen is the UUID string length + length of "-00001.parquet", which is 36
    // + 14 = 60. It's OK
    // to be not accurate here.
    int dataFileSuffixLen = 60;
    UUID uuid = UUID.randomUUID();
    if (path.length() > dataFileSuffixLen) {
      return path.subSequence(0, dataFileSuffixLen) + uuid.toString();
    } else {
      return uuid.toString();
    }
  }
}
