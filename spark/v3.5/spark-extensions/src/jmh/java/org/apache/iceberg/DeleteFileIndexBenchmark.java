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

import static org.apache.spark.sql.functions.lit;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.StructType;
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
import org.openjdk.jmh.infra.Blackhole;

/**
 * A benchmark that evaluates the delete file index build and lookup performance.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-extensions-3.5_2.12:jmh
 *       -PjmhIncludeRegex=DeleteFileIndexBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-delete-file-index-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Timeout(time = 20, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.SingleShotTime)
public class DeleteFileIndexBenchmark {

  private static final String TABLE_NAME = "test_table";
  private static final String PARTITION_COLUMN = "ss_ticket_number";

  private static final int NUM_PARTITIONS = 50;
  private static final int NUM_REAL_DATA_FILES_PER_PARTITION = 25;
  private static final int NUM_REPLICA_DATA_FILES_PER_PARTITION = 50_000;
  private static final int NUM_DELETE_FILES_PER_PARTITION = 100;
  private static final int NUM_ROWS_PER_DATA_FILE = 500;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;
  private Table table;

  private List<DataFile> dataFiles;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException {
    setupSpark();
    initTable();
    initDataAndDeletes();
    loadDataFiles();
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
    tearDownSpark();
  }

  @Benchmark
  @Threads(1)
  public void buildIndexAndLookup(Blackhole blackhole) {
    DeleteFileIndex deletes = buildDeletes();
    for (DataFile dataFile : dataFiles) {
      DeleteFile[] deleteFiles = deletes.forDataFile(dataFile.dataSequenceNumber(), dataFile);
      blackhole.consume(deleteFiles);
    }
  }

  private void loadDataFiles() {
    table.refresh();

    Snapshot snapshot = table.currentSnapshot();

    ManifestGroup manifestGroup =
        new ManifestGroup(table.io(), snapshot.dataManifests(table.io()), ImmutableList.of());

    try (CloseableIterable<ManifestEntry<DataFile>> entries = manifestGroup.entries()) {
      List<DataFile> files = Lists.newArrayList();
      for (ManifestEntry<DataFile> entry : entries) {
        files.add(entry.file().copyWithoutStats());
      }
      this.dataFiles = files;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private DeleteFileIndex buildDeletes() {
    table.refresh();

    List<ManifestFile> deleteManifests = table.currentSnapshot().deleteManifests(table.io());

    return DeleteFileIndex.builderFor(table.io(), deleteManifests)
        .specsById(table.specs())
        .planWith(ThreadPools.getWorkerPool())
        .build();
  }

  private DataFile loadAddedDataFile() {
    table.refresh();

    Iterable<DataFile> addedDataFiles = table.currentSnapshot().addedDataFiles(table.io());
    return Iterables.getOnlyElement(addedDataFiles);
  }

  private DeleteFile loadAddedDeleteFile() {
    table.refresh();

    Iterable<DeleteFile> addedDeleteFiles = table.currentSnapshot().addedDeleteFiles(table.io());
    return Iterables.getOnlyElement(addedDeleteFiles);
  }

  private void initDataAndDeletes() throws NoSuchTableException {
    Schema schema = table.schema();
    PartitionSpec spec = table.spec();
    LocationProvider locations = table.locationProvider();

    for (int partitionOrdinal = 0; partitionOrdinal < NUM_PARTITIONS; partitionOrdinal++) {
      Dataset<Row> inputDF =
          randomDataDF(schema, NUM_ROWS_PER_DATA_FILE)
              .drop(PARTITION_COLUMN)
              .withColumn(PARTITION_COLUMN, lit(partitionOrdinal));

      for (int fileOrdinal = 0; fileOrdinal < NUM_REAL_DATA_FILES_PER_PARTITION; fileOrdinal++) {
        appendAsFile(inputDF);
      }

      DataFile dataFile = loadAddedDataFile();

      sql(
          "DELETE FROM %s WHERE ss_item_sk IS NULL AND %s = %d",
          TABLE_NAME, PARTITION_COLUMN, partitionOrdinal);

      DeleteFile deleteFile = loadAddedDeleteFile();

      AppendFiles append = table.newFastAppend();

      for (int fileOrdinal = 0; fileOrdinal < NUM_REPLICA_DATA_FILES_PER_PARTITION; fileOrdinal++) {
        String replicaFileName = UUID.randomUUID() + "-replica.parquet";
        DataFile replicaDataFile =
            DataFiles.builder(spec)
                .copy(dataFile)
                .withPath(locations.newDataLocation(spec, dataFile.partition(), replicaFileName))
                .build();
        append.appendFile(replicaDataFile);
      }

      append.commit();

      RowDelta rowDelta = table.newRowDelta();

      for (int fileOrdinal = 0; fileOrdinal < NUM_DELETE_FILES_PER_PARTITION; fileOrdinal++) {
        String replicaFileName = UUID.randomUUID() + "-replica.parquet";
        DeleteFile replicaDeleteFile =
            FileMetadata.deleteFileBuilder(spec)
                .copy(deleteFile)
                .withPath(locations.newDataLocation(spec, deleteFile.partition(), replicaFileName))
                .build();
        rowDelta.addDeletes(replicaDeleteFile);
      }

      rowDelta.commit();
    }
  }

  private void appendAsFile(Dataset<Row> df) throws NoSuchTableException {
    df.coalesce(1).writeTo(TABLE_NAME).append();
  }

  private Dataset<Row> randomDataDF(Schema schema, int numRows) {
    Iterable<InternalRow> rows = RandomData.generateSpark(schema, numRows, 0);
    JavaSparkContext context = JavaSparkContext.fromSparkContext(spark.sparkContext());
    JavaRDD<InternalRow> rowRDD = context.parallelize(Lists.newArrayList(rows));
    StructType rowSparkType = SparkSchemaUtil.convert(schema);
    return spark.internalCreateDataFrame(JavaRDD.toRDD(rowRDD), rowSparkType, false);
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
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
    sql(
        "CREATE TABLE %s ( "
            + " `ss_sold_date_sk` INT, "
            + " `ss_sold_time_sk` INT, "
            + " `ss_item_sk` INT, "
            + " `ss_customer_sk` STRING, "
            + " `ss_cdemo_sk` STRING, "
            + " `ss_hdemo_sk` STRING, "
            + " `ss_addr_sk` STRING, "
            + " `ss_store_sk` STRING, "
            + " `ss_promo_sk` STRING, "
            + " `ss_ticket_number` INT, "
            + " `ss_quantity` STRING, "
            + " `ss_wholesale_cost` STRING, "
            + " `ss_list_price` STRING, "
            + " `ss_sales_price` STRING, "
            + " `ss_ext_discount_amt` STRING, "
            + " `ss_ext_sales_price` STRING, "
            + " `ss_ext_wholesale_cost` STRING, "
            + " `ss_ext_list_price` STRING, "
            + " `ss_ext_tax` STRING, "
            + " `ss_coupon_amt` STRING, "
            + " `ss_net_paid` STRING, "
            + " `ss_net_paid_inc_tax` STRING, "
            + " `ss_net_profit` STRING "
            + ")"
            + "USING iceberg "
            + "PARTITIONED BY (%s) "
            + "TBLPROPERTIES ("
            + " '%s' '%b',"
            + " '%s' '%s',"
            + " '%s' '%d')",
        TABLE_NAME,
        PARTITION_COLUMN,
        TableProperties.MANIFEST_MERGE_ENABLED,
        false,
        TableProperties.DELETE_MODE,
        RowLevelOperationMode.MERGE_ON_READ.modeName(),
        TableProperties.FORMAT_VERSION,
        2);

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
