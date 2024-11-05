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
package org.apache.iceberg.spark;

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BatchScan;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileGenerationUtil;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.SparkDistributedDataScan;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
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
 * A benchmark that evaluates the job planning performance.
 *
 * <p>To run this benchmark for spark-3.5: <code>
 *   ./gradlew -DsparkVersions=3.5 :iceberg-spark:iceberg-spark-extensions-3.5_2.12:jmh
 *       -PjmhIncludeRegex=PlanningBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-planning-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Timeout(time = 20, timeUnit = TimeUnit.MINUTES)
@BenchmarkMode(Mode.SingleShotTime)
public class PlanningBenchmark {

  private static final String TABLE_NAME = "test_table";
  private static final String PARTITION_COLUMN = "ss_ticket_number";
  private static final int PARTITION_VALUE = 10;
  private static final String SORT_KEY_COLUMN = "ss_sold_date_sk";
  private static final int SORT_KEY_VALUE = 5;

  private static final Expression SORT_KEY_PREDICATE =
      Expressions.equal(SORT_KEY_COLUMN, SORT_KEY_VALUE);
  private static final Expression PARTITION_PREDICATE =
      Expressions.equal(PARTITION_COLUMN, PARTITION_VALUE);
  private static final Expression PARTITION_AND_SORT_KEY_PREDICATE =
      Expressions.and(PARTITION_PREDICATE, SORT_KEY_PREDICATE);

  private static final int NUM_PARTITIONS = 30;
  private static final int NUM_DATA_FILES_PER_PARTITION = 50_000;
  private static final int NUM_DELETE_FILES_PER_PARTITION = 50;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;
  private Table table;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException {
    setupSpark();
    initTable();
    initDataAndDeletes();
  }

  @TearDown
  public void tearDownBenchmark() {
    dropTable();
    tearDownSpark();
  }

  @Benchmark
  @Threads(1)
  public void localPlanningWithPartitionAndMinMaxFilter(Blackhole blackhole) {
    BatchScan scan = table.newBatchScan();
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, PARTITION_AND_SORT_KEY_PREDICATE);
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void distributedPlanningWithPartitionAndMinMaxFilter(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(DISTRIBUTED, DISTRIBUTED);
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, PARTITION_AND_SORT_KEY_PREDICATE);
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void localPlanningWithMinMaxFilter(Blackhole blackhole) {
    BatchScan scan = table.newBatchScan();
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, SORT_KEY_PREDICATE);
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void distributedPlanningWithMinMaxFilter(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(DISTRIBUTED, DISTRIBUTED);
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, SORT_KEY_PREDICATE);
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void localPlanningWithoutFilter(Blackhole blackhole) {
    BatchScan scan = table.newBatchScan();
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void distributedPlanningWithoutFilter(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(DISTRIBUTED, DISTRIBUTED);
    List<ScanTask> fileTasks = planFilesWithoutColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void localPlanningWithoutFilterWithStats(Blackhole blackhole) {
    BatchScan scan = table.newBatchScan();
    List<ScanTask> fileTasks = planFilesWithColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void distributedPlanningWithoutFilterWithStats(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(DISTRIBUTED, DISTRIBUTED);
    List<ScanTask> fileTasks = planFilesWithColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void distributedDataLocalDeletesPlanningWithoutFilterWithStats(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(DISTRIBUTED, LOCAL);
    List<ScanTask> fileTasks = planFilesWithColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void localDataDistributedDeletesPlanningWithoutFilterWithStats(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(LOCAL, DISTRIBUTED);
    List<ScanTask> fileTasks = planFilesWithColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  @Benchmark
  @Threads(1)
  public void localPlanningViaDistributedScanWithoutFilterWithStats(Blackhole blackhole) {
    BatchScan scan = newDistributedScan(LOCAL, LOCAL);
    List<ScanTask> fileTasks = planFilesWithColumnStats(scan, Expressions.alwaysTrue());
    blackhole.consume(fileTasks);
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.driver.maxResultSize", "8G")
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

  private void initDataAndDeletes() {
    for (int partitionOrdinal = 0; partitionOrdinal < NUM_PARTITIONS; partitionOrdinal++) {
      StructLike partition = TestHelpers.Row.of(partitionOrdinal);

      RowDelta rowDelta = table.newRowDelta();

      for (int fileOrdinal = 0; fileOrdinal < NUM_DATA_FILES_PER_PARTITION; fileOrdinal++) {
        DataFile dataFile = generateDataFile(partition, Integer.MIN_VALUE, Integer.MIN_VALUE);
        rowDelta.addRows(dataFile);
      }

      // add one data file that would match the sort key predicate
      DataFile sortKeyDataFile = generateDataFile(partition, SORT_KEY_VALUE, SORT_KEY_VALUE);
      rowDelta.addRows(sortKeyDataFile);

      for (int fileOrdinal = 0; fileOrdinal < NUM_DELETE_FILES_PER_PARTITION; fileOrdinal++) {
        DeleteFile deleteFile = FileGenerationUtil.generatePositionDeleteFile(table, partition);
        rowDelta.addDeletes(deleteFile);
      }

      rowDelta.commit();
    }
  }

  private DataFile generateDataFile(StructLike partition, int sortKeyMin, int sortKeyMax) {
    int sortKeyFieldId = table.schema().findField(SORT_KEY_COLUMN).fieldId();
    ByteBuffer lower = Conversions.toByteBuffer(Types.IntegerType.get(), sortKeyMin);
    Map<Integer, ByteBuffer> lowerBounds = ImmutableMap.of(sortKeyFieldId, lower);
    ByteBuffer upper = Conversions.toByteBuffer(Types.IntegerType.get(), sortKeyMax);
    Map<Integer, ByteBuffer> upperBounds = ImmutableMap.of(sortKeyFieldId, upper);
    return FileGenerationUtil.generateDataFile(table, partition, lowerBounds, upperBounds);
  }

  private String newWarehouseDir() {
    return hadoopConf.get("hadoop.tmp.dir") + UUID.randomUUID();
  }

  private List<ScanTask> planFilesWithoutColumnStats(BatchScan scan, Expression predicate) {
    return planFiles(scan, predicate, false);
  }

  private List<ScanTask> planFilesWithColumnStats(BatchScan scan, Expression predicate) {
    return planFiles(scan, predicate, true);
  }

  private List<ScanTask> planFiles(BatchScan scan, Expression predicate, boolean withColumnStats) {
    table.refresh();

    BatchScan configuredScan = scan.filter(predicate);

    if (withColumnStats) {
      configuredScan = scan.includeColumnStats();
    }

    try (CloseableIterable<ScanTask> fileTasks = configuredScan.planFiles()) {
      return Lists.newArrayList(fileTasks);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private BatchScan newDistributedScan(PlanningMode dataMode, PlanningMode deleteMode) {
    table
        .updateProperties()
        .set(TableProperties.DATA_PLANNING_MODE, dataMode.modeName())
        .set(TableProperties.DELETE_PLANNING_MODE, deleteMode.modeName())
        .commit();
    SparkReadConf readConf = new SparkReadConf(spark, table, ImmutableMap.of());
    return new SparkDistributedDataScan(spark, table, readConf);
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
