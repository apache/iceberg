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

import static org.apache.spark.sql.functions.current_date;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;

import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.FormatString;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.RowLevelOperationMode;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.internal.SQLConf;
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
import org.openjdk.jmh.annotations.Warmup;

/**
 * A benchmark that evaluates the performance of the cardinality check in MERGE operations.
 *
 * <p>To run this benchmark for spark-4.0: <code>
 *   ./gradlew -DsparkVersions=4.0 :iceberg-spark:iceberg-spark-extensions-4.0_2.13:jmh
 *       -PjmhIncludeRegex=MergeCardinalityCheckBenchmark
 *       -PjmhOutputPath=benchmark/iceberg-merge-cardinality-check-benchmark.txt
 * </code>
 */
@Fork(1)
@State(Scope.Benchmark)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.SingleShotTime)
public class MergeCardinalityCheckBenchmark {

  private static final String TABLE_NAME = "test_table";
  private static final int NUM_FILES = 5;
  private static final int NUM_ROWS_PER_FILE = 1_000_000;
  private static final int NUM_UNMATCHED_RECORDS_PER_MERGE = 100_000;

  private final Configuration hadoopConf = new Configuration();
  private SparkSession spark;
  private long originalSnapshotId;

  @Setup
  public void setupBenchmark() throws NoSuchTableException, ParseException {
    setupSpark();
    initTable();
    appendData();

    Table table = Spark3Util.loadIcebergTable(spark, TABLE_NAME);
    this.originalSnapshotId = table.currentSnapshot().snapshotId();
  }

  @TearDown
  public void tearDownBenchmark() {
    tearDownSpark();
    dropTable();
  }

  @Benchmark
  @Threads(1)
  public void copyOnWriteMergeCardinalityCheck10PercentUpdates() {
    runBenchmark(RowLevelOperationMode.COPY_ON_WRITE, 0.1);
  }

  @Benchmark
  @Threads(1)
  public void copyOnWriteMergeCardinalityCheck30PercentUpdates() {
    runBenchmark(RowLevelOperationMode.COPY_ON_WRITE, 0.3);
  }

  @Benchmark
  @Threads(1)
  public void copyOnWriteMergeCardinalityCheck90PercentUpdates() {
    runBenchmark(RowLevelOperationMode.COPY_ON_WRITE, 0.9);
  }

  @Benchmark
  @Threads(1)
  public void mergeOnReadMergeCardinalityCheck10PercentUpdates() {
    runBenchmark(RowLevelOperationMode.MERGE_ON_READ, 0.1);
  }

  @Benchmark
  @Threads(1)
  public void mergeOnReadMergeCardinalityCheck30PercentUpdates() {
    runBenchmark(RowLevelOperationMode.MERGE_ON_READ, 0.3);
  }

  @Benchmark
  @Threads(1)
  public void mergeOnReadMergeCardinalityCheck90PercentUpdates() {
    runBenchmark(RowLevelOperationMode.MERGE_ON_READ, 0.9);
  }

  private void runBenchmark(RowLevelOperationMode mode, double updatePercentage) {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s' '%s')",
        TABLE_NAME, TableProperties.MERGE_MODE, mode.modeName());

    Dataset<Long> insertDataDF = spark.range(-NUM_UNMATCHED_RECORDS_PER_MERGE, 0, 1);
    Dataset<Long> updateDataDF = spark.range((long) (updatePercentage * NUM_ROWS_PER_FILE));
    Dataset<Long> sourceDF = updateDataDF.union(insertDataDF);
    sourceDF.createOrReplaceTempView("source");

    sql(
        "MERGE INTO %s t USING source s "
            + "ON t.id = s.id "
            + "WHEN MATCHED THEN "
            + " UPDATE SET stringCol = 'invalid' "
            + "WHEN NOT MATCHED THEN "
            + " INSERT (id, intCol, floatCol, doubleCol, decimalCol, dateCol, timestampCol, stringCol) "
            + "   VALUES (s.id, null, null, null, null, null, null, 'new')",
        TABLE_NAME);

    sql(
        "CALL system.rollback_to_snapshot(table => '%s', snapshot_id => %dL)",
        TABLE_NAME, originalSnapshotId);
  }

  private void setupSpark() {
    this.spark =
        SparkSession.builder()
            .config("spark.ui.enabled", false)
            .config("spark.sql.extensions", IcebergSparkSessionExtensions.class.getName())
            .config("spark.sql.catalog.spark_catalog", SparkSessionCatalog.class.getName())
            .config("spark.sql.catalog.spark_catalog.type", "hadoop")
            .config("spark.sql.catalog.spark_catalog.warehouse", newWarehouseDir())
            .config(SQLConf.DYNAMIC_PARTITION_PRUNING_ENABLED().key(), "false")
            .config(SQLConf.RUNTIME_ROW_LEVEL_OPERATION_GROUP_FILTER_ENABLED().key(), "false")
            .config(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false")
            .config(SQLConf.SHUFFLE_PARTITIONS().key(), "2")
            .master("local")
            .getOrCreate();
  }

  private void tearDownSpark() {
    spark.stop();
  }

  private void initTable() {
    sql(
        "CREATE TABLE %s ( "
            + " id LONG, intCol INT, floatCol FLOAT, doubleCol DOUBLE, "
            + " decimalCol DECIMAL(20, 5), dateCol DATE, timestampCol TIMESTAMP, "
            + " stringCol STRING)"
            + "USING iceberg "
            + "TBLPROPERTIES ("
            + " '%s' '%s',"
            + " '%s' '%d',"
            + " '%s' '%d')",
        TABLE_NAME,
        TableProperties.MERGE_DISTRIBUTION_MODE,
        DistributionMode.NONE.modeName(),
        TableProperties.SPLIT_OPEN_FILE_COST,
        Integer.MAX_VALUE,
        TableProperties.FORMAT_VERSION,
        2);

    sql("ALTER TABLE %s WRITE ORDERED BY id", TABLE_NAME);
  }

  private void dropTable() {
    sql("DROP TABLE IF EXISTS %s PURGE", TABLE_NAME);
  }

  private void appendData() throws NoSuchTableException {
    for (int fileNum = 1; fileNum <= NUM_FILES; fileNum++) {
      Dataset<Row> inputDF =
          spark
              .range(NUM_ROWS_PER_FILE)
              .withColumn("intCol", expr("CAST(id AS INT)"))
              .withColumn("floatCol", expr("CAST(id AS FLOAT)"))
              .withColumn("doubleCol", expr("CAST(id AS DOUBLE)"))
              .withColumn("decimalCol", expr("CAST(id AS DECIMAL(20, 5))"))
              .withColumn("dateCol", date_add(current_date(), fileNum))
              .withColumn("timestampCol", expr("TO_TIMESTAMP(dateCol)"))
              .withColumn("stringCol", expr("CAST(dateCol AS STRING)"));
      appendAsFile(inputDF);
    }
  }

  private void appendAsFile(Dataset<Row> df) throws NoSuchTableException {
    // ensure the schema is precise (including nullability)
    StructType sparkSchema = spark.table(TABLE_NAME).schema();
    spark.createDataFrame(df.rdd(), sparkSchema).coalesce(1).writeTo(TABLE_NAME).append();
  }

  private String newWarehouseDir() {
    return hadoopConf.get("hadoop.tmp.dir") + UUID.randomUUID();
  }

  @FormatMethod
  private void sql(@FormatString String query, Object... args) {
    spark.sql(String.format(query, args));
  }
}
