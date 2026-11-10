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

import static org.apache.iceberg.spark.source.SparkSQLExecutionHelper.lastExecutedMetricValue;
import static org.apache.spark.sql.functions.date_add;
import static org.apache.spark.sql.functions.expr;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.source.metrics.ScanDuration;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.metric.CustomMetric;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.collection.JavaConverters;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReadMetrics extends TestBaseWithCatalog {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS dim");
  }

  @TestTemplate
  void effectiveSplitTargetSize() throws NoSuchTableException, IOException {
    long tableSplitSize = 128L * 1024 * 1024;
    long optionSplitSize = 32L * 1024 * 1024;
    int parallelism = 1000;
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('read.split.target-size'='%s')",
        tableName, tableSplitSize);
    spark.range(100).coalesce(1).writeTo(tableName).append();

    long scanSize = 0;
    try (CloseableIterable<FileScanTask> tasks =
        validationCatalog.loadTable(tableIdent).newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        scanSize += task.sizeBytes();
      }
    }
    long adaptiveSplitSize = TableScanUtil.adjustSplitSize(scanSize, parallelism, tableSplitSize);
    assertThat(adaptiveSplitSize).isNotEqualTo(tableSplitSize);

    // adaptive sizing lowers the table target for a small scan
    withSQLConf(
        ImmutableMap.of(
            SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED,
            "true",
            SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_PARALLELISM,
            String.valueOf(parallelism)),
        () -> {
          Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
          df.collect();
          SparkPlan scan = df.queryExecution().executedPlan().collectLeaves().head();
          assertThat(scan.metrics().get("effectiveSplitTargetSize").get().value())
              .isEqualTo(adaptiveSplitSize);
        });

    // without adaptive sizing, the table property is the target
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED, "false"),
        () -> {
          Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
          df.collect();
          SparkPlan scan = df.queryExecution().executedPlan().collectLeaves().head();
          assertThat(scan.metrics().get("effectiveSplitTargetSize").get().value())
              .isEqualTo(tableSplitSize);
        });

    // an explicit read option is used as is, even with adaptive sizing enabled
    withSQLConf(
        ImmutableMap.of(SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED, "true"),
        () -> {
          Dataset<Row> df =
              spark
                  .read()
                  .format("iceberg")
                  .option(SparkReadOptions.SPLIT_SIZE, String.valueOf(optionSplitSize))
                  .load(tableName);
          df.collect();
          SparkPlan scan = df.queryExecution().executedPlan().collectLeaves().head();
          assertThat(scan.metrics().get("effectiveSplitTargetSize").get().value())
              .isEqualTo(optionSplitSize);
        });
  }

  @TestTemplate
  void effectiveSplitTargetSizeWithoutSnapshot() {
    sql("CREATE TABLE %s (id BIGINT) USING iceberg", tableName);

    // no task groups are planned for a table without a snapshot
    Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
    df.collect();
    SparkPlan scan = df.queryExecution().executedPlan().collectLeaves().head();
    assertThat(scan.metrics().get("effectiveSplitTargetSize").get().value()).isZero();
  }

  @TestTemplate
  void effectiveSplitTargetSizeWithRuntimeFilter() throws NoSuchTableException, IOException {
    long tableSplitSize = 128L * 1024 * 1024;
    int parallelism = 1000;
    sql(
        "CREATE TABLE %s (id BIGINT, date DATE) USING iceberg PARTITIONED BY (date) "
            + "TBLPROPERTIES ('read.split.target-size'='%s')",
        tableName, tableSplitSize);
    spark
        .range(1, 100)
        .withColumn("date", date_add(expr("DATE '1970-01-01'"), expr("CAST(id % 4 AS INT)")))
        .coalesce(1)
        .writeTo(tableName)
        .option(SparkWriteOptions.FANOUT_ENABLED, "true")
        .append();

    sql("CREATE TABLE dim (id BIGINT, date DATE) USING parquet");
    spark
        .range(1, 10)
        .withColumn("date", expr("DATE '1970-01-02'"))
        .coalesce(1)
        .write()
        .mode("append")
        .insertInto("dim");

    long filteredScanSize = 0;
    try (CloseableIterable<FileScanTask> tasks =
        validationCatalog
            .loadTable(tableIdent)
            .newScan()
            .filter(Expressions.equal("date", "1970-01-02"))
            .planFiles()) {
      for (FileScanTask task : tasks) {
        filteredScanSize += task.sizeBytes();
      }
    }
    long expectedSplitSize =
        TableScanUtil.adjustSplitSize(filteredScanSize, parallelism, tableSplitSize);
    long expectedRows =
        spark
            .sql(String.format("SELECT * FROM %s WHERE date = DATE '1970-01-02'", tableName))
            .count();

    // the metric is reported for a scan whose task groups are re-planned after a runtime filter;
    // small scans clamp to the same adaptive target before and after filtering
    withSQLConf(
        ImmutableMap.of(
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
            "false",
            SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_ENABLED,
            "true",
            SparkSQLProperties.READ_ADAPTIVE_SPLIT_SIZE_PARALLELISM,
            String.valueOf(parallelism)),
        () -> {
          Dataset<Row> df =
              spark.sql(
                  String.format(
                      "SELECT f.* FROM %s f JOIN dim d ON f.date = d.date AND d.id = 1",
                      tableName));
          assertThat(df.collectAsList()).hasSize((int) expectedRows);

          List<SparkPlan> scans =
              seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves())
                  .asJava()
                  .stream()
                  .filter(BatchScanExec.class::isInstance)
                  .collect(Collectors.toList());
          assertThat(scans).hasSize(1);
          assertThat(((BatchScanExec) scans.get(0)).runtimeFilters().nonEmpty()).isTrue();

          Map<String, SQLMetric> metricsMap =
              JavaConverters.mapAsJavaMapConverter(scans.get(0).metrics()).asJava();
          assertThat(metricsMap)
              .hasEntrySatisfying(
                  "effectiveSplitTargetSize",
                  sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(expectedSplitSize));
        });
  }

  @TestTemplate
  void customMetricsAreLoadableBySpark() {
    sql("CREATE TABLE %s (id BIGINT) USING iceberg", tableName);

    Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
    SparkPlan scan = df.queryExecution().executedPlan().collectLeaves().head();

    // Spark's SQL listener loads each metric by class name, which requires a public class with a
    // public zero-argument constructor
    for (CustomMetric metric : ((BatchScanExec) scan).scan().supportedCustomMetrics()) {
      Class<?> metricClass = metric.getClass();
      assertThat(Modifier.isPublic(metricClass.getModifiers()))
          .as("Metric class %s should be public", metricClass.getName())
          .isTrue();
      assertThatCode(() -> metricClass.getConstructor().newInstance())
          .as("Metric class %s should have a public no-arg constructor", metricClass.getName())
          .doesNotThrowAnyException();
    }
  }

  @TestTemplate
  public void testReadMetricsForV1Table() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='1')",
        tableName);

    spark.range(10000).coalesce(1).writeTo(tableName).append();
    spark.range(10001, 20000).coalesce(1).writeTo(tableName).append();

    Dataset<Row> df = spark.sql(String.format("select * from %s where id < 10000", tableName));
    df.collect();

    List<SparkPlan> sparkPlans =
        seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
    Map<String, SQLMetric> metricsMap =
        JavaConverters.mapAsJavaMapConverter(sparkPlans.get(0).metrics()).asJava();

    // Common
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "effectiveSplitTargetSize",
            sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalPlanningDuration", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scanDuration", sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));

    // data manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataManifest", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(2));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(2));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // data files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataFileSize", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));

    // delete manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // delete files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteFileSize", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "equalityDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "indexedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "positionalDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
  }

  @TestTemplate
  public void testReadMetricsForV2Table() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')",
        tableName);

    spark.range(10000).coalesce(1).writeTo(tableName).append();
    spark.range(10001, 20000).coalesce(1).writeTo(tableName).append();

    Dataset<Row> df = spark.sql(String.format("select * from %s where id < 10000", tableName));
    df.collect();

    List<SparkPlan> sparkPlans =
        seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
    Map<String, SQLMetric> metricsMap =
        JavaConverters.mapAsJavaMapConverter(sparkPlans.get(0).metrics()).asJava();

    // Common
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "effectiveSplitTargetSize",
            sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalPlanningDuration", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scanDuration", sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));

    // data manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataManifest", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(2));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(2));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // data files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataFileSize", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));

    // delete manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // delete files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteFileSize", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "equalityDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "indexedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "positionalDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
  }

  @TestTemplate
  public void scanDurationAggregatedAcrossTasks() throws NoSuchTableException {
    sql("CREATE TABLE %s (id BIGINT) USING iceberg", tableName);

    spark.range(10000).coalesce(1).writeTo(tableName).append();
    spark.range(10001, 20000).coalesce(1).writeTo(tableName).append();

    Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
    assertThat(df.collectAsList()).hasSize(19999);

    List<SparkPlan> sparkPlans =
        seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
    Map<String, SQLMetric> metricsMap =
        JavaConverters.mapAsJavaMapConverter(sparkPlans.get(0).metrics()).asJava();

    // the driver-side accumulator sums TaskScanDuration across all tasks of the scan
    long aggregatedNanos = metricsMap.get("scanDuration").value();
    assertThat(aggregatedNanos).isGreaterThan(0);

    // the value the UI renders, read back from the SQL status store
    assertThat(lastExecutedMetricValue(spark, new ScanDuration().description()))
        .containsPattern(
            "total \\(min, med, max\\)\\R"
                + "\\d+(\\.\\d+)? (ns|us|ms|s) "
                + "\\(\\d+(\\.\\d+)? (ns|us|ms|s), "
                + "\\d+(\\.\\d+)? (ns|us|ms|s), "
                + "\\d+(\\.\\d+)? (ns|us|ms|s)\\)");
  }

  @TestTemplate
  public void scanDurationForNonVectorizedRead() throws NoSuchTableException {
    // the other tests read through BatchDataReader, disable vectorization to cover RowDataReader
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('%s'='false')",
        tableName, TableProperties.PARQUET_VECTORIZATION_ENABLED);

    spark.range(10000).coalesce(1).writeTo(tableName).append();

    Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
    df.collect();

    List<SparkPlan> sparkPlans =
        seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
    // sanity check that this really exercised the row-based path
    assertThat(sparkPlans.get(0).supportsColumnar()).isFalse();

    Map<String, SQLMetric> metricsMap =
        JavaConverters.mapAsJavaMapConverter(sparkPlans.get(0).metrics()).asJava();
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scanDuration", sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));
  }

  @TestTemplate
  public void testDeleteMetrics() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT)"
            + " USING iceberg"
            + " TBLPROPERTIES (\n"
            + "    'write.delete.mode'='merge-on-read',\n"
            + "    'write.update.mode'='merge-on-read',\n"
            + "    'write.merge.mode'='merge-on-read',\n"
            + "    'format-version'='2'\n"
            + "  )",
        tableName);

    spark.range(10000).coalesce(1).writeTo(tableName).append();

    spark.sql(String.format("DELETE FROM %s WHERE id = 1", tableName)).collect();
    Dataset<Row> df = spark.sql(String.format("SELECT * FROM %s", tableName));
    df.collect();

    List<SparkPlan> sparkPlans =
        seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
    Map<String, SQLMetric> metricsMap =
        JavaConverters.mapAsJavaMapConverter(sparkPlans.get(0).metrics()).asJava();

    // Common
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalPlanningDuration", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scanDuration", sqlMetric -> assertThat(sqlMetric.value()).isGreaterThan(0));

    // data manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataManifest", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // data files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDataFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDataFileSize", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));

    // delete manifests
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "scannedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteManifests", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));

    // delete files
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "totalDeleteFileSize", sqlMetric -> assertThat(sqlMetric.value()).isNotEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "resultDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "equalityDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "indexedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "positionalDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            "skippedDeleteFiles", sqlMetric -> assertThat(sqlMetric.value()).isEqualTo(0));
  }
}
