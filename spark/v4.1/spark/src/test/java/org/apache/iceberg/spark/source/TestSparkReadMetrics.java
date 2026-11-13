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
import static org.assertj.core.api.Assertions.assertThat;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.source.metrics.ScanDuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
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

  @TestTemplate
  public void metricsWhenTaskReadsSeveralPartitions() {
    sql(
        "CREATE TABLE %s (id BIGINT, dep STRING, region STRING) USING iceberg "
            + "PARTITIONED BY (dep, region) "
            + "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read')",
        tableName);
    sql(
        "INSERT INTO %s VALUES (1, 'hr', 'us'), (2, 'hr', 'eu'), (3, 'hr', 'apac'), "
            + "(4, 'ops', 'us'), (5, 'ops', 'eu'), (11, 'hr', 'us'), (12, 'hr', 'eu'), "
            + "(13, 'hr', 'apac'), (14, 'ops', 'us'), (15, 'ops', 'eu')",
        tableName);
    // one deleted row in each of the 5 partitions
    sql("DELETE FROM %s WHERE id > 10", tableName);

    // joining on a subset of the partition keys makes Spark group the 3 hr and the 2 ops
    // partitions into one task each, so every task opens several readers
    Map<String, String> spjConf =
        ImmutableMap.of(
            SQLConf.V2_BUCKETING_ENABLED().key(),
            "true",
            SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS().key(),
            "true",
            SQLConf.REQUIRE_ALL_CLUSTER_KEYS_FOR_CO_PARTITION().key(),
            "false",
            SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
            "false",
            SQLConf.AUTO_BROADCASTJOIN_THRESHOLD().key(),
            "-1",
            SparkSQLProperties.PRESERVE_DATA_GROUPING,
            "true");

    for (String vectorized : new String[] {"true", "false"}) {
      sql(
          "ALTER TABLE %s SET TBLPROPERTIES ('%s'='%s')",
          tableName, TableProperties.PARQUET_VECTORIZATION_ENABLED, vectorized);

      withSQLConf(
          spjConf,
          () -> {
            Dataset<Row> df =
                spark.sql(
                    String.format(
                        "SELECT * FROM %s a JOIN %s b ON a.dep = b.dep", tableName, tableName));
            df.collect();

            List<SparkPlan> scans =
                seqAsJavaListConverter(df.queryExecution().executedPlan().collectLeaves()).asJava();
            assertThat(scans).hasSize(2);
            for (SparkPlan scan : scans) {
              assertThat(scan.metrics().get("numSplits").get().value())
                  .as("numSplits with vectorization %s", vectorized)
                  .isEqualTo(5L);
              assertThat(scan.metrics().get("numDeletes").get().value())
                  .as("numDeletes with vectorization %s", vectorized)
                  .isEqualTo(5L);
            }
          });
    }
  }
}
