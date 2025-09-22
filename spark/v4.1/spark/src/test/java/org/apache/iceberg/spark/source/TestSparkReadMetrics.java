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

import static org.assertj.core.api.Assertions.assertThat;
import static scala.collection.JavaConverters.seqAsJavaListConverter;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
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
