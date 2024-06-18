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
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.junit.After;
import org.junit.Test;
import scala.collection.JavaConverters;

public class TestSparkReadMetrics extends SparkTestBaseWithCatalog {

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
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
    assertThat(metricsMap.get("totalPlanningDuration").value()).isNotEqualTo(0);

    // data manifests
    assertThat(metricsMap.get("totalDataManifest").value()).isEqualTo(2);
    assertThat(metricsMap.get("scannedDataManifests").value()).isEqualTo(2);
    assertThat(metricsMap.get("skippedDataManifests").value()).isEqualTo(0);

    // data files
    assertThat(metricsMap.get("resultDataFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDataFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("totalDataFileSize").value()).isNotEqualTo(0);

    // delete manifests
    assertThat(metricsMap.get("totalDeleteManifests").value()).isEqualTo(0);
    assertThat(metricsMap.get("scannedDeleteManifests").value()).isEqualTo(0);
    assertThat(metricsMap.get("skippedDeleteManifests").value()).isEqualTo(0);

    // delete files
    assertThat(metricsMap.get("totalDeleteFileSize").value()).isEqualTo(0);
    assertThat(metricsMap.get("resultDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("equalityDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("indexedDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("positionalDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("skippedDeleteFiles").value()).isEqualTo(0);
  }

  @Test
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
    assertThat(metricsMap.get("totalPlanningDuration").value()).isNotEqualTo(0);

    // data manifests
    assertThat(metricsMap.get("totalDataManifest").value()).isEqualTo(2);
    assertThat(metricsMap.get("scannedDataManifests").value()).isEqualTo(2);
    assertThat(metricsMap.get("skippedDataManifests").value()).isEqualTo(0);

    // data files
    assertThat(metricsMap.get("resultDataFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDataFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("totalDataFileSize").value()).isNotEqualTo(0);

    // delete manifests
    assertThat(metricsMap.get("totalDeleteManifests").value()).isEqualTo(0);
    assertThat(metricsMap.get("scannedDeleteManifests").value()).isEqualTo(0);
    assertThat(metricsMap.get("skippedDeleteManifests").value()).isEqualTo(0);

    // delete files
    assertThat(metricsMap.get("totalDeleteFileSize").value()).isEqualTo(0);
    assertThat(metricsMap.get("resultDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("equalityDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("indexedDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("positionalDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("skippedDeleteFiles").value()).isEqualTo(0);
  }

  @Test
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
    assertThat(metricsMap.get("totalPlanningDuration").value()).isNotEqualTo(0);

    // data manifests
    assertThat(metricsMap.get("totalDataManifest").value()).isEqualTo(1);
    assertThat(metricsMap.get("scannedDataManifests").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDataManifests").value()).isEqualTo(0);

    // data files
    assertThat(metricsMap.get("resultDataFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDataFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("totalDataFileSize").value()).isNotEqualTo(0);

    // delete manifests
    assertThat(metricsMap.get("totalDeleteManifests").value()).isEqualTo(1);
    assertThat(metricsMap.get("scannedDeleteManifests").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDeleteManifests").value()).isEqualTo(0);

    // delete files
    assertThat(metricsMap.get("totalDeleteFileSize").value()).isNotEqualTo(0);
    assertThat(metricsMap.get("resultDeleteFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("equalityDeleteFiles").value()).isEqualTo(0);
    assertThat(metricsMap.get("indexedDeleteFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("positionalDeleteFiles").value()).isEqualTo(1);
    assertThat(metricsMap.get("skippedDeleteFiles").value()).isEqualTo(0);
  }
}
