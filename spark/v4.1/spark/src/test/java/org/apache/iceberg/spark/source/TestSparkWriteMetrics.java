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

import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.spark.source.metrics.AddedDataFiles;
import org.apache.iceberg.spark.source.metrics.AddedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.AddedEqualityDeleteFiles;
import org.apache.iceberg.spark.source.metrics.AddedEqualityDeletes;
import org.apache.iceberg.spark.source.metrics.AddedFileSizeInBytes;
import org.apache.iceberg.spark.source.metrics.AddedPositionalDeleteFiles;
import org.apache.iceberg.spark.source.metrics.AddedPositionalDeletes;
import org.apache.iceberg.spark.source.metrics.AddedRecords;
import org.apache.iceberg.spark.source.metrics.RemovedDataFiles;
import org.apache.iceberg.spark.source.metrics.RemovedDeleteFiles;
import org.apache.iceberg.spark.source.metrics.RemovedEqualityDeleteFiles;
import org.apache.iceberg.spark.source.metrics.RemovedEqualityDeletes;
import org.apache.iceberg.spark.source.metrics.RemovedFileSizeInBytes;
import org.apache.iceberg.spark.source.metrics.RemovedPositionalDeleteFiles;
import org.apache.iceberg.spark.source.metrics.RemovedPositionalDeletes;
import org.apache.iceberg.spark.source.metrics.RemovedRecords;
import org.apache.iceberg.spark.source.metrics.TotalDataFiles;
import org.apache.iceberg.spark.source.metrics.TotalDeleteFiles;
import org.apache.iceberg.spark.source.metrics.TotalEqualityDeletes;
import org.apache.iceberg.spark.source.metrics.TotalFileSizeInBytes;
import org.apache.iceberg.spark.source.metrics.TotalPositionalDeletes;
import org.apache.iceberg.spark.source.metrics.TotalRecords;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.jdk.javaapi.CollectionConverters;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkWriteMetrics extends TestBaseWithCatalog {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void writeMetrics() throws NoSuchTableException {
    sql("CREATE TABLE %s (id BIGINT) USING iceberg", tableName);

    String insertSql = String.format("INSERT INTO %s SELECT id FROM range(1000)", tableName);
    Dataset<Row> result = spark.sql(insertSql);
    result.collect();

    SparkPlan plan = result.queryExecution().executedPlan();
    Map<String, SQLMetric> metricsMap = CollectionConverters.asJava(plan.metrics());

    // If we are at the root, check if we have the metrics.
    // Sometimes the plan structure is complex (e.g. AdaptiveSparkPlanExec).
    // We might want to find the specific write node.

    if (!metricsMap.containsKey(AddedDataFiles.NAME)) {
      // Attempt to find a node with these metrics
      metricsMap = findMetrics(plan, AddedDataFiles.NAME);
    }

    assertThat(metricsMap).isNotNull();

    assertThat(metricsMap)
        .hasEntrySatisfying(AddedDataFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(2));

    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedRecords.NAME, metric -> assertThat(metric.value()).isEqualTo(1000));

    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedFileSizeInBytes.NAME, metric -> assertThat(metric.value()).isGreaterThan(0));

    assertThat(metricsMap)
        .hasEntrySatisfying(TotalDataFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(2));

    assertThat(metricsMap)
        .hasEntrySatisfying(
            TotalRecords.NAME, metric -> assertThat(metric.value()).isEqualTo(1000));

    assertThat(metricsMap)
        .hasEntrySatisfying(
            TotalFileSizeInBytes.NAME, metric -> assertThat(metric.value()).isGreaterThan(0));

    // Verify other metrics are 0
    String[] zeroMetrics = {
      AddedDeleteFiles.NAME,
      AddedEqualityDeleteFiles.NAME,
      AddedPositionalDeleteFiles.NAME,
      AddedEqualityDeletes.NAME,
      AddedPositionalDeletes.NAME,
      RemovedDataFiles.NAME,
      RemovedDeleteFiles.NAME,
      RemovedEqualityDeleteFiles.NAME,
      RemovedPositionalDeleteFiles.NAME,
      RemovedEqualityDeletes.NAME,
      RemovedPositionalDeletes.NAME,
      RemovedRecords.NAME,
      RemovedFileSizeInBytes.NAME,
      TotalDeleteFiles.NAME,
      TotalEqualityDeletes.NAME,
      TotalPositionalDeletes.NAME
    };

    for (String metric : zeroMetrics) {
      assertThat(metricsMap)
          .hasEntrySatisfying(metric, m -> assertThat(m.value()).as(metric).isEqualTo(0));
    }
  }

  @TestTemplate
  public void deleteMetrics() throws NoSuchTableException {
    sql(
        "CREATE TABLE %s (id BIGINT) USING iceberg TBLPROPERTIES ('write.delete.mode'='merge-on-read')",
        tableName);

    spark.range(100).coalesce(1).writeTo(tableName).append();

    String deleteSql = String.format("DELETE FROM %s WHERE id = 1", tableName);
    Dataset<Row> result = spark.sql(deleteSql);
    result.collect();

    SparkPlan plan = result.queryExecution().executedPlan();

    Map<String, SQLMetric> metricsMap = findMetrics(plan, AddedPositionalDeleteFiles.NAME);

    assertThat(metricsMap).isNotNull();

    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedPositionalDeleteFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            RemovedDataFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedPositionalDeletes.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            TotalDeleteFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            TotalPositionalDeletes.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedDeleteFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            AddedFileSizeInBytes.NAME, metric -> assertThat(metric.value()).isGreaterThan(0));
    assertThat(metricsMap)
        .hasEntrySatisfying(TotalDataFiles.NAME, metric -> assertThat(metric.value()).isEqualTo(1));
    assertThat(metricsMap)
        .hasEntrySatisfying(TotalRecords.NAME, metric -> assertThat(metric.value()).isEqualTo(100));
    assertThat(metricsMap)
        .hasEntrySatisfying(
            TotalFileSizeInBytes.NAME, metric -> assertThat(metric.value()).isGreaterThan(0));

    // Verify other metrics are 0
    String[] zeroMetrics = {
      AddedDataFiles.NAME,
      AddedEqualityDeleteFiles.NAME,
      AddedEqualityDeletes.NAME,
      AddedRecords.NAME,
      RemovedDeleteFiles.NAME,
      RemovedEqualityDeleteFiles.NAME,
      RemovedPositionalDeleteFiles.NAME,
      RemovedEqualityDeletes.NAME,
      RemovedPositionalDeletes.NAME,
      RemovedRecords.NAME,
      RemovedFileSizeInBytes.NAME,
      TotalEqualityDeletes.NAME
    };

    for (String metric : zeroMetrics) {
      assertThat(metricsMap)
          .hasEntrySatisfying(metric, m -> assertThat(m.value()).as(metric).isEqualTo(0));
    }
  }

  private Map<String, SQLMetric> findMetrics(SparkPlan plan, String metricName) {
    Map<String, SQLMetric> metrics = CollectionConverters.asJava(plan.metrics());
    if (metrics.containsKey(metricName)) {
      return metrics;
    }

    for (SparkPlan child : CollectionConverters.asJava(plan.children())) {
      Map<String, SQLMetric> result = findMetrics(child, metricName);
      if (result != null) {
        return result;
      }
    }

    for (Object child : CollectionConverters.asJava(plan.innerChildren())) {
      if (child instanceof SparkPlan) {
        Map<String, SQLMetric> result = findMetrics((SparkPlan) child, metricName);
        if (result != null) {
          return result;
        }
      }
    }

    return null;
  }
}
