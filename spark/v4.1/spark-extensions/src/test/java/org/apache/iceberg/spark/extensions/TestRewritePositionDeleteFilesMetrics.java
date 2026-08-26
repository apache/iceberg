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
package org.apache.iceberg.spark.extensions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.spark.source.metrics.WriteCloseDuration;
import org.apache.iceberg.spark.source.metrics.WriteDuration;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Covers the write timing metrics on the position deletes rewrite path, which is driven by a
 * procedure rather than a plain SQL write.
 */
@ExtendWith(ParameterizedTestExtension.class)
public class TestRewritePositionDeleteFilesMetrics extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void writeDurationForDVRewrite() throws Exception {
    // v3 tables rewrite deletes through DVWriter rather than DeleteWriter
    createTableWithDeletes(3);

    sql(
        "CALL %s.system.rewrite_position_delete_files("
            + "table => '%s', options => map('rewrite-all','true'))",
        catalogName, tableIdent);

    assertThat(maxMetricValue(new WriteDuration().description()))
        .as("write duration should be reported by the DV rewrite")
        .isGreaterThan(0);
    assertThat(maxMetricValue(new WriteCloseDuration().description()))
        .as("write close duration should be reported by the DV rewrite")
        .isGreaterThan(0);
  }

  private void createTableWithDeletes(int formatVersion) throws Exception {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='%d', 'write.delete.mode'='merge-on-read',"
            + " 'write.delete.granularity'='partition')",
        tableName, formatVersion);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id = 1", tableName);
    sql("DELETE FROM %s WHERE id = 2", tableName);
  }

  @TestTemplate
  public void writeDurationForPositionDeletesRewrite() throws Exception {
    createTableWithDeletes(2);

    sql(
        "CALL %s.system.rewrite_position_delete_files("
            + "table => '%s', options => map('rewrite-all','true'))",
        catalogName, tableIdent);

    // the rewrite runs as its own SQL execution, so look for the write node across executions
    assertThat(maxMetricValue(new WriteDuration().description()))
        .as("write duration should be reported by the position deletes rewrite")
        .isGreaterThan(0);
    assertThat(maxMetricValue(new WriteCloseDuration().description()))
        .as("write close duration should be reported by the position deletes rewrite")
        .isGreaterThan(0);
  }

  /** Looks up a metric by the description Spark stores in the UI, across all executions. */
  private long maxMetricValue(String metricDescription) {
    SQLAppStatusStore statusStore = spark.sharedState().statusStore();
    long max = 0L;

    for (SQLExecutionUIData execution : CollectionConverters.asJava(statusStore.executionsList())) {
      Map<Object, String> metricValues =
          CollectionConverters.asJava(
              statusStore.execution(execution.executionId()).get().metricValues());

      for (SQLPlanMetric metric : CollectionConverters.asJava(execution.metrics())) {
        if (metric.name().equals(metricDescription)) {
          String value = metricValues.get(metric.accumulatorId());
          if (value != null) {
            max = Math.max(max, parseNanos(value));
          }
        }
      }
    }

    return max;
  }

  /** Durations are stored formatted (e.g. "1.5 s"), so recover a comparable magnitude. */
  private long parseNanos(String formatted) {
    String[] parts = formatted.trim().split(" ");
    double value = Double.parseDouble(parts[0].replace(",", ""));
    switch (parts[1]) {
      case "ns":
        return (long) value;
      case "us":
        return (long) (value * 1_000);
      case "ms":
        return (long) (value * 1_000_000);
      case "s":
        return (long) (value * 1_000_000_000L);
      default:
        throw new IllegalArgumentException("Unknown duration unit: " + formatted);
    }
  }
}
