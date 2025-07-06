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

import java.time.Duration;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.awaitility.Awaitility;
import scala.Option;

public class SparkSQLExecutionHelper {

  private SparkSQLExecutionHelper() {}

  /**
   * Finds the value of a specified metric for the last SQL query that was executed. Metric values
   * are stored in the `SQLAppStatusStore` as strings.
   *
   * @param spark SparkSession used to run the SQL query
   * @param metricName name of the metric
   * @return value of the metric
   */
  public static String lastExecutedMetricValue(SparkSession spark, String metricName) {
    SQLAppStatusStore statusStore = spark.sharedState().statusStore();
    SQLExecutionUIData lastExecution = statusStore.executionsList().last();
    Option<SQLPlanMetric> sqlPlanMetric =
        lastExecution.metrics().find(metric -> metric.name().equals(metricName));
    assertThat(sqlPlanMetric.isDefined())
        .as(String.format("Metric '%s' not found in last execution", metricName))
        .isTrue();
    long metricId = sqlPlanMetric.get().accumulatorId();

    // Refresh metricValues, they will remain null until the execution is complete and metrics are
    // aggregated
    Awaitility.await()
        .atMost(Duration.ofSeconds(3))
        .pollInterval(Duration.ofMillis(100))
        .untilAsserted(
            () -> assertThat(statusStore.execution(lastExecution.executionId()).get()).isNotNull());

    SQLExecutionUIData exec = statusStore.execution(lastExecution.executionId()).get();

    assertThat(exec.metricValues()).as("Metric values were not finalized").isNotNull();
    String metricValue = exec.metricValues().get(metricId).getOrElse(null);
    assertThat(metricValue)
        .as(String.format("Metric '%s' was not finalized", metricName))
        .isNotNull();
    return metricValue;
  }
}
