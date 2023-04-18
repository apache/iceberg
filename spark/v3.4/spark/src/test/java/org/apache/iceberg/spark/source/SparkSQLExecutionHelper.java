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

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ui.SQLAppStatusStore;
import org.apache.spark.sql.execution.ui.SQLExecutionUIData;
import org.apache.spark.sql.execution.ui.SQLPlanMetric;
import org.junit.Assert;
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
    Assert.assertTrue(
        String.format("Metric '%s' not found in last execution", metricName),
        sqlPlanMetric.isDefined());
    long metricId = sqlPlanMetric.get().accumulatorId();

    // Refresh metricValues, they will remain null until the execution is complete and metrics are
    // aggregated
    int attempts = 3;
    while (lastExecution.metricValues() == null && attempts > 0) {
      try {
        Thread.sleep(100);
        attempts--;
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      lastExecution = statusStore.execution(lastExecution.executionId()).get();
    }

    Assert.assertNotNull("Metric values were not finalized", lastExecution.metricValues());
    String metricValue = lastExecution.metricValues().get(metricId).getOrElse(null);
    Assert.assertNotNull(String.format("Metric '%s' was not finalized", metricName), metricValue);
    return metricValue;
  }
}
