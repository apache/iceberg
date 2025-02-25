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
package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import scala.collection.JavaConverters

object MetricsUtils {

  def postDriverMetrics(sparkContext: SparkContext, metricValues: java.util.Map[CustomMetric, Long]): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val metrics = SQLExecution.getQueryExecution(executionId.toLong).executedPlan.metrics
    val sqlMetrics = JavaConverters.mapAsScalaMap(metricValues).map { case (metric, value) =>
      val sqlMetric = metrics(metric.name)
      sqlMetric.set(value)
      sqlMetric
    }.toSeq
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, sqlMetrics)
  }
}
