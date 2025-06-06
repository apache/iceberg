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

import org.apache.iceberg.metrics.CounterResult
import org.apache.iceberg.metrics.InMemoryMetricsReporter
import org.apache.iceberg.spark.source.metrics.AddedDataFiles
import org.apache.iceberg.spark.source.metrics.AddedDeleteFiles
import org.apache.iceberg.spark.source.metrics.AddedEqualityDeleteFiles
import org.apache.iceberg.spark.source.metrics.AddedEqualityDeletes
import org.apache.iceberg.spark.source.metrics.AddedFileSizeInBytes
import org.apache.iceberg.spark.source.metrics.AddedPositionalDeleteFiles
import org.apache.iceberg.spark.source.metrics.AddedPositionalDeletes
import org.apache.iceberg.spark.source.metrics.AddedRecords
import org.apache.iceberg.spark.source.metrics.RemovedDataFiles
import org.apache.iceberg.spark.source.metrics.RemovedDeleteFiles
import org.apache.iceberg.spark.source.metrics.RemovedEqualityDeleteFiles
import org.apache.iceberg.spark.source.metrics.RemovedEqualityDeletes
import org.apache.iceberg.spark.source.metrics.RemovedFileSizeInBytes
import org.apache.iceberg.spark.source.metrics.RemovedPositionalDeleteFiles
import org.apache.iceberg.spark.source.metrics.RemovedPositionalDeletes
import org.apache.iceberg.spark.source.metrics.RemovedRecords
import org.apache.iceberg.spark.source.metrics.TotalDataFiles
import org.apache.iceberg.spark.source.metrics.TotalDeleteFiles
import org.apache.iceberg.spark.source.metrics.TotalEqualityDeletes
import org.apache.iceberg.spark.source.metrics.TotalFileSizeInBytes
import org.apache.iceberg.spark.source.metrics.TotalPositionalDeletes
import org.apache.iceberg.spark.source.metrics.TotalRecords
import org.apache.spark.SparkContext
import org.apache.spark.sql.connector.metric.CustomMetric
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import scala.collection.mutable

object MetricsUtils {

  def supportedCustomMetrics(): Array[CustomMetric] = {
    Array(
      new AddedDataFiles,
      new AddedDeleteFiles,
      new AddedEqualityDeletes,
      new AddedEqualityDeleteFiles,
      new AddedFileSizeInBytes,
      new AddedPositionalDeletes,
      new AddedPositionalDeleteFiles,
      new AddedRecords,
      new RemovedDataFiles,
      new RemovedDeleteFiles,
      new RemovedRecords,
      new RemovedEqualityDeleteFiles,
      new RemovedEqualityDeletes,
      new RemovedFileSizeInBytes,
      new RemovedPositionalDeleteFiles,
      new RemovedPositionalDeletes,
      new TotalDataFiles,
      new TotalDeleteFiles,
      new TotalEqualityDeletes,
      new TotalFileSizeInBytes,
      new TotalPositionalDeletes,
      new TotalRecords
    )
  }

  def postWriteMetrics(metricsReporter: InMemoryMetricsReporter, sparkContext: SparkContext): Unit = {
    if (metricsReporter != null) {
      val commitReport = metricsReporter.commitReport
      if (commitReport != null) {
        val metricsResult = commitReport.commitMetrics
        val metricValues = mutable.Map.empty[CustomMetric, Long]
        addMetricValue(new AddedDataFiles, metricsResult.addedDataFiles, metricValues)
        addMetricValue(new AddedDeleteFiles, metricsResult.addedDeleteFiles, metricValues)
        addMetricValue(new AddedEqualityDeletes, metricsResult.addedEqualityDeletes, metricValues)
        addMetricValue(new AddedEqualityDeleteFiles, metricsResult.addedEqualityDeleteFiles, metricValues)
        addMetricValue(new AddedFileSizeInBytes, metricsResult.addedFilesSizeInBytes, metricValues)
        addMetricValue(new AddedPositionalDeletes, metricsResult.addedPositionalDeletes, metricValues)
        addMetricValue(new AddedPositionalDeleteFiles, metricsResult.addedPositionalDeleteFiles, metricValues)
        addMetricValue(new AddedRecords, metricsResult.addedRecords, metricValues)
        addMetricValue(new RemovedDataFiles, metricsResult.removedDataFiles, metricValues)
        addMetricValue(new RemovedDeleteFiles, metricsResult.removedDeleteFiles, metricValues)
        addMetricValue(new RemovedRecords, metricsResult.removedRecords, metricValues)
        addMetricValue(new RemovedEqualityDeleteFiles, metricsResult.removedEqualityDeleteFiles, metricValues)
        addMetricValue(new RemovedEqualityDeletes, metricsResult.removedEqualityDeletes, metricValues)
        addMetricValue(new RemovedFileSizeInBytes, metricsResult.removedFilesSizeInBytes, metricValues)
        addMetricValue(new RemovedPositionalDeleteFiles, metricsResult.removedPositionalDeleteFiles, metricValues)
        addMetricValue(new RemovedPositionalDeletes, metricsResult.removedPositionalDeletes, metricValues)
        addMetricValue(new TotalDataFiles, metricsResult.totalDataFiles, metricValues)
        addMetricValue(new TotalDeleteFiles, metricsResult.totalDeleteFiles, metricValues)
        addMetricValue(new TotalEqualityDeletes, metricsResult.totalEqualityDeletes, metricValues)
        addMetricValue(new TotalFileSizeInBytes, metricsResult.totalFilesSizeInBytes, metricValues)
        addMetricValue(new TotalPositionalDeletes, metricsResult.totalPositionalDeletes, metricValues)
        addMetricValue(new TotalRecords, metricsResult.totalRecords, metricValues)
        postDriverMetrics(sparkContext, metricValues.toMap)
      }
    }
  }

  private def addMetricValue(metric: CustomMetric, result: CounterResult,
      metricValues: mutable.Map[CustomMetric, Long]): Unit = {
    if (result != null) {
      metricValues.put(metric, result.value)
    }
  }

  private def postDriverMetrics(sparkContext: SparkContext,
       metricValues: Map[CustomMetric, Long]): Unit = {
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val metrics = SQLExecution.getQueryExecution(executionId.toLong).executedPlan.metrics
    val sqlMetrics = metricValues.map { case (metric, value) =>
      val sqlMetric = metrics(metric.name)
      sqlMetric.set(value)
      sqlMetric
    }.toSeq
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, sqlMetrics)
  }
}
