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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.catalyst.utils.SetAccumulator
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.vectorized.ColumnarBatch
import scala.collection.JavaConverters._

abstract class DynamicFileFilterExecBase(
    scanExec: SparkPlan,
    fileFilterExec: SparkPlan) extends BinaryExecNode {

  override lazy val metrics = Map(
    "candidateFiles" -> SQLMetrics.createMetric(sparkContext, "candidate files"),
    "matchingFiles" -> SQLMetrics.createMetric(sparkContext, "matching files"))

  @transient
  override lazy val references: AttributeSet = AttributeSet(fileFilterExec.output)

  override def left: SparkPlan = scanExec
  override def right: SparkPlan = fileFilterExec
  override def output: Seq[Attribute] = scanExec.output
  override def outputPartitioning: physical.Partitioning = scanExec.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = scanExec.outputOrdering
  override def supportsColumnar: Boolean = scanExec.supportsColumnar

  /*
  If both target and source have the same partitioning we can have a problem here if our filter exec actually
  changes the output partitioning of the node. Currently this can only occur in the SinglePartition distribution is
  in use which only happens if both the target and source have a single partition, but if it does we have the potential
  of eliminating the only partition in the target. If there are no partitions in the target then we will throw an
  exception because the partitioning was assumed to be the same 1 partition in source and target. We fix this by making
  sure that we always return at least 1 empty partition, in the future we may need to handle more complicated
  partitioner scenarios.
   */

  override protected def doExecute(): RDD[InternalRow] = {
    val result = scanExec.execute()
    if (result.partitions.length == 0) {
      sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      result
    }
  }
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val result = scanExec.executeColumnar()
    if (result.partitions.length == 0) {
      sparkContext.parallelize(Array.empty[ColumnarBatch], 1)
    } else {
      result
    }
  }

  override def simpleString(maxFields: Int): String = {
    s"DynamicFileFilterExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  def postFileFilterMetric(candidateFiles: Int, matchingFiles: Int): Unit = {
    longMetric("candidateFiles").set(candidateFiles)
    longMetric("matchingFiles").set(matchingFiles)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
  }
}

case class DynamicFileFilterExec(
    scanExec: SparkPlan,
    fileFilterExec: SparkPlan,
    @transient filterable: SupportsFileFilter)
  extends DynamicFileFilterExecBase(scanExec, fileFilterExec) {

  override protected def doPrepare(): Unit = {
    val rows = fileFilterExec.executeCollect()
    val matchedFileLocations = rows.map(_.getString(0))
    val metric = filterable.filterFiles(matchedFileLocations.toSet.asJava)
    postFileFilterMetric(metric.candidateFiles(), metric.matchingFiles())
  }
}

case class DynamicFileFilterWithCardinalityCheckExec(
    scanExec: SparkPlan,
    fileFilterExec: SparkPlan,
    @transient filterable: SupportsFileFilter,
    filesAccumulator: SetAccumulator[String])
  extends DynamicFileFilterExecBase(scanExec, fileFilterExec)  {

  override protected def doPrepare(): Unit = {
    val rows = fileFilterExec.executeCollect()
    if (rows.length > 0) {
      throw new SparkException(
        "The ON search condition of the MERGE statement matched a single row from " +
        "the target table with multiple rows of the source table. This could result " +
        "in the target row being operated on more than once with an update or delete operation " +
        "and is not allowed.")
    }
    val matchedFileLocations = filesAccumulator.value
    val metric = filterable.filterFiles(matchedFileLocations)
    postFileFilterMetric(metric.candidateFiles(), metric.matchingFiles())
  }
}
