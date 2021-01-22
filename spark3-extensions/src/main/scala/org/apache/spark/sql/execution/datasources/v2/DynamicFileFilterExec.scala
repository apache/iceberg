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

import collection.JavaConverters._
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.catalyst.utils.SetAccumulator
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class DynamicFileFilterExecBase(
    scanExec: SparkPlan,
    fileFilterExec: SparkPlan) extends BinaryExecNode {

  @transient
  override lazy val references: AttributeSet = AttributeSet(fileFilterExec.output)

  override def left: SparkPlan = scanExec
  override def right: SparkPlan = fileFilterExec
  override def output: Seq[Attribute] = scanExec.output
  override def outputPartitioning: physical.Partitioning = scanExec.outputPartitioning
  override def outputOrdering: Seq[SortOrder] = scanExec.outputOrdering
  override def supportsColumnar: Boolean = scanExec.supportsColumnar

  override protected def doExecute(): RDD[InternalRow] = scanExec.execute()
  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = scanExec.executeColumnar()

  override def simpleString(maxFields: Int): String = {
    s"DynamicFileFilterExec${truncatedString(output, "[", ", ", "]", maxFields)}"
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
    filterable.filterFiles(matchedFileLocations.toSet.asJava)
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
    filterable.filterFiles(matchedFileLocations)
  }
}
