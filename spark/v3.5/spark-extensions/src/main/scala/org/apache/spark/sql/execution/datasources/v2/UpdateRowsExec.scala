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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.UnaryExecNode

case class UpdateRowsExec(
    deleteOutput: Seq[Expression],
    insertOutput: Seq[Expression],
    output: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode {

  @transient override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  override def simpleString(maxFields: Int): String = {
    s"UpdateRowsExec${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions(processPartition)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }

  private def processPartition(rowIterator: Iterator[InternalRow]): Iterator[InternalRow] = {
    val deleteProj = createProjection(deleteOutput)
    val insertProj = createProjection(insertOutput)
    new UpdateAsDeleteAndInsertRowIterator(rowIterator, deleteProj, insertProj)
  }

  private def createProjection(exprs: Seq[Expression]): UnsafeProjection = {
    UnsafeProjection.create(exprs, child.output)
  }

  class UpdateAsDeleteAndInsertRowIterator(
      private val inputRows: Iterator[InternalRow],
      private val deleteProj: UnsafeProjection,
      private val insertProj: UnsafeProjection)
    extends Iterator[InternalRow] {

    var cachedInsertRow: InternalRow = _

    override def hasNext: Boolean = cachedInsertRow != null || inputRows.hasNext

    override def next(): InternalRow = {
      if (cachedInsertRow != null) {
        val insertRow = cachedInsertRow
        cachedInsertRow = null
        return insertRow
      }

      val row = inputRows.next()
      cachedInsertRow = insertProj.apply(row)
      deleteProj.apply(row)
    }
  }
}
