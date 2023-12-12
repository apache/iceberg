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

package org.apache.spark.sql.catalyst.plans.logical

import io.openlineage.spark.builtin.scala.v1.ColumnLevelLineageNode
import io.openlineage.spark.builtin.scala.v1.DatasetFieldLineage
import io.openlineage.spark.builtin.scala.v1.ExpressionDependency
import io.openlineage.spark.builtin.scala.v1.ExpressionDependencyWithDelegate
import io.openlineage.spark.builtin.scala.v1.InputDatasetFieldFromDelegate
import io.openlineage.spark.builtin.scala.v1.OlExprId
import io.openlineage.spark.builtin.scala.v1.OpenLineageContext
import io.openlineage.spark.builtin.scala.v1.OutputDatasetField
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.truncatedString
import scala.collection.mutable.ListBuffer

case class MergeRows(
    isSourceRowPresent: Expression,
    isTargetRowPresent: Expression,
    matchedConditions: Seq[Expression],
    matchedOutputs: Seq[Seq[Seq[Expression]]],
    notMatchedConditions: Seq[Expression],
    notMatchedOutputs: Seq[Seq[Expression]],
    targetOutput: Seq[Expression],
    performCardinalityCheck: Boolean,
    emitNotMatchedTargetRows: Boolean,
    output: Seq[Attribute],
    child: LogicalPlan) extends UnaryNode with ColumnLevelLineageNode {

  require(targetOutput.nonEmpty || !emitNotMatchedTargetRows)

  override lazy val producedAttributes: AttributeSet = {
    AttributeSet(output.filterNot(attr => inputSet.contains(attr)))
  }

  override lazy val references: AttributeSet = child.outputSet

  override def simpleString(maxFields: Int): String = {
    s"MergeRows${truncatedString(output, "[", ", ", "]", maxFields)}"
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }

  override def columnLevelLineageDependencies(context: OpenLineageContext): List[ExpressionDependency] = {
    val deps: ListBuffer[ExpressionDependency] = ListBuffer()

    output.zipWithIndex.foreach {
      case (attr: Attribute, index: Int) =>
        notMatchedOutputs
          .toStream
          .filter(exprs => exprs.size > index)
          .map(exprs => exprs(index))
          .foreach(expr => deps += ExpressionDependencyWithDelegate(OlExprId(attr.exprId.id), expr))
        matchedOutputs
          .foreach {
            matched =>
              matched
                .toStream
                .filter(exprs => exprs.size > index)
                .map(exprs => exprs(index))
                .foreach(expr => deps += ExpressionDependencyWithDelegate(OlExprId(attr.exprId.id), expr))
          }
    }

    deps.toList
  }

  override def columnLevelLineageInputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
    List(InputDatasetFieldFromDelegate(child))
  }

  override def columnLevelLineageOutputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
    output.map(a => OutputDatasetField(a.name, OlExprId(a.exprId.id))).toList
  }
}
