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

import io.openlineage.spark.builtin.column.{ColumnLevelLineageFromNode, ColumnLevelLineageNode, OlExprId}
import io.openlineage.spark.builtin.common.OpenLineageContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.util.truncatedString

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

  override def columnLevelLineage(context: OpenLineageContext): ColumnLevelLineageFromNode = {
    val deps: Map[OlExprId, List[OlExprId]] = (notMatchedConditions zip output).map {
      case (expr: NamedExpression, output: Attribute) => {
        (OlExprId(expr.exprId.id), List(OlExprId(output.exprId.id)))
      }
    }.toMap.withDefaultValue(List())

    (matchedConditions zip output).foreach {
      case (expr: NamedExpression, output: Attribute) => {
        val key: OlExprId = OlExprId(expr.exprId.id);
        if (!deps.contains(key)) {
          deps.put(key, List())
        }
        deps(key).add(OlExprId(output.exprId.id))
      }
    }

    ColumnLevelLineageFromNode(deps, List(), List())
  }
}
