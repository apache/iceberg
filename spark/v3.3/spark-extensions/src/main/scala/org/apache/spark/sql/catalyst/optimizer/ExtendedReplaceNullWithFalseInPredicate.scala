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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.CaseWhen
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.InsertStarAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeAction
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateStarAction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.INSET
import org.apache.spark.sql.catalyst.trees.TreePattern.NULL_LITERAL
import org.apache.spark.sql.catalyst.trees.TreePattern.TRUE_OR_FALSE_LITERAL
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.util.Utils

/**
 * A rule similar to ReplaceNullWithFalseInPredicate in Spark but applies to Iceberg row-level commands.
 */
object ExtendedReplaceNullWithFalseInPredicate extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(NULL_LITERAL, TRUE_OR_FALSE_LITERAL, INSET)) {

    case d @ DeleteFromIcebergTable(_, Some(cond), _) =>
      d.copy(condition = Some(replaceNullWithFalse(cond)))

    case u @ UpdateIcebergTable(_, _, Some(cond), _) =>
      u.copy(condition = Some(replaceNullWithFalse(cond)))

    case m @ MergeIntoIcebergTable(_, _, mergeCond, matchedActions, notMatchedActions, _) =>
      m.copy(
        mergeCondition = replaceNullWithFalse(mergeCond),
        matchedActions = replaceNullWithFalse(matchedActions),
        notMatchedActions = replaceNullWithFalse(notMatchedActions))
  }

  /**
   * Recursively traverse the Boolean-type expression to replace
   * `Literal(null, BooleanType)` with `FalseLiteral`, if possible.
   *
   * Note that `transformExpressionsDown` can not be used here as we must stop as soon as we hit
   * an expression that is not [[CaseWhen]], [[If]], [[And]], [[Or]] or
   * `Literal(null, BooleanType)`.
   */
  private def replaceNullWithFalse(e: Expression): Expression = e match {
    case Literal(null, BooleanType) =>
      FalseLiteral
    // In SQL, the `Not(IN)` expression evaluates as follows:
    // `NULL not in (1)` -> NULL
    // `NULL not in (1, NULL)` -> NULL
    // `1 not in (1, NULL)` -> false
    // `1 not in (2, NULL)` -> NULL
    // In predicate, NULL is equal to false, so we can simplify them to false directly.
    case Not(In(value, list)) if (value +: list).exists(isNullLiteral) =>
      FalseLiteral
    case Not(InSet(value, list)) if isNullLiteral(value) || list.contains(null) =>
      FalseLiteral

    case And(left, right) =>
      And(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case Or(left, right) =>
      Or(replaceNullWithFalse(left), replaceNullWithFalse(right))
    case cw: CaseWhen if cw.dataType == BooleanType =>
      val newBranches = cw.branches.map { case (cond, value) =>
        replaceNullWithFalse(cond) -> replaceNullWithFalse(value)
      }
      val newElseValue = cw.elseValue.map(replaceNullWithFalse).getOrElse(FalseLiteral)
      CaseWhen(newBranches, newElseValue)
    case i @ If(pred, trueVal, falseVal) if i.dataType == BooleanType =>
      If(replaceNullWithFalse(pred), replaceNullWithFalse(trueVal), replaceNullWithFalse(falseVal))
    case e if e.dataType == BooleanType =>
      e
    case e =>
      val message = "Expected a Boolean type expression in replaceNullWithFalse, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`."
      if (Utils.isTesting) {
        throw new IllegalArgumentException(message)
      } else {
        logWarning(message)
        e
      }
  }

  private def isNullLiteral(e: Expression): Boolean = e match {
    case Literal(null, _) => true
    case _ => false
  }

  private def replaceNullWithFalse(mergeActions: Seq[MergeAction]): Seq[MergeAction] = {
    mergeActions.map {
      case u @ UpdateAction(Some(cond), _) => u.copy(condition = Some(replaceNullWithFalse(cond)))
      case u @ UpdateStarAction(Some(cond)) => u.copy(condition = Some(replaceNullWithFalse(cond)))
      case d @ DeleteAction(Some(cond)) => d.copy(condition = Some(replaceNullWithFalse(cond)))
      case i @ InsertAction(Some(cond), _) => i.copy(condition = Some(replaceNullWithFalse(cond)))
      case i @ InsertStarAction(Some(cond)) => i.copy(condition = Some(replaceNullWithFalse(cond)))
      case other => other
    }
  }
}
