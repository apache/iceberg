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
import org.apache.spark.sql.catalyst.expressions.Coalesce
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.CASE_WHEN
import org.apache.spark.sql.catalyst.trees.TreePattern.IF
import org.apache.spark.sql.types.BooleanType

/**
 * A rule similar to SimplifyConditionalsInPredicate in Spark but applies to Iceberg row-level commands.
 */
object ExtendedSimplifyConditionalsInPredicate extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CASE_WHEN, IF)) {

    case d @ DeleteFromIcebergTable(_, Some(cond), _) =>
      d.copy(condition = Some(simplifyConditional(cond)))

    case u @ UpdateIcebergTable(_, _, Some(cond), _) =>
      u.copy(condition = Some(simplifyConditional(cond)))

    case m @ MergeIntoIcebergTable(_, _, mergeCond, matchedActions, notMatchedActions, _) =>
      m.copy(
        mergeCondition = simplifyConditional(mergeCond),
        matchedActions = simplifyConditional(matchedActions),
        notMatchedActions = simplifyConditional(notMatchedActions))
  }

  private def simplifyConditional(e: Expression): Expression = e match {
    case And(left, right) => And(simplifyConditional(left), simplifyConditional(right))
    case Or(left, right) => Or(simplifyConditional(left), simplifyConditional(right))
    case If(cond, trueValue, FalseLiteral) => And(cond, trueValue)
    case If(cond, trueValue, TrueLiteral) => Or(Not(Coalesce(Seq(cond, FalseLiteral))), trueValue)
    case If(cond, FalseLiteral, falseValue) =>
      And(Not(Coalesce(Seq(cond, FalseLiteral))), falseValue)
    case If(cond, TrueLiteral, falseValue) => Or(cond, falseValue)
    case CaseWhen(Seq((cond, trueValue)),
    Some(FalseLiteral) | Some(Literal(null, BooleanType)) | None) =>
      And(cond, trueValue)
    case CaseWhen(Seq((cond, trueValue)), Some(TrueLiteral)) =>
      Or(Not(Coalesce(Seq(cond, FalseLiteral))), trueValue)
    case CaseWhen(Seq((cond, FalseLiteral)), Some(elseValue)) =>
      And(Not(Coalesce(Seq(cond, FalseLiteral))), elseValue)
    case CaseWhen(Seq((cond, TrueLiteral)), Some(elseValue)) =>
      Or(cond, elseValue)
    case e if e.dataType == BooleanType => e
    case e =>
      assert(e.dataType != BooleanType,
        "Expected a Boolean type expression in ExtendedSimplifyConditionalsInPredicate, " +
        s"but got the type `${e.dataType.catalogString}` in `${e.sql}`.")
      e
  }

  private def simplifyConditional(mergeActions: Seq[MergeAction]): Seq[MergeAction] = {
    mergeActions.map {
      case u @ UpdateAction(Some(cond), _) => u.copy(condition = Some(simplifyConditional(cond)))
      case u @ UpdateStarAction(Some(cond)) => u.copy(condition = Some(simplifyConditional(cond)))
      case d @ DeleteAction(Some(cond)) => d.copy(condition = Some(simplifyConditional(cond)))
      case i @ InsertAction(Some(cond), _) => i.copy(condition = Some(simplifyConditional(cond)))
      case i @ InsertStarAction(Some(cond)) => i.copy(condition = Some(simplifyConditional(cond)))
      case other => other
    }
  }
}
