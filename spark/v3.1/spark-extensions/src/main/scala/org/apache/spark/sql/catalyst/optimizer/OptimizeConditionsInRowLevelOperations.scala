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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

// we have to optimize expressions used in delete/update before we can rewrite row-level operations
// otherwise, we will have to deal with redundant casts and will not detect noop deletes
// it is a temp solution since we cannot inject rewrite of row-level ops after operator optimizations
object OptimizeConditionsInRowLevelOperations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeleteFromTable(table, cond)
        if !SubqueryExpression.hasSubquery(cond.getOrElse(Literal.TrueLiteral)) && isIcebergRelation(table) =>
      val optimizedCond = optimizeCondition(cond.getOrElse(Literal.TrueLiteral), table)
      d.copy(condition = Some(optimizedCond))
    case u @ UpdateTable(table, _, cond)
        if !SubqueryExpression.hasSubquery(cond.getOrElse(Literal.TrueLiteral)) && isIcebergRelation(table) =>
      val optimizedCond = optimizeCondition(cond.getOrElse(Literal.TrueLiteral), table)
      u.copy(condition = Some(optimizedCond))
  }

  private def optimizeCondition(cond: Expression, table: LogicalPlan): Expression = {
    val optimizer = SparkSession.active.sessionState.optimizer
    optimizer.execute(Filter(cond, table)) match {
      case Filter(optimizedCondition, _) => optimizedCondition
      case _: LocalRelation => Literal.FalseLiteral
      case _: DataSourceV2ScanRelation => Literal.TrueLiteral
      case _ => cond
    }
  }
}
