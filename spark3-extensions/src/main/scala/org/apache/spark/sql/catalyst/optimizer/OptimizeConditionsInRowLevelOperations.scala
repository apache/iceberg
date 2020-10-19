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
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

// a temp solution until we move the rewrite of row level ops after operator optimization rules
object OptimizeConditionsInRowLevelOperations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // TODO: fix this?
    case d @ DeleteFromTable(table, cond) if !SubqueryExpression.hasSubquery(cond.getOrElse(Literal.TrueLiteral)) =>
      val optimizedCond = optimizeCondition(cond.getOrElse(Literal.TrueLiteral), table)
      d.copy(condition = Some(optimizedCond))
  }

  private def optimizeCondition(condition: Expression, targetTable: LogicalPlan): Expression = {
    val optimizer = SparkSession.active.sessionState.optimizer
    optimizer.execute(Filter(condition, targetTable)) match {
      case Filter(optimizedCondition, _) => optimizedCondition
      case _: LocalRelation => Literal.FalseLiteral
      case _: DataSourceV2ScanRelation => Literal.TrueLiteral
    }
  }
}
