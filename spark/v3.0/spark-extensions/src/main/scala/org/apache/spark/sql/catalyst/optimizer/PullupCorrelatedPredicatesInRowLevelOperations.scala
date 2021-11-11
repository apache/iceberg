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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation

// a temp solution until PullupCorrelatedPredicates handles row-level operations in Spark
object PullupCorrelatedPredicatesInRowLevelOperations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeleteFromTable(table, Some(cond)) if SubqueryExpression.hasSubquery(cond) && isIcebergRelation(table) =>
      val transformedCond = transformCond(table, cond)
      d.copy(condition = Some(transformedCond))

    case u @ UpdateTable(table, _, Some(cond)) if SubqueryExpression.hasSubquery(cond) && isIcebergRelation(table) =>
      val transformedCond = transformCond(table, cond)
      u.copy(condition = Some(transformedCond))
  }

  // Spark pulls up correlated predicates only for UnaryNodes
  // DeleteFromTable and UpdateTable do not extend UnaryNode so they are ignored in that rule
  // We have this workaround until it is fixed in Spark
  private def transformCond(table: LogicalPlan, cond: Expression): Expression = {
    val filter = Filter(cond, table)
    val transformedFilter = PullupCorrelatedPredicates.apply(filter)
    transformedFilter.asInstanceOf[Filter].condition
  }
}
