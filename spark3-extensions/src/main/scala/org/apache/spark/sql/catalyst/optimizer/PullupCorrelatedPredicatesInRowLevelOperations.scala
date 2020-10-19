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

import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{DeleteFromTable, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

// a temp solution until PullupCorrelatedPredicates handles row-level operations in Spark
object PullupCorrelatedPredicatesInRowLevelOperations extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case d @ DeleteFromTable(table, Some(cond)) if SubqueryExpression.hasSubquery(cond) =>
      // Spark pulls up correlated predicates only for UnaryNodes
      // DeleteFromTable does not extend UnaryNode so it is ignored in that rule
      // We have this workaround until it is fixed in Spark
      val filter = Filter(cond, table)
      val transformedFilter = PullupCorrelatedPredicates.apply(filter)
      val transformedCond = transformedFilter.asInstanceOf[Filter].condition
      d.copy(condition = Some(transformedCond))
  }
}
