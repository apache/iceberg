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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation

object MergeIntoTablePredicateCheck extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case merge: MergeIntoTable if isIcebergRelation(merge.targetTable) =>
        validateMergeIntoConditions(merge)
      case _ => // OK
    }
  }

  private def validateMergeIntoConditions(merge: MergeIntoTable): Unit = {
    checkMergeIntoCondition(merge.mergeCondition, "SEARCH")
    val actions = merge.matchedActions ++ merge.notMatchedActions
    actions.foreach {
      case DeleteAction(Some(cond)) => checkMergeIntoCondition(cond, "DELETE")
      case UpdateAction(Some(cond), _) => checkMergeIntoCondition(cond, "UPDATE")
      case InsertAction(Some(cond), _) => checkMergeIntoCondition(cond, "INSERT")
      case _ => // OK
    }
  }

  private def checkMergeIntoCondition(cond: Expression, condName: String): Unit = {
    // Spark already validates the conditions are deterministic and don't contain aggregates
    if (SubqueryExpression.hasSubquery(cond)) {
      throw new AnalysisException(
        s"Subqueries are not supported in conditions of MERGE operations. " +
        s"Found a subquery in the $condName condition: ${cond.sql}")
    }
  }
}
