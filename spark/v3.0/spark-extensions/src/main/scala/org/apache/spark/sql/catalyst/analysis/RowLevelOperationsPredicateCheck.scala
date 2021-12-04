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
import org.apache.spark.sql.catalyst.expressions.InSubquery
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromTable
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation

object RowLevelOperationsPredicateCheck extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case DeleteFromTable(r, Some(condition)) if hasNullAwarePredicateWithinNot(condition) && isIcebergRelation(r) =>
        // this limitation is present since SPARK-25154 fix is not yet available
        // we use Not(EqualsNullSafe(cond, true)) when deciding which records to keep
        // such conditions are rewritten by Spark as an existential join and currently Spark
        // does not handle correctly NOT IN subqueries nested into other expressions
        failAnalysis("Null-aware predicate subqueries are not currently supported in DELETE")

      case UpdateTable(r, _, Some(condition)) if hasNullAwarePredicateWithinNot(condition) && isIcebergRelation(r) =>
        // this limitation is present since SPARK-25154 fix is not yet available
        // we use Not(EqualsNullSafe(cond, true)) when processing records that did not match
        // the update condition but were present in files we are overwriting
        // such conditions are rewritten by Spark as an existential join and currently Spark
        // does not handle correctly NOT IN subqueries nested into other expressions
        failAnalysis("Null-aware predicate subqueries are not currently supported in UPDATE")

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

  private def hasNullAwarePredicateWithinNot(cond: Expression): Boolean = {
    cond.find {
      case Not(expr) if expr.find(_.isInstanceOf[InSubquery]).isDefined => true
      case _ => false
    }.isDefined
  }

  private def failAnalysis(msg: String): Unit = throw new AnalysisException(msg)
}
