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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExtendedV2ExpressionUtils
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.RowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.trees.TreePattern.SORT
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * A rule that adds a runtime filter for row-level commands.
 *
 * Note that only group-based rewrite plans (i.e. ReplaceData) are taken into account.
 * Row-based rewrite plans are subject to usual runtime filtering.
 */
case class RowLevelCommandDynamicPruning(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // apply special dynamic filtering only for plans that don't support deltas
    case RewrittenRowLevelCommand(
        command: RowLevelCommand,
        DataSourceV2ScanRelation(_, scan: SupportsRuntimeFiltering, _),
        rewritePlan: ReplaceData) if conf.dynamicPartitionPruningEnabled && isCandidate(command) =>

      // use reference equality to find exactly the required scan relations
      val newRewritePlan = rewritePlan transformUp {
        case r: DataSourceV2ScanRelation if r.scan eq scan =>
          val pruningKeys = ExtendedV2ExpressionUtils.resolveRefs[Attribute](scan.filterAttributes, r)
          val dynamicPruningCond = buildDynamicPruningCondition(r, command, pruningKeys)
          val filter = Filter(dynamicPruningCond, r)
          // always optimize dynamic filtering subqueries for row-level commands as it is important
          // to rewrite introduced predicates as joins because Spark recently stopped optimizing
          // dynamic subqueries to facilitate broadcast reuse
          optimizeSubquery(filter)
      }
      command.withNewRewritePlan(newRewritePlan)
  }

  private def isCandidate(command: RowLevelCommand): Boolean = command.condition match {
    case Some(cond) if cond != Literal.TrueLiteral => true
    case _ => false
  }

  private def buildDynamicPruningCondition(
      relation: DataSourceV2ScanRelation,
      command: RowLevelCommand,
      pruningKeys: Seq[Attribute]): Expression = {

    // construct a filtering plan with the original scan relation
    val matchingRowsPlan = command match {
      case d: DeleteFromIcebergTable =>
        Filter(d.condition.get, relation)
    }

    // clone the original relation in the filtering plan and assign new expr IDs to avoid conflicts
    val transformedMatchingRowsPlan = matchingRowsPlan transformUpWithNewOutput {
      case r: DataSourceV2ScanRelation if r eq relation =>
        val oldOutput = r.output
        val newOutput = oldOutput.map(_.newInstance())
        r.copy(output = newOutput) -> oldOutput.zip(newOutput)
    }

    val filterableScan = relation.scan.asInstanceOf[SupportsRuntimeFiltering]
    val buildKeys = ExtendedV2ExpressionUtils.resolveRefs[Attribute](
      filterableScan.filterAttributes,
      transformedMatchingRowsPlan)
    val buildQuery = Project(buildKeys, transformedMatchingRowsPlan)
    val dynamicPruningSubqueries = pruningKeys.zipWithIndex.map { case (key, index) =>
      DynamicPruningSubquery(key, buildQuery, buildKeys, index, onlyInBroadcast = false)
    }

    // combine all dynamic subqueries to produce the final condition
    dynamicPruningSubqueries.reduce(And)
  }

  // borrowed from OptimizeSubqueries in Spark
  private def optimizeSubquery(plan: LogicalPlan): LogicalPlan = plan.transformAllExpressionsWithPruning(
    _.containsPattern(PLAN_EXPRESSION)) {
    case s: SubqueryExpression =>
      val Subquery(newPlan, _) = spark.sessionState.optimizer.execute(Subquery.fromExpression(s))
      // At this point we have an optimized subquery plan that we are going to attach
      // to this subquery expression. Here we can safely remove any top level sort
      // in the plan as tuples produced by a subquery are un-ordered.
      s.withNewPlan(removeTopLevelSort(newPlan))
  }

  // borrowed from OptimizeSubqueries in Spark
  private def removeTopLevelSort(plan: LogicalPlan): LogicalPlan = {
    if (!plan.containsPattern(SORT)) {
      return plan
    }
    plan match {
      case Sort(_, _, child) => child
      case Project(fields, child) => Project(fields, removeTopLevelSort(child))
      case other => other
    }
  }
}
