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
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExtendedV2ExpressionUtils
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData
import org.apache.spark.sql.catalyst.plans.logical.RowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.Subquery
import org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.trees.TreePattern.SORT
import org.apache.spark.sql.connector.read.SupportsRuntimeFiltering
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits
import scala.collection.compat.immutable.ArraySeq

/**
 * A rule that adds a runtime filter for row-level commands.
 *
 * Note that only group-based rewrite plans (i.e. ReplaceData) are taken into account.
 * Row-based rewrite plans are subject to usual runtime filtering.
 */
case class RowLevelCommandDynamicPruning(spark: SparkSession) extends Rule[LogicalPlan] with PredicateHelper {

  import ExtendedDataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // apply special dynamic filtering only for plans that don't support deltas
    case RewrittenRowLevelCommand(
        command: RowLevelCommand,
        DataSourceV2ScanRelation(_, scan: SupportsRuntimeFiltering, _, _),
        rewritePlan: ReplaceIcebergData) if conf.dynamicPartitionPruningEnabled && isCandidate(command) =>

      // use reference equality to find exactly the required scan relations
      val newRewritePlan = rewritePlan transformUp {
        case r: DataSourceV2ScanRelation if r.scan eq scan =>
          // use the original table instance that was loaded for this row-level operation
          // in order to leverage a regular batch scan in the group filter query
          val originalTable = r.relation.table.asRowLevelOperationTable.table
          val relation = r.relation.copy(table = originalTable)
          val matchingRowsPlan = buildMatchingRowsPlan(relation, command)

          val filterAttrs = ArraySeq.unsafeWrapArray(scan.filterAttributes)
          val buildKeys = ExtendedV2ExpressionUtils.resolveRefs[Attribute](filterAttrs, matchingRowsPlan)
          val pruningKeys = ExtendedV2ExpressionUtils.resolveRefs[Attribute](filterAttrs, r)
          val dynamicPruningCond = buildDynamicPruningCond(matchingRowsPlan, buildKeys, pruningKeys)

          Filter(dynamicPruningCond, r)
      }

      // always optimize dynamic filtering subqueries for row-level commands as it is important
      // to rewrite introduced predicates as joins because Spark recently stopped optimizing
      // dynamic subqueries to facilitate broadcast reuse
      command.withNewRewritePlan(optimizeSubquery(newRewritePlan))
  }

  private def isCandidate(command: RowLevelCommand): Boolean = command.condition match {
    case Some(cond) if cond != Literal.TrueLiteral => true
    case _ => false
  }

  private def buildMatchingRowsPlan(
      relation: DataSourceV2Relation,
      command: RowLevelCommand): LogicalPlan = {

    // construct a filtering plan with the original scan relation
    val matchingRowsPlan = command match {
      case d: DeleteFromIcebergTable =>
        Filter(d.condition.get, relation)

      case u: UpdateIcebergTable =>
        // UPDATEs with subqueries are rewritten using a UNION with two identical scan relations
        // the analyzer clones of them and assigns fresh expr IDs so that attributes don't collide
        // this rule assigns dynamic filters to both scan relations based on the update condition
        // the condition always refers to the original expr IDs and must be transformed
        // see RewriteUpdateTable for more details
        val attrMap = buildAttrMap(u.table.output, relation.output)
        val transformedCond = u.condition.get transform {
          case attr: AttributeReference if attrMap.contains(attr) => attrMap(attr)
        }
        Filter(transformedCond, relation)

      case m: MergeIntoIcebergTable =>
        Join(relation, m.sourceTable, LeftSemi, Some(m.mergeCondition), JoinHint.NONE)
    }

    // clone the original relation in the filtering plan and assign new expr IDs to avoid conflicts
    matchingRowsPlan transformUpWithNewOutput {
      case r: DataSourceV2Relation if r eq relation =>
        val oldOutput = r.output
        val newOutput = oldOutput.map(_.newInstance())
        r.copy(output = newOutput) -> oldOutput.zip(newOutput)
    }
  }

  private def buildDynamicPruningCond(
      matchingRowsPlan: LogicalPlan,
      buildKeys: Seq[Attribute],
      pruningKeys: Seq[Attribute]): Expression = {

    val buildQuery = Project(buildKeys, matchingRowsPlan)
    val dynamicPruningSubqueries = pruningKeys.zipWithIndex.map { case (key, index) =>
      DynamicPruningSubquery(key, buildQuery, buildKeys, index, onlyInBroadcast = false)
    }
    dynamicPruningSubqueries.reduce(And)
  }

  private def buildAttrMap(
      tableAttrs: Seq[Attribute],
      scanAttrs: Seq[Attribute]): AttributeMap[Attribute] = {

    val resolver = conf.resolver
    val attrMapping = tableAttrs.flatMap { tableAttr =>
      scanAttrs
        .find(scanAttr => resolver(scanAttr.name, tableAttr.name))
        .map(scanAttr => tableAttr -> scanAttr)
    }
    AttributeMap(attrMapping)
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
