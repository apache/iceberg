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

import org.apache.iceberg.DistributionMode
import org.apache.iceberg.TableProperties
import org.apache.iceberg.TableProperties.MERGE_CARDINALITY_CHECK_ENABLED
import org.apache.iceberg.TableProperties.MERGE_CARDINALITY_CHECK_ENABLED_DEFAULT
import org.apache.iceberg.spark.Spark3Util.toClusteredDistribution
import org.apache.iceberg.spark.Spark3Util.toOrderedDistribution
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.util.PropertyUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Ascending
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NullsFirst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.RightOuter
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeAction
import org.apache.spark.sql.catalyst.plans.logical.MergeInto
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoParams
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.catalyst.utils.RewriteRowLevelOperationHelper
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.BooleanType

case class RewriteMergeInto(spark: SparkSession) extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper  {
  private val ROW_FROM_SOURCE = "_row_from_source_"
  private val ROW_FROM_TARGET = "_row_from_target_"
  private val TRUE_LITERAL = Literal(true, BooleanType)
  private val FALSE_LITERAL = Literal(false, BooleanType)

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  override def resolver: Resolver = spark.sessionState.conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case MergeIntoTable(target: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if matchedActions.isEmpty && isIcebergRelation(target) =>

        val targetTableScan = buildSimpleScanPlan(target, cond)

        // when there are no matched actions, use a left anti join to remove any matching rows and rewrite to use
        // append instead of replace. only unmatched source rows are passed to the merge and actions are all inserts.
        val joinPlan = Join(source, targetTableScan, LeftAnti, Some(cond), JoinHint.NONE)

        val mergeParams = MergeIntoParams(
          isSourceRowNotPresent = FALSE_LITERAL,
          isTargetRowNotPresent = TRUE_LITERAL,
          matchedConditions = Nil,
          matchedOutputs = Nil,
          notMatchedConditions = notMatchedActions.map(getClauseCondition),
          notMatchedOutputs = notMatchedActions.map(actionOutput),
          targetOutput = Nil,
          joinedAttributes = joinPlan.output
        )

        val mergePlan = MergeInto(mergeParams, target.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, target.table)

        AppendData.byPosition(target, writePlan, Map.empty)

      case MergeIntoTable(target: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if notMatchedActions.isEmpty && isIcebergRelation(target) =>

        val mergeBuilder = target.table.asMergeable.newMergeBuilder("merge", newWriteInfo(target.schema))

        // rewrite the matched actions to ensure there is always an action to produce the output row
        val (matchedConditions, matchedOutputs) = rewriteMatchedActions(matchedActions, target.output)

        // when there are no not-matched actions, use a right outer join to ignore source rows that do not match, but
        // keep all unmatched target rows that must be preserved.
        val sourceTableProj = source.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_SOURCE)())
        val newSourceTableScan = Project(sourceTableProj, source)
        val targetTableScan = buildDynamicFilterTargetScan(mergeBuilder, target, source, cond, matchedActions)
        val joinPlan = Join(newSourceTableScan, targetTableScan, RightOuter, Some(cond), JoinHint.NONE)

        val mergeParams = MergeIntoParams(
          isSourceRowNotPresent = IsNull(findOutputAttr(joinPlan.output, ROW_FROM_SOURCE)),
          isTargetRowNotPresent = FALSE_LITERAL,
          matchedConditions = matchedConditions,
          matchedOutputs = matchedOutputs,
          notMatchedConditions = Nil,
          notMatchedOutputs = Nil,
          targetOutput = target.output,
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, target.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, target.table)
        val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()

        ReplaceData(target, batchWrite, writePlan)

      case MergeIntoTable(target: DataSourceV2Relation, source, cond, matchedActions, notMatchedActions)
          if isIcebergRelation(target) =>

        val mergeBuilder = target.table.asMergeable.newMergeBuilder("merge", newWriteInfo(target.schema))

        // rewrite the matched actions to ensure there is always an action to produce the output row
        val (matchedConditions, matchedOutputs) = rewriteMatchedActions(matchedActions, target.output)

        // use a full outer join because there are both matched and not matched actions
        val sourceTableProj = source.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_SOURCE)())
        val newSourceTableScan = Project(sourceTableProj, source)
        val targetTableScan = buildDynamicFilterTargetScan(mergeBuilder, target, source, cond, matchedActions)
        val targetTableProj = targetTableScan.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_TARGET)())
        val newTargetTableScan = Project(targetTableProj, targetTableScan)
        val joinPlan = Join(newSourceTableScan, newTargetTableScan, FullOuter, Some(cond), JoinHint.NONE)

        val mergeParams = MergeIntoParams(
          isSourceRowNotPresent = IsNull(findOutputAttr(joinPlan.output, ROW_FROM_SOURCE)),
          isTargetRowNotPresent = IsNull(findOutputAttr(joinPlan.output, ROW_FROM_TARGET)),
          matchedConditions = matchedConditions,
          matchedOutputs = matchedOutputs,
          notMatchedConditions = notMatchedActions.map(getClauseCondition),
          notMatchedOutputs = notMatchedActions.map(actionOutput),
          targetOutput = target.output,
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, target.output, joinPlan)
        val writePlan = buildWritePlan(mergePlan, target.table)
        val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()

        ReplaceData(target, batchWrite, writePlan)
    }
  }

  private def actionOutput(clause: MergeAction): Option[Seq[Expression]] = {
    clause match {
      case u: UpdateAction =>
        Some(u.assignments.map(_.value))
      case _: DeleteAction =>
        None
      case i: InsertAction =>
        Some(i.assignments.map(_.value))
    }
  }

  private def getClauseCondition(clause: MergeAction): Expression = {
    clause.condition.getOrElse(TRUE_LITERAL)
  }

  private def buildDynamicFilterTargetScan(
      mergeBuilder: MergeBuilder,
      target: DataSourceV2Relation,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction]): LogicalPlan = {
    // Construct the plan to prune target based on join condition between source and target.
    val table = target.table
    val output = target.output
    val matchingRowsPlanBuilder = rel => Join(source, rel, Inner, Some(cond), JoinHint.NONE)
    val runCardinalityCheck = isCardinalityCheckEnabled(table) && isCardinalityCheckNeeded(matchedActions)
    buildDynamicFilterScanPlan(spark, table, output, mergeBuilder, cond, matchingRowsPlanBuilder, runCardinalityCheck)
  }

  private def rewriteMatchedActions(
      matchedActions: Seq[MergeAction],
      targetOutput: Seq[Expression]): (Seq[Expression], Seq[Option[Seq[Expression]]]) = {
    val startMatchedConditions = matchedActions.map(getClauseCondition)
    val catchAllIndex = startMatchedConditions.indexWhere {
      case Literal(true, BooleanType) =>
        true
      case _ =>
        false
    }

    val outputs = matchedActions.map(actionOutput)
    if (catchAllIndex < 0) {
      // all of the actions have non-trivial conditions. add an action to emit the target row if no action matches
      (startMatchedConditions :+ TRUE_LITERAL, outputs :+ Some(targetOutput))
    } else {
      // one "catch all" action will always match, prune the actions after it
      (startMatchedConditions.take(catchAllIndex + 1), outputs.take(catchAllIndex + 1))
    }
  }

  private def isCardinalityCheckEnabled(table: Table): Boolean = {
    PropertyUtil.propertyAsBoolean(
      table.properties(),
      MERGE_CARDINALITY_CHECK_ENABLED,
      MERGE_CARDINALITY_CHECK_ENABLED_DEFAULT)
  }

  private def isCardinalityCheckNeeded(actions: Seq[MergeAction]): Boolean = {
    def hasUnconditionalDelete(action: Option[MergeAction]): Boolean = {
      action match {
        case Some(DeleteAction(None)) => true
        case _ => false
      }
    }
    !(actions.size == 1 && hasUnconditionalDelete(actions.headOption))
  }

  def buildWritePlan(
     childPlan: LogicalPlan,
     table: Table): LogicalPlan = {
    val defaultDistributionMode = table match {
      case iceberg: SparkTable if !iceberg.table.sortOrder.isUnsorted =>
        TableProperties.WRITE_DISTRIBUTION_MODE_RANGE
      case _ =>
        TableProperties.WRITE_DISTRIBUTION_MODE_DEFAULT
    }

    table match {
      case iceTable: SparkTable =>
        val numShufflePartitions = spark.sessionState.conf.numShufflePartitions
        val table = iceTable.table()
        val distributionMode: String = table.properties
          .getOrDefault(TableProperties.WRITE_DISTRIBUTION_MODE, defaultDistributionMode)
        val order = toCatalyst(toOrderedDistribution(table.spec(), table.sortOrder(), true), childPlan)
        DistributionMode.fromName(distributionMode) match {
          case DistributionMode.NONE =>
            Sort(buildSortOrder(order), global = false, childPlan)
          case DistributionMode.HASH =>
            val clustering = toCatalyst(toClusteredDistribution(table.spec()), childPlan)
            val hashPartitioned = RepartitionByExpression(clustering, childPlan, numShufflePartitions)
            Sort(buildSortOrder(order), global = false, hashPartitioned)
          case DistributionMode.RANGE =>
            val roundRobin = Repartition(numShufflePartitions, shuffle = true, childPlan)
            Sort(buildSortOrder(order), global = true, roundRobin)
        }
      case _ =>
        childPlan
    }
  }

  private def buildSortOrder(exprs: Seq[Expression]): Seq[SortOrder] = {
    exprs.map { expr =>
      expr match {
        case e: SortOrder => e
        case other =>
          SortOrder(other, Ascending, NullsFirst, Set.empty)
      }
    }
  }
}

