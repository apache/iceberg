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

import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.InputFileName
import org.apache.spark.sql.catalyst.expressions.IsNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.Inner
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
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.RewriteRowLevelOperationHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

case class RewriteMergeInto(conf: SQLConf) extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper  {
  private val ROW_FROM_SOURCE = "_row_from_source_"
  private val ROW_FROM_TARGET = "_row_from_target_"
  private val TRUE_LITERAL = Literal(true, BooleanType)
  private val FALSE_LITERAL = Literal(false, BooleanType)

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  override def resolver: Resolver = conf.resolver

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case MergeIntoTable(target: DataSourceV2Relation, source: LogicalPlan, cond, matchedActions, notMatchedActions) =>
        // Construct the plan to prune target based on join condition between source and target.
        val writeInfo = newWriteInfo(target.schema)
        val mergeBuilder = target.table.asMergeable.newMergeBuilder("merge", writeInfo)
        val matchingRowsPlanBuilder = (rel: DataSourceV2ScanRelation) =>
          Join(source, rel, Inner, Some(cond), JoinHint.NONE)
        val targetTableScan = buildScanPlan(target.table, target.output, mergeBuilder, cond, matchingRowsPlanBuilder)

        // Construct an outer join to help track changes in source and target.
        // TODO : Optimize this to use LEFT ANTI or RIGHT OUTER when applicable.
        val sourceTableProj = source.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_SOURCE)())
        val targetTableProj = target.output ++ Seq(Alias(TRUE_LITERAL, ROW_FROM_TARGET)())
        val newTargetTableScan = Project(targetTableProj, targetTableScan)
        val newSourceTableScan = Project(sourceTableProj, source)
        val joinPlan = Join(newSourceTableScan, newTargetTableScan, FullOuter, Some(cond), JoinHint.NONE)

        // Construct the plan to replace the data based on the output of `MergeInto`
        val mergeParams = MergeIntoParams(
          isSourceRowNotPresent = IsNull(findOutputAttr(joinPlan.output, ROW_FROM_SOURCE)),
          isTargetRowNotPresent = IsNull(findOutputAttr(joinPlan.output, ROW_FROM_TARGET)),
          matchedConditions = matchedActions.map(getClauseCondition),
          matchedOutputs = matchedActions.map(actionOutput(_, target.output)),
          notMatchedConditions = notMatchedActions.map(getClauseCondition),
          notMatchedOutputs = notMatchedActions.map(actionOutput(_, target.output)),
          targetOutput = target.output :+ FALSE_LITERAL,
          deleteOutput = target.output :+ TRUE_LITERAL,
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, target, joinPlan)
        val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()
        ReplaceData(target, batchWrite, mergePlan)
    }
  }

  private def actionOutput(clause: MergeAction, targetOutputCols: Seq[Expression]): Seq[Expression] = {
    clause match {
      case u: UpdateAction =>
        u.assignments.map(_.value) :+ FALSE_LITERAL
      case _: DeleteAction =>
        targetOutputCols :+ TRUE_LITERAL
      case i: InsertAction =>
        i.assignments.map(_.value) :+ FALSE_LITERAL
    }
  }

  private def getClauseCondition(clause: MergeAction): Expression = {
    clause.condition.getOrElse(TRUE_LITERAL)
  }
}

