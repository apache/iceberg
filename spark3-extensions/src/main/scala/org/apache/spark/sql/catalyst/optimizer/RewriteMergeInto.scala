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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

object RewriteMergeInto extends Rule[LogicalPlan] with PlanHelper with Logging  {
  val ROW_FROM_SOURCE = "_row_from_source_"
  val ROW_FROM_TARGET = "_row_from_target_"

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // rewrite all operations that require reading the table to delete records
      case MergeIntoTable(target: DataSourceV2Relation,
                          source: LogicalPlan, cond, actions, notActions) =>
        val targetOutputCols = target.output
        val newProjectCols = target.output ++ Seq(Alias(InputFileName(), FILE_NAME_COL)())
        val newTargetTable = Project(newProjectCols, target)

        // Construct the plan to prune target based on join condition between source and
        // target.
        val prunedTargetPlan = Join(source, newTargetTable, Inner, Some(cond), JoinHint.NONE)
        val writeInfo = newWriteInfo(target.schema)
        val mergeBuilder = target.table.asMergeable.newMergeBuilder("delete", writeInfo)
        val targetTableScan =  buildScanPlan(target.table, target.output, mergeBuilder, prunedTargetPlan)

        // Construct an outer join to help track changes in source and target.
        // TODO : Optimize this to use LEFT ANTI or RIGHT OUTER when applicable.
        val sourceTableProj = source.output ++ Seq(Alias(lit(true).expr, ROW_FROM_SOURCE)())
        val targetTableProj = target.output ++ Seq(Alias(lit(true).expr, ROW_FROM_TARGET)())
        val newTargetTableScan = Project(targetTableProj, targetTableScan)
        val newSourceTableScan = Project(sourceTableProj, source)
        val joinPlan = Join(newSourceTableScan, newTargetTableScan, FullOuter, Some(cond), JoinHint.NONE)

        // Construct the plan to replace the data based on the output of `MergeInto`
        val mergeParams = MergeIntoParams(
          isSourceRowNotPresent = IsNull(findOutputAttr(joinPlan, ROW_FROM_SOURCE)),
          isTargetRowNotPresent = IsNull(findOutputAttr(joinPlan, ROW_FROM_TARGET)),
          matchedConditions = actions.map(getClauseCondition),
          matchedOutputs = actions.map(actionOutput(_, targetOutputCols)),
          notMatchedConditions = notActions.map(getClauseCondition),
          notMatchedOutputs = notActions.map(actionOutput(_, targetOutputCols)),
          targetOutput = targetOutputCols :+ Literal(false),
          deleteOutput = targetOutputCols :+ Literal(true),
          joinedAttributes = joinPlan.output
        )
        val mergePlan = MergeInto(mergeParams, target, joinPlan)
        val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()
        ReplaceData(target, batchWrite, mergePlan)
    }
  }

  def getTargetOutputCols(target: DataSourceV2Relation): Seq[NamedExpression] = {
    target.schema.map { col =>
      target.output.find(attr => SQLConf.get.resolver(attr.name, col.name)).getOrElse {
        Alias(Literal(null, col.dataType), col.name)()
      }
    }
  }

  def actionOutput(clause: MergeAction, targetOutputCols: Seq[Expression]): Seq[Expression] = {
    clause match {
      case u: UpdateAction =>
        u.assignments.map(_.value) :+ Literal(false)
      case _: DeleteAction =>
        targetOutputCols :+ Literal(true)
      case i: InsertAction =>
        i.assignments.map(_.value) :+ Literal(false)
    }
  }

  def getClauseCondition(clause: MergeAction): Expression = {
    clause.condition.getOrElse(Literal(true))
  }
}

