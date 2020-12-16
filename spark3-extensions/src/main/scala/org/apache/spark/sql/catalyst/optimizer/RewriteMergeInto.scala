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

import java.util.UUID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, JoinType}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, LogicalWriteInfoImpl}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object RewriteMergeInto extends Rule[LogicalPlan]
  with PredicateHelper
  with Logging  {
  val ROW_ID_COL = "_row_id_"
  val FILE_NAME_COL = "_file_name_"
  val SOURCE_ROW_PRESENT_COL = "_source_row_present_"
  val TARGET_ROW_PRESENT_COL = "_target_row_present_"

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      // rewrite all operations that require reading the table to delete records
      case MergeIntoTable(target: DataSourceV2Relation,
                          source: LogicalPlan, cond, actions, notActions) =>
        // Find the files in target that matches the JOIN condition from source.
        val targetOutputCols = target.output
        val newProjectCols = target.output ++ Seq(Alias(InputFileName(), FILE_NAME_COL)())
        val newTargetTable = Project(newProjectCols, target)
        val prunedTargetPlan = Join(source, newTargetTable, Inner, Some(cond), JoinHint.NONE)

        val writeInfo = newWriteInfo(target.schema)
        val mergeBuilder = target.table.asMergeable.newMergeBuilder("delete", writeInfo)
        val targetTableScan =  buildScanPlan(target.table, target.output, mergeBuilder, prunedTargetPlan)
        val sourceTableProj = source.output ++ Seq(Alias(lit(true).expr, SOURCE_ROW_PRESENT_COL)())
        val targetTableProj = target.output ++ Seq(Alias(lit(true).expr, TARGET_ROW_PRESENT_COL)())
        val newTargetTableScan = Project(targetTableProj, targetTableScan)
        val newSourceTableScan = Project(sourceTableProj, source)
        val joinPlan = Join(newSourceTableScan, newTargetTableScan, FullOuter, Some(cond), JoinHint.NONE)

        val mergeIntoProcessor = new MergeIntoProcessor(
          isSourceRowNotPresent = resolveExprs(Seq(col(SOURCE_ROW_PRESENT_COL).isNull.expr), joinPlan).head,
          isTargetRowNotPresent = resolveExprs(Seq(col(TARGET_ROW_PRESENT_COL).isNull.expr), joinPlan).head,
          matchedConditions = actions.map(resolveClauseCondition(_, joinPlan)),
          matchedOutputs = actions.map(actionOutput(_, targetOutputCols, joinPlan)),
          notMatchedConditions = notActions.map(resolveClauseCondition(_, joinPlan)),
          notMatchedOutputs = notActions.map(actionOutput(_, targetOutputCols, joinPlan)),
          targetOutput = resolveExprs(targetOutputCols :+ Literal(false), joinPlan),
          joinedAttributes = joinPlan.output
        )

        val mergePlan = MergeInto(mergeIntoProcessor, target, joinPlan)
        val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()
        ReplaceData(target, batchWrite, mergePlan)
    }
  }

  private def buildScanPlan(
      table: Table,
      output: Seq[AttributeReference],
      mergeBuilder: MergeBuilder,
      prunedTargetPlan: LogicalPlan): LogicalPlan = {

    val scanBuilder = mergeBuilder.asScanBuilder
    val scan = scanBuilder.build()
    val scanRelation = DataSourceV2ScanRelation(table, scan, output)

    scan match {
      case _: SupportsFileFilter =>
        val matchingFilePlan = buildFileFilterPlan(prunedTargetPlan)
        val dynamicFileFilter = DynamicFileFilter(scanRelation, matchingFilePlan)
        dynamicFileFilter
      case _ =>
        scanRelation
    }
  }

  private def newWriteInfo(schema: StructType): LogicalWriteInfo = {
    val uuid = UUID.randomUUID()
    LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
  }

  private def buildFileFilterPlan(prunedTargetPlan: LogicalPlan): LogicalPlan = {
    val fileAttr = findOutputAttr(prunedTargetPlan, FILE_NAME_COL)
    Aggregate(Seq(fileAttr), Seq(fileAttr), prunedTargetPlan)
  }

  private def findOutputAttr(plan: LogicalPlan, attrName: String): Attribute = {
    val resolver = SQLConf.get.resolver
    plan.output.find(attr => resolver(attr.name, attrName)).getOrElse {
      throw new AnalysisException(s"Cannot find $attrName in ${plan.output}")
    }
  }

  private def resolveExprs(exprs: Seq[Expression], plan: LogicalPlan): Seq[Expression] = {
    val spark = SparkSession.active
    exprs.map { expr => resolveExpressionInternal(spark, expr, plan) }
  }

  def getTargetOutputCols(target: DataSourceV2Relation): Seq[NamedExpression] = {
    target.schema.map { col =>
      target.output.find(attr => SQLConf.get.resolver(attr.name, col.name)).getOrElse {
        Alias(Literal(null, col.dataType), col.name)()
      }
    }
  }

  def actionOutput(clause: MergeAction,
                   targetOutputCols: Seq[Expression],
                   plan: LogicalPlan): Seq[Expression] = {
    val exprs = clause match {
      case u: UpdateAction =>
        u.assignments.map(_.value) :+ Literal(false)
      case _: DeleteAction =>
        targetOutputCols :+ Literal(true)
      case i: InsertAction =>
        i.assignments.map(_.value) :+ Literal(false)
    }
    resolveExprs(exprs, plan)
  }

  def resolveClauseCondition(clause: MergeAction, plan: LogicalPlan): Expression = {
    val condExpr = clause.condition.getOrElse(Literal(true))
    resolveExprs(Seq(condExpr), plan).head
  }

  def resolveExpressionInternal(spark: SparkSession, expr: Expression, plan: LogicalPlan): Expression = {
    val dummyPlan = Filter(expr, plan)
    spark.sessionState.analyzer.execute(dummyPlan) match {
      case Filter(resolvedExpr, _) => resolvedExpr
      case _ => throw new AnalysisException(s"Could not resolve expression $expr", plan = Option(plan))
    }
  }
}

