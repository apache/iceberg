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

import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.iceberg.spark.functions.SparkFunctions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.NoStatsUnaryNode
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData
import org.apache.spark.sql.catalyst.plans.logical.RowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.UnplannedGroupBasedMergeOperation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.annotation.tailrec

/**
 * Spark analyzes the Iceberg system function to {@link StaticInvoke} which could not be pushed
 * down to datasource. This rule will replace {@link StaticInvoke} to
 * {@link ApplyFunctionExpression} for Iceberg system function in a filter condition.
 */
object ReplaceStaticInvoke extends Rule[LogicalPlan] with SQLConfHelper {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (systemFuncPushDownEnabled()) {
      rewrite(plan)
    } else {
      plan
    }
  }

  private def rewrite(plan: LogicalPlan): LogicalPlan = plan transform {
    case rowLevelCommand: RowLevelCommand if rowLevelCommand.rewritePlan.nonEmpty =>
      // Align with RowLevelCommandScanRelationPushDown
      rowLevelCommand match {
        case RewrittenRowLevelCommand(_, _: DataSourceV2Relation, rewritePlan: WriteIcebergDelta) =>
          rowLevelCommand.withNewRewritePlan(rewrite(rewritePlan))

        case UnplannedGroupBasedMergeOperation(_, rd: ReplaceIcebergData,
                                               Join(_, _, _, Some(joinCond), _), relation: DataSourceV2Relation) =>
          val newQuery = rd.query transform {
            case j @ Join(
            NoStatsUnaryNode(
            ScanOperation(_, pushDownFilters, pushUpFilters, r: DataSourceV2Relation)), _, _, _, _)
              if j.condition.nonEmpty && pushUpFilters.isEmpty && pushDownFilters.isEmpty &&
                r.table.eq(relation.table) =>
              val newCondition = replaceStaticInvoke(j.condition.get)
              j.copy(condition = Some(newCondition))
          }

          rowLevelCommand.withNewRewritePlan(rd.copy(query = newQuery))

        case RewrittenRowLevelCommand(_, _: DataSourceV2Relation, _) =>
          if (rowLevelCommand.condition.nonEmpty) {
            rowLevelCommand.withNewCondition(replaceStaticInvoke(rowLevelCommand.condition.get))
          } else {
            rowLevelCommand
          }
      }

    case replaceData: ReplaceData if icebergRelation(replaceData.table) =>
      val newCondition = replaceStaticInvoke(replaceData.condition)
      replaceData.copy(condition = newCondition)

    case writeDelta: WriteDelta if icebergRelation(writeDelta.table) =>
      val newCondition = replaceStaticInvoke(writeDelta.condition)
      writeDelta.copy(condition = newCondition)

    case filter @ Filter(condition, child) if icebergRelation(child) =>
      val newCondition = replaceStaticInvoke(condition)
      if (newCondition fastEquals condition) {
        filter
      } else {
        filter.copy(condition = newCondition)
      }
  }

  private def systemFuncPushDownEnabled(): Boolean = {
    conf.getConfString(
      SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED,
      SparkSQLProperties.SYSTEM_FUNC_PUSH_DOWN_ENABLED_DEFAULT.toString
      ).toBoolean
  }

  private def replaceStaticInvoke(expression: Expression): Expression = expression.transform {
    case invoke: StaticInvoke =>
      invoke.functionName match {
        case "invoke" =>
          // Adaptive from `resolveV2Function` in org.apache.spark.sql.catalyst.analysis.ResolveFunctions
          val unbound = SparkFunctions.loadFunctionByClass(invoke.staticObject)
          if (unbound == null) {
            return invoke
          }

          val inputType = StructType(invoke.arguments.zipWithIndex.map {
            case (exp, pos) => StructField(s"_$pos", exp.dataType, exp.nullable)
          })

          val bound = try {
            unbound.bind(inputType)
          } catch {
            case _: Exception =>
              return invoke
          }

          if (bound.inputTypes().length != invoke.arguments.length) {
            return invoke
          }

          bound match {
            case scalarFunc: ScalarFunction[_] =>
              ApplyFunctionExpression(scalarFunc, invoke.arguments)
            case _ => invoke
          }
      }
  }

  @tailrec
  private def icebergRelation(plan: LogicalPlan): Boolean = {

    @tailrec
    def isIcebergTable(table: Table): Boolean = table match {
      case _: SparkTable => true
      case r: RowLevelOperationTable => isIcebergTable(r.table)
      case _ => false
    }

    plan match {
      case relation: DataSourceV2Relation =>
        isIcebergTable(relation.table)
      case scanRelation: DataSourceV2ScanRelation =>
        icebergRelation(scanRelation.relation)
      case _ => false
    }
  }
}
