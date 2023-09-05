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
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
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
          if (invoke.arguments.forall(_.foldable)) {
            // The invoke should be folded into constant
            return invoke
          }

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

    def isIcebergTable(table: Table): Boolean = table match {
      case _: SparkTable => true
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
