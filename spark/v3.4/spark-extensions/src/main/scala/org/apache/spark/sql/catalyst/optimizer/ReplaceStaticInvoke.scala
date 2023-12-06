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

import org.apache.iceberg.spark.functions.SparkFunctions
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.expressions.BinaryComparison
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BINARY_COMPARISON
import org.apache.spark.sql.catalyst.trees.TreePattern.FILTER
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN
import org.apache.spark.sql.connector.catalog.functions.ScalarFunction
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Spark analyzes the Iceberg system function to {@link StaticInvoke} which could not be pushed
 * down to datasource. This rule will replace {@link StaticInvoke} to
 * {@link ApplyFunctionExpression} for Iceberg system function in a filter condition.
 */
object ReplaceStaticInvoke extends Rule[LogicalPlan] {

  private val rule:PartialFunction[Expression, Expression] = {
    case c@BinaryComparison(left: StaticInvoke, right) if canReplace(left) && right.foldable =>
      c.withNewChildren(Seq(replaceStaticInvoke(left), right))

    case c@BinaryComparison(left, right: StaticInvoke) if canReplace(right) && left.foldable =>
      c.withNewChildren(Seq(left, replaceStaticInvoke(right)))
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsAnyPattern(FILTER, JOIN)) {
      case filter @ Filter(condition, _) =>
        val newCondition = condition.transformWithPruning(_.containsPattern(BINARY_COMPARISON))(rule)
        if (newCondition fastEquals condition) {
          filter
        } else {
          filter.copy(condition = newCondition)
        }
      case j @ Join(_, _, _, Some(condition), _) =>
        val newCondition = condition.transformWithPruning(_.containsPattern(BINARY_COMPARISON))(rule)
        if (newCondition fastEquals condition) {
          j
        } else {
          j.copy(condition = Some(newCondition))
        }
    }
  }

  private def replaceStaticInvoke(invoke: StaticInvoke): Expression = {
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

  @inline
  private def canReplace(invoke: StaticInvoke): Boolean = {
    invoke.functionName == ScalarFunction.MAGIC_METHOD_NAME && !invoke.foldable
  }
}
