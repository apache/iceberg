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
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.BINARY_COMPARISON
import org.apache.spark.sql.catalyst.trees.TreePattern.COMMAND
import org.apache.spark.sql.catalyst.trees.TreePattern.FILTER
import org.apache.spark.sql.catalyst.trees.TreePattern.IN
import org.apache.spark.sql.catalyst.trees.TreePattern.INSET
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

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformWithPruning(_.containsAnyPattern(COMMAND, FILTER, JOIN)) {
      case join @ Join(_, _, _, Some(cond), _) =>
        replaceStaticInvoke(join, cond, newCond => join.copy(condition = Some(newCond)))

      case filter @ Filter(cond, _) =>
        replaceStaticInvoke(filter, cond, newCond => filter.copy(condition = newCond))
    }

  private def replaceStaticInvoke[T <: LogicalPlan](
      node: T,
      condition: Expression,
      copy: Expression => T): T = {
    val newCondition = replaceStaticInvoke(condition)
    if (newCondition fastEquals condition) node else copy(newCondition)
  }

  private def replaceStaticInvoke(condition: Expression): Expression = {
    condition.transformWithPruning(_.containsAnyPattern(BINARY_COMPARISON, IN, INSET)) {
      case in @ In(value: StaticInvoke, _) if canReplace(value) =>
        in.copy(value = replaceStaticInvoke(value))

      case in @ InSet(value: StaticInvoke, _) if canReplace(value) =>
        in.copy(child = replaceStaticInvoke(value))

      case c @ BinaryComparison(left: StaticInvoke, right) if canReplace(left) && right.foldable =>
        c.withNewChildren(Seq(replaceStaticInvoke(left), right))

      case c @ BinaryComparison(left, right: StaticInvoke) if canReplace(right) && left.foldable =>
        c.withNewChildren(Seq(left, replaceStaticInvoke(right)))
    }
  }

  private def replaceStaticInvoke(invoke: StaticInvoke): Expression = {
    // Adaptive from `resolveV2Function` in org.apache.spark.sql.catalyst.analysis.ResolveFunctions
    val unbound = SparkFunctions.loadFunctionByClass(invoke.staticObject)
    if (unbound == null) {
      return invoke
    }

    val inputType = StructType(invoke.arguments.zipWithIndex.map { case (exp, pos) =>
      StructField(s"_$pos", exp.dataType, exp.nullable)
    })

    val bound =
      try {
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
