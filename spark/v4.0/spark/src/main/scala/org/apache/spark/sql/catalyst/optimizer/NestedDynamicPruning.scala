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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.DynamicPruningSubquery
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Drops each dynamic pruning filter whose filtering plan holds dynamic pruning filters of its own.
 *
 * When the session runs this rule, Iceberg scans offer their data columns to Spark's dynamic
 * pruning, so a join on any key can prune them, not only a join on a partition source column.
 * Spark adds one pruning subquery per such join, holding the plan of the join's other side. When
 * that side holds pruned scans too, as a common table expression joined with itself does, each
 * subquery copies their subqueries, and the physical plan grows exponentially with the depth of
 * the joins. A filtering side that big is no broadcast dimension either, so its pruning would not
 * run.
 *
 * A dynamic pruning filter only skips rows the join drops anyway, so dropping the filter leaves
 * the result as it is.
 */
object NestedDynamicPruning extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Adds the rule to the user provided optimizations of `session`, once.
   *
   * No session extension point runs after Spark's own dynamic pruning, but the user provided
   * optimizations do, and Spark reads them each time it optimizes a query.
   */
  def install(session: SparkSession): Unit = {
    val experimental = session.experimental
    experimental.synchronized {
      if (!experimental.extraOptimizations.contains(this)) {
        experimental.extraOptimizations = experimental.extraOptimizations :+ this
      }
    }
  }

  /** Whether `session` runs the rule, so its scans can offer their data columns to dynamic pruning. */
  def installed(session: SparkSession): Boolean =
    session.experimental.extraOptimizations.contains(this)

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan.transformUp {
      case Filter(condition, child) if condition.exists(isNested) =>
        splitConjunctivePredicates(condition)
          .filterNot(isNested)
          .reduceOption(And)
          .map(Filter(_, child))
          .getOrElse(child)
    }

  private def isNested(expression: Expression): Boolean =
    expression match {
      case subquery: DynamicPruningSubquery =>
        subquery.buildQuery.exists(
          _.expressions.exists(_.exists(_.isInstanceOf[DynamicPruningSubquery])))
      case _ => false
    }
}

/**
 * The analyzer rule through which the session extensions install [[NestedDynamicPruning]]. Spark
 * builds the analyzer of a session before it optimizes a query of that session, so the rule is
 * in place in time. The rule itself changes no plan.
 */
case class InstallNestedDynamicPruning(session: SparkSession) extends Rule[LogicalPlan] {
  NestedDynamicPruning.install(session)

  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
