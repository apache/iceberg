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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.catalyst.utils.RewriteRowLevelOperationHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits

case class RewriteUpdate(spark: SparkSession) extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper {

  import ExtendedDataSourceV2Implicits._

  // TODO: can we do any better for no-op updates? when conditions evaluate to false/true?
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case UpdateTable(r: DataSourceV2Relation, assignments, Some(cond))
        if isIcebergRelation(r) && SubqueryExpression.hasSubquery(cond) =>
      throw new AnalysisException("UPDATE statements with subqueries are not currently supported")

    case UpdateTable(r: DataSourceV2Relation, assignments, Some(cond)) if isIcebergRelation(r) =>
      val writeInfo = newWriteInfo(r.schema)
      val mergeBuilder = r.table.asMergeable.newMergeBuilder("update", writeInfo)

      val matchingRowsPlanBuilder = scanRelation => Filter(cond, scanRelation)
      val scanPlan = buildDynamicFilterScanPlan(spark, r.table, r.output, mergeBuilder, cond, matchingRowsPlanBuilder)

      val updateProjection = buildUpdateProjection(r, scanPlan, assignments, cond)

      val mergeWrite = mergeBuilder.asWriteBuilder.buildForBatch()
      val writePlan = buildWritePlan(updateProjection, r.table)
      ReplaceData(r, mergeWrite, writePlan)
  }

  private def buildUpdateProjection(
      relation: DataSourceV2Relation,
      scanPlan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression): LogicalPlan = {

    // this method relies on the fact that the assignments have been aligned before
    require(relation.output.size == assignments.size, "assignments must be aligned")

    // Spark is going to execute the condition for each column but it seems we cannot avoid this
    val assignedExprs = assignments.map(_.value)
    val updatedExprs = assignedExprs.zip(relation.output).map { case (assignedExpr, attr) =>
      // use semanticEquals to avoid unnecessary if expressions as we may run after operator optimization
      val updatedExpr = if (attr.semanticEquals(assignedExpr)) {
        attr
      } else {
        If(cond, assignedExpr, attr)
      }
      Alias(updatedExpr, attr.name)()
    }

    Project(updatedExprs, scanPlan)
  }
}
