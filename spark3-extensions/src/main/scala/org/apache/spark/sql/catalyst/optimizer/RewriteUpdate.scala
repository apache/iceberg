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
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilter
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.catalyst.utils.RewriteRowLevelOperationHelper
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.BooleanType

case class RewriteUpdate(spark: SparkSession) extends Rule[LogicalPlan] with RewriteRowLevelOperationHelper {

  import ExtendedDataSourceV2Implicits._

  override def conf: SQLConf = SQLConf.get

  // TODO: can we do any better for no-op updates? when conditions evaluate to false/true?
  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case UpdateTable(r: DataSourceV2Relation, assignments, Some(cond))
        if isIcebergRelation(r) && SubqueryExpression.hasSubquery(cond) =>

      val writeInfo = newWriteInfo(r.schema)
      val mergeBuilder = r.table.asMergeable.newMergeBuilder("update", writeInfo)

      // since we are processing matched and not matched rows using separate jobs
      // there will be two scans but we want to execute the dynamic file filter only once
      // so the first job uses DynamicFileFilter and the second one uses the underlying scan plan
      // both jobs share the same SparkMergeScan instance to ensure they operate on same files
      val matchingRowsPlanBuilder = scanRelation => Filter(cond, scanRelation)
      val scanPlan = buildDynamicFilterScanPlan(spark, r, r.output, mergeBuilder, cond, matchingRowsPlanBuilder)
      val underlyingScanPlan = scanPlan match {
        case DynamicFileFilter(plan, _, _) => plan.clone()
        case _ => scanPlan.clone()
      }

      // build a plan for records that match the cond and should be updated
      val matchedRowsPlan = Filter(cond, scanPlan)
      val updatedRowsPlan = buildUpdateProjection(r, matchedRowsPlan, assignments)

      // build a plan for records that did not match the cond but had to be copied over
      val remainingRowFilter = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
      val remainingRowsPlan = Filter(remainingRowFilter, Project(r.output, underlyingScanPlan))

      // new state is a union of updated and copied over records
      val updatePlan = Union(updatedRowsPlan, remainingRowsPlan)

      val mergeWrite = mergeBuilder.asWriteBuilder.buildForBatch()
      val writePlan = buildWritePlan(updatePlan, r.table)
      ReplaceData(r, mergeWrite, writePlan)

    case UpdateTable(r: DataSourceV2Relation, assignments, Some(cond)) if isIcebergRelation(r) =>
      val writeInfo = newWriteInfo(r.schema)
      val mergeBuilder = r.table.asMergeable.newMergeBuilder("update", writeInfo)

      val matchingRowsPlanBuilder = scanRelation => Filter(cond, scanRelation)
      val scanPlan = buildDynamicFilterScanPlan(spark, r, r.output, mergeBuilder, cond, matchingRowsPlanBuilder)

      val updateProjection = buildUpdateProjection(r, scanPlan, assignments, cond)

      val mergeWrite = mergeBuilder.asWriteBuilder.buildForBatch()
      val writePlan = buildWritePlan(updateProjection, r.table)
      ReplaceData(r, mergeWrite, writePlan)
  }

  private def buildUpdateProjection(
      relation: DataSourceV2Relation,
      scanPlan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = Literal.TrueLiteral): LogicalPlan = {

    // this method relies on the fact that the assignments have been aligned before
    require(relation.output.size == assignments.size, "assignments must be aligned")

    // Spark is going to execute the condition for each column but it seems we cannot avoid this
    val assignedExprs = assignments.map(_.value)
    val updatedExprs = assignedExprs.zip(relation.output).map { case (assignedExpr, attr) =>
      // use semanticEquals to avoid unnecessary if expressions as we may run after operator optimization
      if (attr.semanticEquals(assignedExpr)) {
        attr
      } else if (cond == Literal.TrueLiteral) {
        Alias(assignedExpr, attr.name)()
      } else {
        val updatedExpr = If(cond, assignedExpr, attr)
        Alias(updatedExpr, attr.name)()
      }
    }

    Project(updatedExprs, scanPlan)
  }
}
