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

import org.apache.iceberg.expressions.{Expression => IcebergExpression}
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.geo.GeospatialLibraryAccessor
import org.apache.iceberg.spark.source.SparkBatchQueryScan
import org.apache.iceberg.spark.source.SparkScan
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.Or
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.PushableColumn
import org.apache.spark.sql.execution.datasources.PushableColumnBase
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import scala.annotation.tailrec

/**
 * Push down spatial predicates such as to iceberg relation scan node [[SparkBatchQueryScan]].
 */
object SpatialPredicatePushDown extends Rule[LogicalPlan] with PredicateHelper {

  import scala.collection.JavaConverters._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, scanRel: DataSourceV2ScanRelation) if isIcebergRelation(scanRel.relation) =>
      val scan = scanRel.scan.asInstanceOf[SparkScan]
      val filters = splitConjunctivePredicates(condition)
      val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, scanRel.output)
      val (_, normalizedFiltersWithoutSubquery) = normalizedFilters.partition(SubqueryExpression.hasSubquery)
      val icebergSpatialPredicates = filtersToIcebergSpatialPredicates(
        normalizedFiltersWithoutSubquery, nestedPredicatePushdownEnabled = true)
      val newScan = scan.withExpressions(icebergSpatialPredicates.asJava)
      if (newScan != scan) {
        Filter(condition, scanRel.copy(scan = newScan))
      } else {
        filter
      }
  }

  @tailrec
  private def isIcebergRelation(plan: LogicalPlan): Boolean = {
    def isIcebergTable(relation: DataSourceV2Relation): Boolean = relation.table match {
      case t: RowLevelOperationTable => t.table.isInstanceOf[SparkTable]
      case _: SparkTable => true
      case _ => false
    }

    plan match {
      case s: SubqueryAlias => isIcebergRelation(s.child)
      case r: DataSourceV2Relation => isIcebergTable(r)
      case _ => false
    }
  }

  private def filtersToIcebergSpatialPredicates(
                                         predicates: Seq[Expression],
                                         nestedPredicatePushdownEnabled: Boolean): Seq[IcebergExpression] = {
    val pushableColumn = PushableColumn(nestedPredicatePushdownEnabled)
    predicates.flatMap { predicate => translateToIcebergSpatialPredicate(predicate, pushableColumn) }
  }

  private def translateToIcebergSpatialPredicate(
                                          predicate: Expression,
                                          pushableColumn: PushableColumnBase): Option[IcebergExpression] = {
    val skippingEligibleSpatialPredicate = SkippingEligibleSpatialPredicate(pushableColumn)
    predicate match {
      case And(left, right) =>
        val icebergLeft = translateToIcebergSpatialPredicate(left, pushableColumn)
        val icebergRight = translateToIcebergSpatialPredicate(right, pushableColumn)
        (icebergLeft, icebergRight) match {
          case (Some(left), Some(right)) => Some(Expressions.and(left, right))
          case (Some(left), None) => Some(left)
          case (None, Some(right)) => Some(right)
          case _ => None
        }

      case Or(left, right) =>
        for {
          icebergLeft <- translateToIcebergSpatialPredicate(left, pushableColumn)
          icebergRight <- translateToIcebergSpatialPredicate(right, pushableColumn)
        } yield Expressions.or(icebergLeft, icebergRight)

      case Not(innerPredicate) =>
        translateToIcebergSpatialPredicate(innerPredicate, pushableColumn).map(Expressions.not)

      case skippingEligibleSpatialPredicate(icebergExpr) =>
        Some(icebergExpr)

      case skippingEligibleSpatialPredicate(icebergExpr) =>
        Some(icebergExpr)

      case _ => None
    }
  }

  private case class SkippingEligibleSpatialPredicate(pushableColumn: PushableColumnBase) {
    def unapply(expr: Expression): Option[IcebergExpression] = {
      if (GeospatialLibraryAccessor.isSpatialFilter(expr)) {
        val icebergExpr = GeospatialLibraryAccessor.translateToIceberg(expr)
        Option(icebergExpr)
      } else {
        None
      }
    }
  }
}
