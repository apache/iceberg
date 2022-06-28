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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression}
import org.apache.spark.sql.connector.expressions.{SortDirection => V2SortDirection}
import org.apache.spark.sql.connector.expressions.{NullOrdering => V2NullOrdering}
import org.apache.spark.sql.connector.expressions.BucketTransform
import org.apache.spark.sql.connector.expressions.DaysTransform
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.HoursTransform
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.connector.expressions.MonthsTransform
import org.apache.spark.sql.connector.expressions.NamedReference
import org.apache.spark.sql.connector.expressions.SortValue
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.expressions.TruncateTransform
import org.apache.spark.sql.connector.expressions.YearsTransform
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A class that is inspired by V2ExpressionUtils in Spark but supports Iceberg transforms.
 */
object ExtendedV2ExpressionUtils extends SQLConfHelper {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.MultipartIdentifierHelper

  def resolveRef[T <: NamedExpression](ref: NamedReference, plan: LogicalPlan): T = {
    plan.resolve(ref.fieldNames.toSeq, conf.resolver) match {
      case Some(namedExpr) =>
        namedExpr.asInstanceOf[T]
      case None =>
        val name = ref.fieldNames.toSeq.quoted
        val outputString = plan.output.map(_.name).mkString(",")
        throw QueryCompilationErrors.cannotResolveAttributeError(name, outputString)
    }
  }

  def resolveRefs[T <: NamedExpression](refs: Seq[NamedReference], plan: LogicalPlan): Seq[T] = {
    refs.map(ref => resolveRef[T](ref, plan))
  }

  def toCatalyst(expr: V2Expression, query: LogicalPlan): Expression = {
    expr match {
      case SortValue(child, direction, nullOrdering) =>
        val catalystChild = toCatalyst(child, query)
        SortOrder(catalystChild, toCatalyst(direction), toCatalyst(nullOrdering), Seq.empty)
      case IdentityTransform(ref) =>
        resolveRef[NamedExpression](ref, query)
      case t: Transform if BucketTransform.unapply(t).isDefined =>
        t match {
            // sort columns will be empty for bucket.
          case BucketTransform(numBuckets, cols, _) =>
            IcebergBucketTransform(numBuckets, resolveRef[NamedExpression](cols.head, query))
          case _ => t.asInstanceOf[Expression]
            // do nothing
        }
      case TruncateTransform(length, ref) =>
        IcebergTruncateTransform(resolveRef[NamedExpression](ref, query), length)
      case YearsTransform(ref) =>
        IcebergYearTransform(resolveRef[NamedExpression](ref, query))
      case MonthsTransform(ref) =>
        IcebergMonthTransform(resolveRef[NamedExpression](ref, query))
      case DaysTransform(ref) =>
        IcebergDayTransform(resolveRef[NamedExpression](ref, query))
      case HoursTransform(ref) =>
        IcebergHourTransform(resolveRef[NamedExpression](ref, query))
      case ref: FieldReference =>
        resolveRef[NamedExpression](ref, query)
      case _ =>
        throw new AnalysisException(s"$expr is not currently supported")
    }
  }

  private def toCatalyst(direction: V2SortDirection): SortDirection = direction match {
    case V2SortDirection.ASCENDING => Ascending
    case V2SortDirection.DESCENDING => Descending
  }

  private def toCatalyst(nullOrdering: V2NullOrdering): NullOrdering = nullOrdering match {
    case V2NullOrdering.NULLS_FIRST => NullsFirst
    case V2NullOrdering.NULLS_LAST => NullsLast
  }
}
