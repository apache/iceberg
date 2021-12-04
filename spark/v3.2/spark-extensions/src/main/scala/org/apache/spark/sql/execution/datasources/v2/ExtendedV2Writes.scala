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

package org.apache.spark.sql.execution.datasources.v2

import java.util.UUID
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.utils.PlanUtils.isIcebergRelation
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl
import org.apache.spark.sql.connector.write.SupportsDynamicOverwrite
import org.apache.spark.sql.connector.write.SupportsOverwrite
import org.apache.spark.sql.connector.write.SupportsTruncate
import org.apache.spark.sql.connector.write.WriteBuilder
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.AlwaysTrue
import org.apache.spark.sql.sources.Filter

/**
 * A rule that is inspired by V2Writes in Spark but supports Iceberg transforms.
 */
object ExtendedV2Writes extends Rule[LogicalPlan] with PredicateHelper {

  import DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case a @ AppendData(r: DataSourceV2Relation, query, options, _, None) if isIcebergRelation(r) =>
      val writeBuilder = newWriteBuilder(r.table, query, options)
      val write = writeBuilder.build()
      val newQuery = ExtendedDistributionAndOrderingUtils.prepareQuery(write, query, conf)
      a.copy(write = Some(write), query = newQuery)

    case o @ OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, options, _, None)
        if isIcebergRelation(r) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).flatMap { pred =>
        val filter = DataSourceStrategy.translateFilter(pred, supportNestedPredicatePushdown = true)
        if (filter.isEmpty) {
          throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(pred)
        }
        filter
      }.toArray

      val table = r.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsTruncate if isTruncate(filters) =>
          builder.truncate().build()
        case builder: SupportsOverwrite =>
          builder.overwrite(filters).build()
        case _ =>
          throw QueryExecutionErrors.overwriteTableByUnsupportedExpressionError(table)
      }

      val newQuery = ExtendedDistributionAndOrderingUtils.prepareQuery(write, query, conf)
      o.copy(write = Some(write), query = newQuery)

    case o @ OverwritePartitionsDynamic(r: DataSourceV2Relation, query, options, _, None)
        if isIcebergRelation(r) =>
      val table = r.table
      val writeBuilder = newWriteBuilder(table, query, options)
      val write = writeBuilder match {
        case builder: SupportsDynamicOverwrite =>
          builder.overwriteDynamicPartitions().build()
        case _ =>
          throw QueryExecutionErrors.dynamicPartitionOverwriteUnsupportedByTableError(table)
      }
      val newQuery = ExtendedDistributionAndOrderingUtils.prepareQuery(write, query, conf)
      o.copy(write = Some(write), query = newQuery)
  }

  private def isTruncate(filters: Array[Filter]): Boolean = {
    filters.length == 1 && filters(0).isInstanceOf[AlwaysTrue]
  }

  private def newWriteBuilder(
      table: Table,
      query: LogicalPlan,
      writeOptions: Map[String, String]): WriteBuilder = {

    val info = LogicalWriteInfoImpl(
      queryId = UUID.randomUUID().toString,
      query.schema,
      writeOptions.asOptions)
    table.asWritable.newWriteBuilder(info)
  }
}
