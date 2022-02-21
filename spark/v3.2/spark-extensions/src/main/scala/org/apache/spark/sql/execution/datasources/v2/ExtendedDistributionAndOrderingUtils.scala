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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExtendedV2ExpressionUtils.toCatalyst
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RepartitionByExpression
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.connector.distributions.ClusteredDistribution
import org.apache.spark.sql.connector.distributions.OrderedDistribution
import org.apache.spark.sql.connector.distributions.UnspecifiedDistribution
import org.apache.spark.sql.connector.write.RequiresDistributionAndOrdering
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import scala.collection.compat.immutable.ArraySeq

/**
 * A rule that is inspired by DistributionAndOrderingUtils in Spark but supports Iceberg transforms.
 *
 * Note that similarly to the original rule in Spark, it does not let AQE pick the number of shuffle
 * partitions. See SPARK-34230 for context.
 */
object ExtendedDistributionAndOrderingUtils {

  def prepareQuery(write: Write, query: LogicalPlan, conf: SQLConf): LogicalPlan = write match {
    case write: RequiresDistributionAndOrdering =>
      val numPartitions = write.requiredNumPartitions()
      val distribution = write.requiredDistribution match {
        case d: OrderedDistribution => d.ordering.map(e => toCatalyst(e, query))
        case d: ClusteredDistribution => d.clustering.map(e => toCatalyst(e, query))
        case _: UnspecifiedDistribution => Array.empty[Expression]
      }

      val queryWithDistribution = if (distribution.nonEmpty) {
        val finalNumPartitions = if (numPartitions > 0) {
          numPartitions
        } else {
          conf.numShufflePartitions
        }
        // the conversion to catalyst expressions above produces SortOrder expressions
        // for OrderedDistribution and generic expressions for ClusteredDistribution
        // this allows RepartitionByExpression to pick either range or hash partitioning
        RepartitionByExpression(ArraySeq.unsafeWrapArray(distribution), query, finalNumPartitions)
      } else if (numPartitions > 0) {
        throw QueryCompilationErrors.numberOfPartitionsNotAllowedWithUnspecifiedDistributionError()
      } else {
        query
      }

      val ordering = write.requiredOrdering.toSeq
        .map(e => toCatalyst(e, query))
        .asInstanceOf[Seq[SortOrder]]

      val queryWithDistributionAndOrdering = if (ordering.nonEmpty) {
        Sort(ordering, global = false, queryWithDistribution)
      } else {
        queryWithDistribution
      }

      queryWithDistributionAndOrdering

    case _ =>
      query
  }
}
