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

package org.apache.spark.sql.catalyst.analysis

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * A rule to add a projection on top of the source table of MergeRows to remove unnecessary
 * metadata columns reading.
 */
object RemoveUnusedMetadataColumns extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case mergeRows: MergeRows if mergeRows.resolved =>
      mergeRows.child match {
        case join @ Join(left, right, _, _, _) if isIcebergSourcePlan(left, conf.resolver) =>
          val projected = projectWithoutMetadata(left)
          mergeRows.withNewChildren(Seq(join.withNewChildren(Seq(projected, right))))

        case join @ Join(left, right, _, _, _) if isIcebergSourcePlan(right, conf.resolver) =>
          val projected = projectWithoutMetadata(right)
          mergeRows.withNewChildren(Seq(join.withNewChildren(Seq(left, projected))))

        case _ => mergeRows
      }
  }

  private def projectWithoutMetadata(plan: LogicalPlan): LogicalPlan = {

    import org.apache.spark.sql.catalyst.util.MetadataColumnHelper

    val outputWithoutMetadata = plan.output.filterNot(col => col.isMetadataCol)
    if (outputWithoutMetadata.nonEmpty && outputWithoutMetadata.size != plan.output.size) {
      Project(outputWithoutMetadata, plan)
    } else {
      plan
    }
  }

  private def isIcebergSourcePlan(plan: LogicalPlan, resolve: Resolver): Boolean = {
    plan.output.exists(col => resolve(col.name, RewriteMergeIntoTable.ROW_FROM_SOURCE)) &&
      onlyHasIcebergRelation(plan)
  }

  private def onlyHasIcebergRelation(plan: LogicalPlan): Boolean = {
    val icebergRelations = plan.collect {
      case node: LeafNode =>
        node
    }

    icebergRelations.forall {
      case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] =>
        true
      case _ => false
    }
  }
}
