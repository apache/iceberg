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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.Metadata

object RewriteUpdateTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    RewriteUpdateTable.apply(plan) resolveOperators {
      case writeDelta@WriteDelta(_, _, _, _, _, _) if updatePlanForRowLineage(writeDelta) => {
        updateDeltaPlan(writeDelta)
      }

      case replaceData@ReplaceData(_, _, _, _, _, _) if updatePlanForRowLineage(replaceData) => {
        updateReplaceDataPlan(replaceData)
      }
    }
  }

  def updateDeltaPlan(writeDelta: WriteDelta): LogicalPlan = {
    writeDelta.query match {
      case Expand(projections, output, child) => {
        val rowLineageAttributes = findRowLineageAttributes(output).map(_._1)
        val rowIdOrdinal = findLineageAttribute(output, ROW_ID_ATTRIBUTE_NAME)
        val lastUpdatedIdOrdinal = findLineageAttribute(output, LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)
        val updatedInsertProjections = projections(1)
          .updated(rowIdOrdinal, projections(0)(rowIdOrdinal))
          .updated(lastUpdatedIdOrdinal, Literal(null))
        val updatedExpand = Expand(Seq(projections.head, updatedInsertProjections), output, child)
        val tableAsDsv2 = writeDelta.table.asInstanceOf[DataSourceV2Relation]
        val relation =
          writeDelta.originalTable.asInstanceOf[DataSourceV2Relation]
        val rowAttrs = relation.output
        val operation = writeDelta.operation
        val rowIdAttrs = resolveRowIdAttrs(relation, operation)
        val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)
        val updatedProjections = buildWriteDeltaProjections(updatedExpand,
          rowAttrs ++ rowLineageAttributes, rowIdAttrs, metadataAttrs)
        WriteDelta(updateDsv2RelationOutputWithRowLineage(
          tableAsDsv2,
          rowLineageAttributes),
          condition = writeDelta.condition,
          query = updatedExpand,
          originalTable = writeDelta.originalTable,
          projections = updatedProjections
        )
      }
      case p => throw new UnsupportedOperationException("Unexpected plan for query " + p)
    }
  }

  def updateReplaceDataPlan(replaceData: ReplaceData): LogicalPlan = {
    replaceData.query match {
      case Union(children, byName, allowingMissingCol) => {
        val updatedProjection = children(0) match {
          case Project(projectList, child) => {
            val updatedProjectionList =
              updateRowLineageProjections(projectList, replaceData.condition)

            Project(updatedProjectionList, child)
          }
          case p => throw new UnsupportedOperationException("Unexpected plan for query " + p)
        }
        val updatedUnionProjection = children.updated(0, updatedProjection)
        val updatedQuery = Union(updatedUnionProjection, byName, allowingMissingCol)

        buildUpdatedReplaceDataPlan(replaceData, updatedQuery)
      }
      case Project(projectList, child) => {
        val updatedProjections = updateRowLineageProjections(projectList, replaceData.condition)

        val updated = buildUpdatedReplaceDataPlan(replaceData, Project(updatedProjections, child))
        // Tricks Spark into treating the output row lineage attributes as data columns so that
        // analysis succeeds
        updated
      }

      case p => throw new UnsupportedOperationException("Unexpected plan for query " + p)

    }
  }

  protected def updateRowLineageProjections(
      projections: Seq[NamedExpression],
      condition: Expression): Seq[NamedExpression] = {
    projections.map {
      case attr @ AttributeReference(name, _, _, metadata)
        if ROW_LINEAGE_ATTRIBUTES(name) && metadata.contains(METADATA_COL_ATTR_KEY) =>
        // Only null out the last updated sequence number based on the condition
        if (name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME) {
          Alias(If(condition, Literal(null), attr.withMetadata(Metadata.empty)), name)()
        } else {
          // Tricks Spark into treating the output
          // row lineage attributes as data columns so that  analysis succeeds
          attr.withMetadata(Metadata.empty)
        }
      case other => other
    }
  }
}
