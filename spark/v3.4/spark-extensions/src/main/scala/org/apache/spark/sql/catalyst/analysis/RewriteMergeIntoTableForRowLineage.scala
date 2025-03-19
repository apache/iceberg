/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommandLike
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object RewriteMergeIntoTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case m@MergeIntoIcebergTable(_, _, _, _, _, Some(rewritePlan)) if updatePlanForRowLineage(m, rewritePlan) => {
        rewritePlan match {
          case writeIcebergDelta@WriteIcebergDelta(_, _, _, _, _) =>
            m.copy(rewritePlan = Some(updateDeltaPlan(writeIcebergDelta)))
          case replaceIcebergData@ReplaceIcebergData(_, _, _, _) =>
            m.copy(rewritePlan = Some(updateReplaceDataPlan(replaceIcebergData)))
          case _: WriteIcebergDelta | _: ReplaceIcebergData => {
            m
          }
        }
      }
      case append@AppendData(table, _, _, _, _, _) =>
        if (append.query.output.exists(attr => attr.name == "_row_id")) {
          append
        } else {
          val dsv2Relation = table.asInstanceOf[DataSourceV2Relation]
          val rowLineageAttributes = rowLineageMetadataAttributes(dsv2Relation.metadataOutput)
          if (rowLineageAttributes.nonEmpty) {
            val additionalRowLineageOutput =
              rowLineageAttributes.zip(Seq("_row_id", "_last_updated_sequence_number"))
                .map { case (expr, name) =>
                  Alias(expr, name)()
                }

            val lineageAttributeRefs = dsv2Relation.metadataOutput.filter(attr => attr.name == "_row_id"
              || attr.name == "_last_updated_sequence_number")
            val project = Project(append.query.output ++ additionalRowLineageOutput, append.query.children.head)
            val updatedAppend = append.withNewTable(
                append.table.asInstanceOf[DataSourceV2Relation]
                  .copy(output = dsv2Relation.output ++ lineageAttributeRefs))
              .withNewQuery(project)

            updatedAppend
          } else {
            append
          }
        }
    }
  }

  private def updatePlanForRowLineage(m: MergeIntoIcebergTable, rewritePlan: LogicalPlan) = {
    rowLineageMetadataAttributes(m.metadataOutput).nonEmpty &&
      rewritePlan.isInstanceOf[V2WriteCommandLike] &&
      !rewritePlan.asInstanceOf[V2WriteCommandLike].table.output.exists(attr => attr.name == "_row_id")
  }


  private def updateReplaceDataPlan(replaceData: ReplaceIcebergData): LogicalPlan = {
    val updatedMergeRows = replaceData.query match {
      case mergeRows@MergeRows(_, _, _, matchedOutputs, _, notMatchedOutputs, _, _, _, _, _) => {
          val updatedMatchOutputs = if (matchedOutputs(0).isEmpty) {
            matchedOutputs
          } else {
            val deleteOutputs = matchedOutputs.head(0)
            val rowIdOrdinal = findMetadataAttributeOrdinal(deleteOutputs, "_row_id")
            val lastUpdatedOrdinal = findMetadataAttributeOrdinal(deleteOutputs, "_last_updated_sequence_number")
            Seq(
              Seq(deleteOutputs.updated(rowIdOrdinal,
                deleteOutputs(rowIdOrdinal)).updated(lastUpdatedOrdinal, Literal(null)))
            )
          }

        val updatedNotMatchedOutputs = if (notMatchedOutputs.nonEmpty) {
          val deleteOutputs = matchedOutputs.head(0)
          val rowIdOrdinal = findMetadataAttributeOrdinal(deleteOutputs, "_row_id")
          val lastUpdatedOrdinal = findMetadataAttributeOrdinal(deleteOutputs, "_last_updated_sequence_number")
            Seq(notMatchedOutputs(0)
              .updated(rowIdOrdinal, Literal(null)).updated(lastUpdatedOrdinal, Literal(null)))
          } else {
            notMatchedOutputs
          }

        mergeRows.copy(
          matchedOutputs = updatedMatchOutputs,
          notMatchedOutputs = updatedNotMatchedOutputs)
      }

      case p => throw new UnsupportedOperationException(s"Unsupported $p")
    }
    val rowLineageAttributes = replaceData.originalTable.metadataOutput.filter(attr => attr.name == "_row_id"
      || attr.name == "_last_updated_sequence_number")

    ReplaceIcebergData(
      replaceData.table.asInstanceOf[DataSourceV2Relation]
        .copy(output = replaceData.table.asInstanceOf[DataSourceV2Relation].output
          ++ rowLineageAttributes.map(attr => attr.asInstanceOf[AttributeReference])),
      updatedMergeRows,
      replaceData.originalTable,
      replaceData.write)
  }

  private def findMetadataAttributeOrdinal(outputs: Seq[Expression], name: String): Int = {
    outputs.indexWhere(attr => attr.isInstanceOf[Attribute]
      && attr.asInstanceOf[Attribute].name == name
      && attr.asInstanceOf[Attribute].metadata.contains(METADATA_COL_ATTR_KEY))
  }
}
