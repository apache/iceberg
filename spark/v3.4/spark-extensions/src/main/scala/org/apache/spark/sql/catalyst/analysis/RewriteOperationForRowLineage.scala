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

import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.UpdateRows
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommandLike
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

trait RewriteOperationForRowLineage extends RewriteRowLevelIcebergCommand {

  protected val ROW_ID_ATTRIBUTE_NAME = "_row_id"
  protected val LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME = "_last_updated_sequence_number"

  protected val ROW_LINEAGE_ATTRIBUTES = Set(
    ROW_ID_ATTRIBUTE_NAME, LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)

  protected def updateDeltaPlan(writeIcebergDelta: WriteIcebergDelta): LogicalPlan = {
    val rowLineageAttributes = findRowLineageAttributes(writeIcebergDelta.originalTable.metadataOutput).map(_._1)
    writeIcebergDelta.query match {
      case mergeRows@MergeRows(_, _, _, matchedOutputs, _, notMatchedOutputs, _, _, _, _, _) => {
        val rowIdOrdinal = findLineageAttribute(matchedOutputs(0)(0), ROW_ID_ATTRIBUTE_NAME)
        val lastUpdatedIdOrdinal = findLineageAttribute(
          matchedOutputs(0)(0), LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)

        /** There are 3 cases for matched outputs.
         * It'll either have 2 sequences (1 for delete + 1 for insert for representing update) OR
         * The output will only have 1 element if it's only a delete OR
         * there simply are no matched outputs.
         * In the first case, we modify the insert outputs to preserve row IDs and null out last updated
         * In the delete and no matched clause cases, nothing needs to be done */

        val updatedMatchOutputs = if (matchedOutputs(0).length == 2) {
          Seq(
            Seq(matchedOutputs(0)(0),
              // Set the metadata attribute to itself so it's preserved
              matchedOutputs(0)(1).updated(rowIdOrdinal, matchedOutputs(0)(0)(rowIdOrdinal))
                .updated(lastUpdatedIdOrdinal, Literal(null)))
          )
        } else {
          matchedOutputs
        }

        /** There are 2 cases for not matched outputs.
         * It'll either have a single sequence with only inserts,
         * in which case both lineage attributes should be nulled out
         * In the other case, there simply is no not matched clause */
        val updatedNotMatchedOutputs = if (notMatchedOutputs.nonEmpty) {
          Seq(notMatchedOutputs(0)
            .updated(rowIdOrdinal, Literal(null))
            .updated(lastUpdatedIdOrdinal, Literal(null)))
        } else {
          notMatchedOutputs
        }

        val updatedMergeRows =
          mergeRows.copy(matchedOutputs = updatedMatchOutputs, notMatchedOutputs = updatedNotMatchedOutputs)
        val updatedProjections = buildWriteDeltaProjectionsWithRowLineage(
          writeIcebergDelta,
          updatedMergeRows.output,
          updatedMergeRows.matchedOutputs.flatten ++ updatedMergeRows.notMatchedOutputs,
          rowLineageAttributes)

        buildUpdatedWriteDeltaPlan(writeIcebergDelta, updatedMergeRows, updatedProjections, rowLineageAttributes)
      }

      case updateRows@UpdateRows(deleteOutput, insertOutput, _, _) => {
        val rowIdOrdinal = findLineageAttribute(deleteOutput, ROW_ID_ATTRIBUTE_NAME)
        val lastUpdatedIdOrdinal = findLineageAttribute(deleteOutput, LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)

        val updatedInsertRows = insertOutput
          .updated(rowIdOrdinal, deleteOutput(rowIdOrdinal))
          .updated(lastUpdatedIdOrdinal, Literal(null))
        val updatedRows = updateRows.copy(insertOutput = updatedInsertRows)
        val updatedProjections = buildWriteDeltaProjectionsWithRowLineage(
          writeIcebergDelta,
          updatedRows.output,
          Seq(updatedRows.deleteOutput, updatedRows.insertOutput), rowLineageAttributes)
        buildUpdatedWriteDeltaPlan(writeIcebergDelta, updatedRows, updatedProjections, rowLineageAttributes)
      }
    }
  }

  protected def buildUpdatedWriteDeltaPlan(
      writeDeltaPlan: WriteIcebergDelta,
      updatedQuery: LogicalPlan,
      projections: WriteDeltaProjections,
      rowLineageAttributes: Seq[Attribute]): WriteIcebergDelta = {
    writeDeltaPlan.copy(
      table = writeDeltaPlan.table.asInstanceOf[DataSourceV2Relation]
        .copy(output = writeDeltaPlan.table.asInstanceOf[DataSourceV2Relation].output
          ++ rowLineageAttributes.map(attr => attr.asInstanceOf[AttributeReference])),
      query = updatedQuery,
      projections = projections
    )
  }

  protected def buildWriteDeltaProjectionsWithRowLineage(
      writeIcebergDelta: WriteIcebergDelta,
      outputAttrs: Seq[Attribute],
      outputs: Seq[Seq[Expression]],
      rowLineageAttributes: Seq[Attribute]): WriteDeltaProjections = {
    val baseProjections = writeIcebergDelta.projections
    val projectedAttributes = writeIcebergDelta.table.output ++ rowLineageAttributes
    val projectedOrdinals = projectedAttributes.map(attr => outputAttrs.indexWhere(_.name == attr.name))
    val structFields = projectedAttributes.zip(projectedOrdinals).map { case (attr, ordinal) =>
      val nullable = outputs.exists(output => output(ordinal).nullable)
      StructField(attr.name, attr.dataType, nullable, attr.metadata)
    }

    val schema = StructType(structFields)
    val newProjection = ProjectingInternalRow(schema, projectedOrdinals)

    WriteDeltaProjections(
      Some(newProjection),
      baseProjections.rowIdProjection,
      baseProjections.metadataProjection
    )
  }


  protected def updatePlanForRowLineage(plan: LogicalPlan, rewritePlan: LogicalPlan) = {
    rowLineageMetadataAttributes(plan.metadataOutput).nonEmpty &&
      rewritePlan.isInstanceOf[V2WriteCommandLike] &&
      !rewritePlan.asInstanceOf[V2WriteCommandLike].table.output.exists(attr => attr.name == ROW_ID_ATTRIBUTE_NAME)
  }

  protected def rowLineageMetadataAttributes(metadataAttributes: Seq[Attribute]): Seq[Attribute] = {
    metadataAttributes.filter(attr => ROW_LINEAGE_ATTRIBUTES.contains(attr.name))
  }

  protected def findRowLineageAttributes(expressions: Seq[Expression]): Seq[(Attribute, Int)] = {
    expressions.zipWithIndex.filter {
      case (expr, _) => expr.isInstanceOf[Attribute] &&
        ROW_LINEAGE_ATTRIBUTES.contains(expr.asInstanceOf[Attribute].name) &&
        expr.asInstanceOf[Attribute].metadata.contains(METADATA_COL_ATTR_KEY)
    }.map(tup => (tup._1.asInstanceOf[Attribute], tup._2))
  }

  protected def findLineageAttribute(expressions: Seq[Expression], lineageAttribute: String): Int = {
    findRowLineageAttributes(expressions)
      .collectFirst {
        case (attr, ordinal) if attr.name == lineageAttribute => ordinal
      }.get
  }
}
