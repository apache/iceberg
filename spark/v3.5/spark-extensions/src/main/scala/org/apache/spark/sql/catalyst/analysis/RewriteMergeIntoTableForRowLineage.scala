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

import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.Metadata

object RewriteMergeIntoTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    RewriteMergeIntoTable.apply(plan) resolveOperators {
      case writeDelta@WriteDelta(_, _, _, _, _, _) if updatePlanForRowLineage(writeDelta) =>
        updateDeltaPlan(writeDelta)

      case replaceData@ReplaceData(_, _, _, _, _, _) if updatePlanForRowLineage(replaceData) =>
        updateReplaceDataPlan(replaceData)
    }
  }

  protected def updateDeltaPlan(writeDelta: WriteDelta): LogicalPlan = {
    val rowLineageAttributes = findRowLineageAttributes(
      writeDelta.originalTable.metadataOutput
    ).map(_._1)
    writeDelta.query match {
      case mergeRows @ MergeRows(
      _,
      _,
      matchedInstructions,
      notMatchedInstructions,
      notMatchedBySourceInstructions,
      _,
      _,
      _
      ) => {
        // At this point there will be at least one of matchedInstructions or notMatchedBySource instructions
        // Whichever exists will be used to determine the ordinals of rowId/lastUpdatedSequenceNumber
        val expressionToSearch = if (matchedInstructions.isEmpty) {
          notMatchedBySourceInstructions.head.outputs(0)
        } else {
          matchedInstructions.head.outputs(0)
        }

        val rowIdOrdinal =
          findLineageAttribute(expressionToSearch, ROW_ID_ATTRIBUTE_NAME)
        val lastUpdatedIdOrdinal = findLineageAttribute(
          expressionToSearch,
          LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME
        )

        val updatedMatchedInstructions = matchedInstructions.map {
          case updateSplit: MergeRows.Split =>

            val matchedOutputs = updateSplit.outputs
            val deleteInstructions = matchedOutputs(0)
            val insertInstructions = matchedOutputs(1)
            // Preserve rowID and null last updated sequence
            val updatedInsert = insertInstructions
              .updated(rowIdOrdinal, matchedOutputs(0)(rowIdOrdinal))
              .updated(lastUpdatedIdOrdinal, Literal(null))
            MergeRows.Split(
              updateSplit.condition,
              deleteInstructions,
              updatedInsert
            )
          case otherInstruction => otherInstruction
        }

        val updatedNotMatchedInstructions =
          if (notMatchedInstructions.isEmpty) {
            Seq.empty
          } else {
            val notMatchedOutputs = notMatchedInstructions.head.outputs
            // Not matched will always be inserts, null out both rowID and last updated
            val insertOutputs = notMatchedOutputs(0)
              .updated(rowIdOrdinal, Literal(null))
              .updated(lastUpdatedIdOrdinal, Literal(null))
            Seq(
              MergeRows.Keep(
                notMatchedInstructions.head.condition,
                insertOutputs
              )
            )
          }

        val updatedNotMatchedBySourceInstructions = notMatchedBySourceInstructions.map {
          case updateSplit: MergeRows.Split =>

            val matchedOutputs = updateSplit.outputs
            val deleteInstructions = matchedOutputs(0)
            val insertInstructions = matchedOutputs(1)

            // Preserve rowID and null last updated sequence
            val updatedInsert = insertInstructions
              .updated(rowIdOrdinal, matchedOutputs(0)(rowIdOrdinal))
              .updated(lastUpdatedIdOrdinal, Literal(null))
            MergeRows.Split(
              updateSplit.condition,
              deleteInstructions,
              updatedInsert
            )
          case otherInstruction => otherInstruction
        }

        val updatedMergeRows =
          mergeRows.copy(
            matchedInstructions = updatedMatchedInstructions,
            notMatchedInstructions = updatedNotMatchedInstructions,
            notMatchedBySourceInstructions = updatedNotMatchedBySourceInstructions
          )

        val operation = writeDelta.operation
        val relation =
          writeDelta.originalTable.asInstanceOf[DataSourceV2Relation]
        val rowAttrs = relation.output
        val rowIdAttrs = resolveRowIdAttrs(relation, operation)
        val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

        val updatedProjections = buildWriteDeltaProjections(
          updatedMergeRows,
          rowAttrs ++ rowLineageAttributes,
          rowIdAttrs,
          metadataAttrs
        )

        buildUpdatedWriteDeltaPlan(
          writeDelta,
          updatedMergeRows,
          updatedProjections
        )
      }
    }
  }

  protected def updateReplaceDataPlan(replaceData: ReplaceData): LogicalPlan = {
    replaceData.query match {
      case mergeRows @ MergeRows(
      _,
      _,
      matchedInstructions,
      notMatchedInstructions,
      notMatchedBySourceInstructions,
      _,
      _,
      _
      ) => {
        val keepInstructionOutput = matchedInstructions.collectFirst {
          case keep: Keep => keep.output
        }.get

        val rowIdOrdinal =
          findLineageAttribute(keepInstructionOutput, ROW_ID_ATTRIBUTE_NAME)
        val lastUpdatedIdOrdinal = findLineageAttribute(
          keepInstructionOutput,
          LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME
        )

        val updatedMatchedInstructions = if (matchedInstructions.isEmpty) {
          matchedInstructions
        } else {
          matchedInstructions.init.map {
            case Keep(cond, output) =>
              Keep(cond, output.updated(lastUpdatedIdOrdinal, Literal(null)))
            case p => p
          } :+ matchedInstructions.last
        }

        val updatedNotMatchedInstructions =
          if (notMatchedInstructions.isEmpty) {
            Seq.empty
          } else {
            val notMatchedOutputs = notMatchedInstructions.head.outputs
            val insertOutputs = notMatchedOutputs(0)
              .updated(rowIdOrdinal, Literal(null))
              .updated(lastUpdatedIdOrdinal, Literal(null))
            Seq(
              MergeRows.Keep(
                notMatchedInstructions.head.condition,
                insertOutputs
              )
            )
          }

        val updatedNotMatchedBySourceInstructions = if (notMatchedBySourceInstructions.isEmpty) {
          notMatchedBySourceInstructions
        } else {
          notMatchedBySourceInstructions.init.map {
            case Keep(cond, output) =>
              Keep(cond, output.updated(lastUpdatedIdOrdinal, Literal(null)))
            case p => p
          } :+ notMatchedBySourceInstructions.last
        }

        // Tricks Spark into treating the output row lineage attributes as data columns so that
        // analysis succeeds
        val updatedOutput = mergeRows.output.map {
          case attr if attr.name == ROW_ID_ATTRIBUTE_NAME =>
            attr.withMetadata(Metadata.empty)
          case attr
            if attr.name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME =>
            attr.withMetadata(Metadata.empty)
          case p => p
        }

        val updatedMergeRows =
          mergeRows.copy(
            matchedInstructions = updatedMatchedInstructions,
            notMatchedInstructions = updatedNotMatchedInstructions,
            notMatchedBySourceInstructions = updatedNotMatchedBySourceInstructions,
            output = updatedOutput
          )

        buildUpdatedReplaceDataPlan(
          replaceDataPlan = replaceData,
          updatedQuery = updatedMergeRows
        )
      }
    }
  }
}
