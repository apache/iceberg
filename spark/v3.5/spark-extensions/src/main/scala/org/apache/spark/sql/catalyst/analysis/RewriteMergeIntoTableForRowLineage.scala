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
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object RewriteMergeIntoTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case m @ MergeIntoTable(_, _, _, matchedActions, _, notMatchedBySourceActions)
        if m.resolved && m.rewritable && m.aligned &&
          (matchedActions.nonEmpty || notMatchedBySourceActions.nonEmpty) &&
          shouldUpdatePlan(m) =>
        updateMergeIntoForRowLineage(m)
    }
  }

  protected def updateMergeIntoForRowLineage(mergeIntoTable: MergeIntoTable): LogicalPlan = {
    EliminateSubqueryAliases(mergeIntoTable.targetTable) match {
      case r: DataSourceV2Relation =>
        val matchedActions = mergeIntoTable.matchedActions
        val notMatchedBySourceActions = mergeIntoTable.notMatchedBySourceActions
        val rowLineageAttributes = findRowLineageAttributes(r.metadataOutput)
        val rowId = rowLineageAttributes.filter(
          attr => attr.name == ROW_ID_ATTRIBUTE_NAME).head
        val lastUpdatedSequence = rowLineageAttributes.filter(
          attr => attr.name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME).head

        val matchedAssignmentsForLineage = matchedActions.map {
          case UpdateAction(cond, actions) =>
            UpdateAction(cond, actions ++ Seq(Assignment(rowId, rowId),
              Assignment(lastUpdatedSequence, Literal(null))))

          case p => p
        }

        val notMatchedBySourceActionsForLineage = notMatchedBySourceActions.map {
          case UpdateAction(cond, actions) =>
            UpdateAction(cond, actions ++ Seq(Assignment(rowId, rowId),
              Assignment(lastUpdatedSequence, Literal(null))))

          case p => p
        }

        // Treat row lineage columns as data columns by removing the metadata attribute
        // This works around the logic in
        // ExposesMetadataColumns, used later in metadata attribute resolution,
        // which prevents surfacing other metadata columns when a single metadata column is in the output
        val rowLineageAsDataColumns = rowLineageAttributes.map(removeMetadataColumnAttribute)

        val tableWithLineage = r.copy(output =
          r.output ++ rowLineageAsDataColumns)

        mergeIntoTable.copy(
          targetTable = tableWithLineage,
          matchedActions = matchedAssignmentsForLineage,
          notMatchedBySourceActions = notMatchedBySourceActionsForLineage)
    }
  }

  // The plan should only be updated if row lineage metadata attributes are present
  // in the target table AND lineage attributes are not already
  // on the output of operation which indicates the rule already ran
  private def shouldUpdatePlan(mergeIntoTable: MergeIntoTable): Boolean = {
    val metadataOutput = mergeIntoTable.targetTable.metadataOutput
    val rowLineageAttrs = findRowLineageAttributes(metadataOutput)
    val allLineageAttrsPresent = rowLineageAttrs.nonEmpty && rowLineageAttrs.forall(metadataOutput.contains)
    val rowIdAbsentFromOutput = !mergeIntoTable.targetTable.output.exists(_.name == ROW_ID_ATTRIBUTE_NAME)

    allLineageAttrsPresent && rowIdAbsentFromOutput
  }
}
