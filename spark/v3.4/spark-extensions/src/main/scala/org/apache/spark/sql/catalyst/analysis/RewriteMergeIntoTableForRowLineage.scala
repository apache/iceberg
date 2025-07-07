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
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object RewriteMergeIntoTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case m @ MergeIntoIcebergTable(_, _, _, matchedActions, _, _)
          if m.resolved && m.aligned &&
            matchedActions.nonEmpty &&
            shouldUpdatePlan(m.targetTable) =>
        updateMergeIntoForRowLineage(m)
    }
  }

  protected def updateMergeIntoForRowLineage(mergeIntoTable: MergeIntoIcebergTable): LogicalPlan = {
    EliminateSubqueryAliases(mergeIntoTable.targetTable) match {
      case r: DataSourceV2Relation =>
        val matchedActions = mergeIntoTable.matchedActions
        val notMatchedActions = mergeIntoTable.notMatchedActions
        val (rowId, lastUpdatedSequenceNumber) = findRowLineageAttributes(r.metadataOutput).get

        val matchedAssignmentsForLineage = matchedActions.map {
          case UpdateAction(cond, assignments) =>
            UpdateAction(
              cond,
              assignments ++ Seq(
                Assignment(rowId, rowId),
                Assignment(lastUpdatedSequenceNumber, Literal(null))))

          case deleteAction => deleteAction
        }

        val notMatchedActionsForLineage = notMatchedActions.map {
          case InsertAction(cond, assignments) =>
            InsertAction(
              cond,
              assignments ++ Seq(
                Assignment(rowId, Literal(null)),
                Assignment(lastUpdatedSequenceNumber, Literal(null))))
        }

        val tableWithLineage = r.copy(output = r.output ++ Seq(rowId, lastUpdatedSequenceNumber))
        mergeIntoTable.copy(
          targetTable = tableWithLineage,
          matchedActions = matchedAssignmentsForLineage,
          notMatchedActions = notMatchedActionsForLineage)
    }
  }
}
