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
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object RewriteUpdateTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case updateTable@UpdateTable(_, _, _) if shouldUpdatePlan(updateTable) =>
        updatePlanWithRowLineage(updateTable)
    }
  }

  // The plan should only be updated if row lineage metadata attributes are present
  // in the target table AND lineage attributes are not already
  // on the output of operation which indicates the rule already ran
  private def shouldUpdatePlan(updateTable: UpdateTable): Boolean = {
    val rowLineageAttrs = findRowLineageAttributes(updateTable.metadataOutput)
    val allLineageAttrsPresent = rowLineageAttrs.nonEmpty && rowLineageAttrs.forall(updateTable.metadataOutput.contains)
    val rowIdAbsentFromOutput = !updateTable.output.exists(_.name == ROW_ID_ATTRIBUTE_NAME)

    allLineageAttrsPresent && rowIdAbsentFromOutput
  }


  private def updatePlanWithRowLineage(updateTable: UpdateTable): LogicalPlan = {
    EliminateSubqueryAliases(updateTable.table) match {
      case r @ DataSourceV2Relation(_: SupportsRowLevelOperations, _, _, _, _) =>
        val rowLineageAttributes = findRowLineageAttributes(updateTable.metadataOutput)
        val lastUpdatedSequence = rowLineageAttributes.filter(
          attr => attr.name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME).head

        val lineageAssignments = updateTable.assignments ++
          Seq(Assignment(lastUpdatedSequence, Literal(null)))

        val rowLineageAsDataColumns = rowLineageAttributes.map(removeMetadataColumnAttribute)
        val tableWithLineage = r.copy(output = r.output ++ rowLineageAsDataColumns)

        updateTable.copy(table = tableWithLineage, assignments = lineageAssignments)
    }
  }
}
