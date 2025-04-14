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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UpdateTable
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.Metadata

object RewriteUpdateTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case updateTable@UpdateTable(table, assignments, condition) if shouldUpdatePlan(updateTable) => {
        updatePlanWithRowLineage(updateTable)
      }
    }
  }

  private def shouldUpdatePlan(updateTable: UpdateTable): Boolean = {
    val rowLineageAttributes = findRowLineageAttributes(updateTable.metadataOutput).map(tup => tup._1)
    rowLineageAttributes.nonEmpty &&
      rowLineageAttributes.forall(updateTable.metadataOutput.contains) &&
      !updateTable.output.exists(attr => attr.name == ROW_ID_ATTRIBUTE_NAME)
  }


  private def updatePlanWithRowLineage(updateTable: UpdateTable): LogicalPlan = {
    EliminateSubqueryAliases(updateTable.table) match {
      case r @ DataSourceV2Relation(_: SupportsRowLevelOperations, _, _, _, _) => {
        val rowLineageAttributes = findRowLineageAttributes(updateTable.metadataOutput
        ).map(_._1)
        val lastUpdatedSequence = rowLineageAttributes.filter(
          attr => attr.name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME).head
        val updatedAssignments = updateTable.assignments ++
          Seq(Assignment(lastUpdatedSequence, Literal(null)))
        val rowLineageAsDataColumns = rowLineageAttributes
          .map(_.asInstanceOf[AttributeReference]).map {
            attr => attr.withMetadata(Metadata.empty)
          }

        val updatedTableWithLineage = r.copy(output =
          r.output ++ rowLineageAsDataColumns)

        updateTable.copy(table = updatedTableWithLineage, assignments = updatedAssignments)
      }
    }

  }
}
