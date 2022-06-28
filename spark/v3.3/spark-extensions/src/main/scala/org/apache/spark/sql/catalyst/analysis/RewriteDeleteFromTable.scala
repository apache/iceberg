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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.plans.logical.DeleteFromIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.iceberg.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.DELETE
import org.apache.spark.sql.connector.iceberg.write.SupportsDelta
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle DELETE statements.
 *
 * If a table implements SupportsDelete and SupportsRowLevelOperations, this rule assigns a rewrite
 * plan but the optimizer will check whether this particular DELETE statement can be handled
 * by simply passing delete filters to the connector. If yes, the optimizer will then discard
 * the rewrite plan.
 */
object RewriteDeleteFromTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case d @ DeleteFromIcebergTable(aliasedTable, Some(cond), None) if d.resolved =>
      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val operation = buildRowLevelOperation(tbl, DELETE)
          val table = RowLevelOperationTable(tbl, operation)
          val rewritePlan = operation match {
            case _: SupportsDelta =>
              buildWriteDeltaPlan(r, table, cond)
            case _ =>
              buildReplaceDataPlan(r, table, cond)
          }
          // keep the original relation in DELETE to try deleting using filters
          DeleteFromIcebergTable(r, Some(cond), Some(rewritePlan))

        case p =>
          throw new AnalysisException(s"$p is not an Iceberg table")
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      cond: Expression): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildReadRelation(relation, operationTable, metadataAttrs)

    // construct a plan that contains unmatched rows in matched groups that must be carried over
    // such rows do not match the condition but have to be copied over as the source can replace
    // only groups of rows
    val remainingRowsFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowsFilter, readRelation)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    ReplaceData(writeRelation, remainingRowsPlan, relation)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      cond: Expression): WriteDelta = {

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowIdAttrs = resolveRowIdAttrs(relation, operationTable.operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildReadRelation(relation, operationTable, metadataAttrs, rowIdAttrs)

    // construct a plan that only contains records to delete
    val deletedRowsPlan = Filter(cond, readRelation)
    val operationType = Alias(Literal(DELETE_OPERATION), OPERATION_COLUMN)()
    val requiredWriteAttrs = dedupAttrs(rowIdAttrs ++ metadataAttrs)
    val project = Project(operationType +: requiredWriteAttrs, deletedRowsPlan)

    // build a plan to write deletes to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(project, Nil, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, project, relation, projections)
  }
}
