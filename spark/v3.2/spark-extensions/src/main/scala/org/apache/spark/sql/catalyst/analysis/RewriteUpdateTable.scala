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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.If
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, Filter, LogicalPlan, Project, ReplaceData, Union, UpdateIcebergTable, View, WriteDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.connector.iceberg.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.UPDATE
import org.apache.spark.sql.connector.iceberg.write.SupportsDelta
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle UPDATE statements.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 * That's why it must be run after AlignRowLevelCommandAssignments.
 *
 * This rule also must be run in the same batch with DeduplicateRelations in Spark.
 */
object RewriteUpdateTable extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UpdateIcebergTable(aliasedTable, assignments, cond, None) if u.resolved && u.aligned =>
      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          rewriteUpdateTable(u, r, tbl)
        case v: View =>
          val relations = v.children.collect { case r: DataSourceV2Relation if r.table.isInstanceOf[SparkTable] =>
            r
          }
          val icebergTableView = relations.nonEmpty && relations.size == 1 &&
            relations.head.table.isInstanceOf[SupportsRowLevelOperations]
          if (icebergTableView) {
            rewriteUpdateTable(u, relations.head, relations.head.table.asInstanceOf[SupportsRowLevelOperations])
          } else {
            throw new AnalysisException(s"$v is not an Iceberg table")
          }
        case p =>
          throw new AnalysisException(s"$p is not an Iceberg table")
      }
  }

  private def rewriteUpdateTable(
      u: UpdateIcebergTable,
      r: DataSourceV2Relation,
      tbl: SupportsRowLevelOperations): UpdateIcebergTable =  {
    val operation = buildRowLevelOperation(tbl, UPDATE)
    val table = RowLevelOperationTable(tbl, operation)
    val updateCond = u.condition.getOrElse(Literal.TrueLiteral)
    val rewritePlan = operation match {
      case _: SupportsDelta =>
        buildWriteDeltaPlan(r, table, u.assignments, updateCond)
      case _ if SubqueryExpression.hasSubquery(updateCond) =>
        buildReplaceDataWithUnionPlan(r, table, u.assignments, updateCond)
      case _ =>
        buildReplaceDataPlan(r, table, u.assignments, updateCond)
    }
    UpdateIcebergTable(r, u.assignments, u.condition, Some(rewritePlan))
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition does NOT contain a subquery
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    val readRelation = buildReadRelation(relation, operationTable, metadataAttrs)

    // build a plan with updated and copied over records
    val updatedAndRemainingRowsPlan = buildUpdateProjection(readRelation, assignments, cond)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    ReplaceData(writeRelation, updatedAndRemainingRowsPlan, relation)
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  // if the condition contains a subquery
  private def buildReplaceDataWithUnionPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a read relation and include all required metadata columns
    // the same read relation will be used to read records that must be updated and be copied over
    // DeduplicateRelations will take care of duplicated attr IDs
    val readRelation = buildReadRelation(relation, operationTable, metadataAttrs)

    // build a plan for records that match the cond and should be updated
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildUpdateProjection(matchedRowsPlan, assignments)

    // build a plan for records that did not match the cond but had to be copied over
    val remainingRowFilter = Not(EqualNullSafe(cond, Literal.TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowFilter, readRelation)

    // new state is a union of updated and copied over records
    val updatedAndRemainingRowsPlan = Union(updatedRowsPlan, remainingRowsPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = operationTable)
    ReplaceData(writeRelation, updatedAndRemainingRowsPlan, relation)
  }

  // build a rewrite plan for sources that support row deltas
  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      assignments: Seq[Assignment],
      cond: Expression): WriteDelta = {

    // resolve all needed attrs (e.g. row ID and any required metadata attrs)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, operationTable.operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    // construct a scan relation and include all required metadata columns
    val readRelation = buildReadRelation(relation, operationTable, metadataAttrs, rowIdAttrs)

    // build a plan for updated records that match the cond
    val matchedRowsPlan = Filter(cond, readRelation)
    val updatedRowsPlan = buildUpdateProjection(matchedRowsPlan, assignments)
    val operationType = Alias(Literal(UPDATE_OPERATION), OPERATION_COLUMN)()
    val project = Project(operationType +: updatedRowsPlan.output, updatedRowsPlan)

    // build a plan to write the row delta to the table
    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(project, rowAttrs, rowIdAttrs, metadataAttrs)
    WriteDelta(writeRelation, project, relation, projections)
  }

  // this method assumes the assignments have been already aligned before
  // the condition passed to this method may be different from the UPDATE condition
  private def buildUpdateProjection(
      plan: LogicalPlan,
      assignments: Seq[Assignment],
      cond: Expression = Literal.TrueLiteral): LogicalPlan = {

    // TODO: avoid executing the condition for each column

    // the plan output may include metadata columns that are not modified
    // that's why the number of assignments may not match the number of plan output columns

    val assignedValues = assignments.map(_.value)
    val updatedValues = plan.output.zipWithIndex.map { case (attr, index) =>
      if (index < assignments.size) {
        val assignedExpr = assignedValues(index)
        val updatedValue = If(cond, assignedExpr, attr)
        Alias(updatedValue, attr.name)()
      } else {
        attr
      }
    }

    Project(updatedValues, plan)
  }
}
