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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExtendedV2ExpressionUtils
import org.apache.spark.sql.catalyst.expressions.IsNotNull
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Literal.FalseLiteral
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.MonotonicallyIncreasingID
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftOuter
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.HintInfo
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeAction
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.MergeRows
import org.apache.spark.sql.catalyst.plans.logical.MergeRowsParams
import org.apache.spark.sql.catalyst.plans.logical.NO_BROADCAST_HASH
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.iceberg.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.connector.iceberg.write.SupportsDelta
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Assigns a rewrite plan for v2 tables that support rewriting data to handle MERGE statements.
 *
 * This rule assumes the commands have been fully resolved and all assignments have been aligned.
 * That's why it must be run after AlignRowLevelCommandAssignments.
 */
object RewriteMergeIntoTable extends RewriteRowLevelCommand {

  private final val ROW_FROM_SOURCE = "__row_from_source"
  private final val ROW_FROM_TARGET = "__row_from_target"
  private final val ROW_ID = "__row_id"

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case m @ MergeIntoIcebergTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && m.aligned && matchedActions.isEmpty && notMatchedActions.size == 1 =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r: DataSourceV2Relation =>
          // NOT MATCHED conditions may only refer to columns in source so they can be pushed down
          val insertAction = notMatchedActions.head.asInstanceOf[InsertAction]
          val filteredSource = insertAction.condition match {
            case Some(insertCond) => Filter(insertCond, source)
            case None => source
          }

          // when there are no MATCHED actions, use a left anti join to remove any matching rows
          // and switch to using a regular append instead of a row-level merge
          // only unmatched source rows that match the condition are appended to the table
          val joinPlan = Join(filteredSource, r, LeftAnti, Some(cond), JoinHint.NONE)

          val outputExprs = insertAction.assignments.map(_.value)
          val outputColNames = r.output.map(_.name)
          val outputCols = outputExprs.zip(outputColNames).map { case (expr, name) =>
            Alias(expr, name)()
          }
          val project = Project(outputCols, joinPlan)

          AppendData(r, project, Map.empty, isByName = false)

        case p =>
          throw new AnalysisException(s"$p is not an Iceberg table")
      }

    case m @ MergeIntoIcebergTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && m.aligned && matchedActions.isEmpty =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r: DataSourceV2Relation =>

          // when there are no MATCHED actions, use a left anti join to remove any matching rows
          // and switch to using a regular append instead of a row-level merge
          // only unmatched source rows that match action conditions are appended to the table
          val joinPlan = Join(source, r, LeftAnti, Some(cond), JoinHint.NONE)

          // we still have to merge rows as we have multiple not matched actions
          val mergeRowsParams = MergeRowsParams(
            isSourceRowPresent = TrueLiteral,
            isTargetRowPresent = FalseLiteral,
            matchedConditions = Nil,
            matchedOutputs = Nil,
            notMatchedConditions = notMatchedActions.map(actionCondition),
            notMatchedOutputs = notMatchedActions.map(output(_, Nil)),
            targetOutput = Nil,
            joinedAttributes = joinPlan.output,
            rowIdAttrs = Nil,
            performCardinalityCheck = false,
            emitNotMatchedTargetRows = false)
          val mergeRows = buildMergeRows(mergeRowsParams, r.output, joinPlan)

          AppendData(r, mergeRows, Map.empty, isByName = false)

        case p =>
          throw new AnalysisException(s"$p is not an Iceberg table")
      }

    case m @ MergeIntoIcebergTable(aliasedTable, source, cond, matchedActions, notMatchedActions, None)
        if m.resolved && m.aligned =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SupportsRowLevelOperations, _, _, _, _) =>
          val operation = buildRowLevelOperation(tbl, MERGE)
          val table = RowLevelOperationTable(tbl, operation)
          val rewritePlan = operation match {
            case _: SupportsDelta =>
              throw new AnalysisException("Delta merges are not currently supported")
            case _ =>
              buildReplaceDataPlan(r, table, source, cond, matchedActions, notMatchedActions)
          }

          m.copy(rewritePlan = Some(rewritePlan))

        case p =>
          throw new AnalysisException(s"$p is not an Iceberg table")
      }
  }

  // build a rewrite plan for sources that support replacing groups of data (e.g. files, partitions)
  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      table: RowLevelOperationTable,
      source: LogicalPlan,
      cond: Expression,
      matchedActions: Seq[MergeAction],
      notMatchedActions: Seq[MergeAction]): ReplaceData = {

    // resolve all needed attrs (e.g. metadata attrs for grouping data on write)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, table.operation)

    // construct a scan relation and include all required metadata columns
    val readRelation = buildReadRelation(relation, table, metadataAttrs)
    val readAttrs = readRelation.output

    // project an extra column to check if a target row exists after the join
    // project a synthetic row ID to perform the cardinality check
    val rowFromTarget = Alias(TrueLiteral, ROW_FROM_TARGET)()
    val rowId = Alias(MonotonicallyIncreasingID(), ROW_ID)()
    val targetTableProjExprs = readAttrs ++ Seq(rowFromTarget, rowId)
    val targetTableProj = Project(targetTableProjExprs, readRelation)

    // project an extra column to check if a source row exists after the join
    val rowFromSource = Alias(TrueLiteral, ROW_FROM_SOURCE)()
    val sourceTableProjExprs = source.output :+ rowFromSource
    val sourceTableProj = Project(sourceTableProjExprs, source)

    // use left outer join if there is no NOT MATCHED action, unmatched source rows can be discarded
    // use full outer join in all other cases, unmatched source rows may be needed
    // disable broadcasts for the target table to perform the cardinality check
    val joinType = if (notMatchedActions.isEmpty) LeftOuter else FullOuter
    val joinHint = JoinHint(leftHint = Some(HintInfo(Some(NO_BROADCAST_HASH))), rightHint = None)
    val joinPlan = Join(targetTableProj, sourceTableProj, joinType, Some(cond), joinHint)

    val rowIdAttr = ExtendedV2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_ID),
      joinPlan)
    val rowFromSourceAttr = ExtendedV2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_SOURCE),
      joinPlan)
    val rowFromTargetAttr = ExtendedV2ExpressionUtils.resolveRef[AttributeReference](
      FieldReference(ROW_FROM_TARGET),
      joinPlan)

    // add an extra matched action to output the original row if none of the actual actions matched
    // this is needed to keep target rows that should be copied over as we are working with groups
    val mergeRowsParams = MergeRowsParams(
      isSourceRowPresent = IsNotNull(rowFromSourceAttr),
      isTargetRowPresent = IsNotNull(rowFromTargetAttr),
      matchedConditions = matchedActions.map(actionCondition) :+ TrueLiteral,
      matchedOutputs = matchedActions.map(output(_, metadataAttrs)) :+ Some(readAttrs),
      notMatchedConditions = notMatchedActions.map(actionCondition),
      notMatchedOutputs = notMatchedActions.map(output(_, metadataAttrs)),
      targetOutput = readAttrs,
      joinedAttributes = joinPlan.output,
      rowIdAttrs = Seq(rowIdAttr),
      performCardinalityCheck = isCardinalityCheckNeeded(matchedActions),
      emitNotMatchedTargetRows = true)
    val mergeRows = buildMergeRows(mergeRowsParams, readAttrs, joinPlan)

    // build a plan to replace read groups in the table
    val writeRelation = relation.copy(table = table)
    ReplaceData(writeRelation, mergeRows, relation)
  }

  private def actionCondition(action: MergeAction): Expression = {
    action.condition.getOrElse(TrueLiteral)
  }

  private def output(
      clause: MergeAction,
      metadataAttrs: Seq[Attribute]): Option[Seq[Expression]] = {

    clause match {
      case u: UpdateAction =>
        Some(u.assignments.map(_.value) ++ metadataAttrs)

      case _: DeleteAction =>
        None

      case i: InsertAction =>
        Some(i.assignments.map(_.value) ++ metadataAttrs.map(attr => Literal(null, attr.dataType)))

      case other =>
        throw new AnalysisException(s"Unexpected action: $other")
    }
  }

  private def buildMergeRows(
      params: MergeRowsParams,
      attrs: Seq[Attribute],
      joinPlan: LogicalPlan): MergeRows = {

    val outputs = params.matchedOutputs.flatten ++ params.notMatchedOutputs.flatten
    assert(outputs.nonEmpty, "must be at least one output")

    val nullabilityMap = attrs.indices.map { index =>
      index -> outputs.exists(output => output(index).nullable)
    }.toMap

    val output = attrs.zipWithIndex.map { case (attr, index) =>
      AttributeReference(attr.name, attr.dataType, nullabilityMap(index))()
    }

    MergeRows(params, output, joinPlan)
  }

  private def isCardinalityCheckNeeded(actions: Seq[MergeAction]): Boolean = actions match {
    case Seq(DeleteAction(None)) => false
    case _ => true
  }
}
