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
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Exists
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.OuterReference
import org.apache.spark.sql.catalyst.plans.LeftAnti
import org.apache.spark.sql.catalyst.plans.LeftSemi
import org.apache.spark.sql.catalyst.plans.logical.CTERelationDef
import org.apache.spark.sql.catalyst.plans.logical.CTERelationRef
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.JoinHint
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.ReplaceScopedData
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.WithCTE
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.ReplaceDataProjections
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.DELETE_OPERATION
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.INSERT_OPERATION
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.OPERATION_COLUMN
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.WRITE_OPERATION
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.WRITE_WITH_METADATA_OPERATION
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.connector.write.SupportsDelta
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

/**
 * Lowers a [[ReplaceScopedData]] command into Iceberg's row-level write path.
 *
 * Scoped replace deletes target rows whose scope columns match a source row, then appends the full
 * source. Unlike MERGE, every source row must be inserted whether or not it matches a target row, so
 * the replacement state is computed from separate carryover and insert branches instead of from
 * per-match joined rows.
 *
 * The source is shared through a CTE so non-deterministic expressions are evaluated consistently by
 * the carryover branch and insert branch. Runtime file pruning is only applied for deterministic
 * sources because Spark requires row-level operation conditions to be deterministic.
 *
 * The row-level operation is requested as [[MERGE]] so copy-on-write vs merge-on-read selection
 * follows the same table configuration path as MERGE.
 */
object RewriteScopedReplace extends RewriteRowLevelCommand {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case rsd @ ReplaceScopedData(aliasedTable, scopeColumns, source)
        if rsd.resolved && source.resolved =>

      EliminateSubqueryAliases(aliasedTable) match {
        case r @ DataSourceV2Relation(tbl: SparkTable, _, _, _, options, _) =>
          if (source.output.size != r.output.size) {
            throw analysisError(
              "The source query of a scoped replace must produce the same number of columns as " +
                s"the target table ${r.name}: expected ${r.output.size}, got ${source.output.size}")
          }

          val operationTable = buildOperationTable(tbl, MERGE, options)
          val alignedSource = alignSourceColumns(r, source)
          operationTable.operation match {
            case deltaOperation: SupportsDelta =>
              buildWriteDeltaPlan(r, operationTable, deltaOperation, scopeColumns, alignedSource)
            case _ =>
              buildReplaceDataPlan(r, operationTable, scopeColumns, alignedSource)
          }

        case other =>
          throw analysisError(
            s"Scoped replace is only supported on Iceberg tables, found: ${other.simpleString(2)}")
      }
  }

  private def buildReplaceDataPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      scopeColumns: Seq[Seq[String]],
      source: LogicalPlan): LogicalPlan = {

    // Scoped replace uses INSERT-style positional alignment between source and target columns.
    val scopeOrdinals = resolveScopeOrdinals(relation, scopeColumns)

    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)
    val rowAttrs = relation.output

    val sourceCte = CTERelationDef(source)
    val scopeSource = newSourceRef(sourceCte)
    val insertSource = newSourceRef(sourceCte)
    val filterSource = newSourceRef(sourceCte)

    val antiJoinCondition = scopeEquality(readRelation.output, scopeSource.output, scopeOrdinals)
    val carryoverJoin =
      Join(readRelation, scopeSource, LeftAnti, Some(antiJoinCondition), JoinHint.NONE)
    val carryoverOutput =
      operationAlias(WRITE_WITH_METADATA_OPERATION) +: readRelation.output
    val carryover = Project(carryoverOutput, carryoverJoin)

    val insertData = alignedSourceData(insertSource, rowAttrs)
    val insertMetadata = metadataAttrs.map { attr =>
      Alias(Literal(null, attr.dataType), attr.name)()
    }
    val insertOutput = operationAlias(WRITE_OPERATION) +: (insertData ++ insertMetadata)
    val inserts = Project(insertOutput, insertSource)

    val replacementQuery = Union(carryover :: inserts :: Nil)
    val projections = buildUnionProjections(replacementQuery, rowAttrs, metadataAttrs)

    val groupFilterCond = runtimeCondition(source, rowAttrs, filterSource, scopeOrdinals)

    // ReplaceData's condition (2nd arg) is the planning-time pushdown filter; groupFilterCond
    // (6th) is the runtime group filter. The replaced scope set is dynamic: it comes from the
    // source, so there is no static target-only predicate to push down, the same as a MERGE whose
    // ON condition is purely join keys. That is why the condition is TrueLiteral. File pruning
    // instead comes from groupFilterCond, which RowLevelOperationRuntimeGroupFiltering turns into a
    // dynamic IN-subquery, so deterministic sources still rewrite only the groups that hold a match.
    val writeRelation = relation.copy(table = operationTable)
    val replaceData =
      ReplaceData(
        writeRelation,
        TrueLiteral,
        replacementQuery,
        relation,
        projections,
        Some(groupFilterCond))

    WithCTE(replaceData, sourceCte :: Nil)
  }

  private def buildWriteDeltaPlan(
      relation: DataSourceV2Relation,
      operationTable: RowLevelOperationTable,
      operation: SupportsDelta,
      scopeColumns: Seq[Seq[String]],
      source: LogicalPlan): LogicalPlan = {

    val scopeOrdinals = resolveScopeOrdinals(relation, scopeColumns)
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)
    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)

    val sourceCte = CTERelationDef(source)
    val scopeSource = newSourceRef(sourceCte)
    val insertSource = newSourceRef(sourceCte)
    val filterSource = newSourceRef(sourceCte)

    val semiJoinCondition = scopeEquality(readRelation.output, scopeSource.output, scopeOrdinals)
    val matchingRows =
      Join(readRelation, scopeSource, LeftSemi, Some(semiJoinCondition), JoinHint.NONE)

    val rowIdSet = AttributeSet(rowIdAttrs)
    val deleteData = rowAttrs.map {
      case attr if rowIdSet.contains(attr) => attr
      case attr => Alias(Literal(null, attr.dataType), attr.name)()
    }
    val deleteMetadata = nullifyMetadataOnDelete(metadataAttrs)
    val deletePayload = deleteData ++ rowIdAttrs ++ deleteMetadata
    val deleteOutput = operationAlias(DELETE_OPERATION) +: deletePayload
    val deletes = Project(deleteOutput, matchingRows)

    val insertData = alignedSourceData(insertSource, rowAttrs)
    val insertRowIds = rowIdAttrs.map { attr =>
      Alias(Literal(null, attr.dataType), attr.name)()
    }
    val insertMetadata = metadataAttrs.map { attr =>
      Alias(Literal(null, attr.dataType), attr.name)()
    }
    val insertOutput =
      operationAlias(INSERT_OPERATION) +: (insertData ++ insertRowIds ++ insertMetadata)
    val inserts = Project(insertOutput, insertSource)

    val deltaQuery = Union(deletes :: inserts :: Nil)
    val projections = buildDeltaUnionProjections(
      rowAttrs,
      rowIdAttrs,
      metadataAttrs,
      insertData,
      rowIdAttrs,
      deleteMetadata)
    val condition = runtimeCondition(source, rowAttrs, filterSource, scopeOrdinals)

    val writeRelation = relation.copy(table = operationTable)
    val writeDelta = WriteDelta(writeRelation, condition, deltaQuery, relation, projections)

    WithCTE(writeDelta, sourceCte :: Nil)
  }

  private def operationAlias(operation: Int): NamedExpression = {
    Alias(Literal(operation), OPERATION_COLUMN)()
  }

  private def runtimeCondition(
      source: LogicalPlan,
      rowAttrs: Seq[Attribute],
      filterSource: CTERelationRef,
      scopeOrdinals: Seq[Int]): Expression = {
    if (source.deterministic) {
      scopeExists(rowAttrs, filterSource, scopeOrdinals)
    } else {
      // A non-deterministic source cannot be used as a row-level operation condition: Spark requires
      // that condition to be deterministic (it is evaluated during scan planning, separately from the
      // write query, so it could disagree with the rows the write actually produces). Falling back to
      // an unconditional operation keeps the result correct because the carryover/delete joins remain
      // the sole arbiter of which rows survive, but it disables file pruning, so the whole table is
      // read and rewritten and the operation's conflict surface widens accordingly.
      logWarning(
        "Scoped replace source is non-deterministic; skipping runtime file pruning. The entire " +
          "target table will be read and rewritten, which may significantly increase write " +
          "amplification and the operation's conflict surface.")
      TrueLiteral
    }
  }

  // ReplaceData projections require fixed ordinals, but the replacement query is a Union over two
  // sources. Both branches therefore use the same [operation, data..., metadata...] layout.
  private def buildUnionProjections(
      query: LogicalPlan,
      rowAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): ReplaceDataProjections = {

    val output = query.output
    val rowOrdinals = rowAttrs.indices.map(_ + 1)
    val rowSchema = StructType(rowAttrs.zipWithIndex.map { case (attr, i) =>
      StructField(attr.name, attr.dataType, output(rowOrdinals(i)).nullable, attr.metadata)
    })
    val rowProjection = ProjectingInternalRow(rowSchema, rowOrdinals.toIndexedSeq)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      val metadataBaseOrdinal = 1 + rowAttrs.size
      val metadataOrdinals = metadataAttrs.indices.map(_ + metadataBaseOrdinal)
      // Insert rows null out metadata; carryover rows preserve the target metadata contract.
      val metadataSchema = StructType(metadataAttrs.map { attr =>
        StructField(attr.name, attr.dataType, attr.nullable, attr.metadata)
      })
      Some(ProjectingInternalRow(metadataSchema, metadataOrdinals.toIndexedSeq))
    } else {
      None
    }

    ReplaceDataProjections(rowProjection, metadataProjection)
  }

  private def buildDeltaUnionProjections(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute],
      rowOutputs: Seq[NamedExpression],
      rowIdOutputs: Seq[Expression],
      metadataOutputs: Seq[Expression]): WriteDeltaProjections = {

    val rowProjection = if (rowAttrs.nonEmpty) {
      val rowOrdinals = rowAttrs.indices.map(_ + 1)
      val rowSchema = StructType(rowAttrs.zipWithIndex.map { case (attr, i) =>
        StructField(attr.name, attr.dataType, rowOutputs(i).nullable, attr.metadata)
      })
      Some(ProjectingInternalRow(rowSchema, rowOrdinals.toIndexedSeq))
    } else {
      None
    }

    val rowIdBaseOrdinal = 1 + rowAttrs.size
    val rowIdOrdinals = rowIdAttrs.indices.map(_ + rowIdBaseOrdinal)
    val rowIdSchema = StructType(rowIdAttrs.zipWithIndex.map { case (attr, i) =>
      StructField(attr.name, attr.dataType, rowIdOutputs(i).nullable, attr.metadata)
    })
    val rowIdProjection = ProjectingInternalRow(rowIdSchema, rowIdOrdinals.toIndexedSeq)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      val metadataBaseOrdinal = rowIdBaseOrdinal + rowIdAttrs.size
      val metadataOrdinals = metadataAttrs.indices.map(_ + metadataBaseOrdinal)
      val metadataSchema = StructType(metadataAttrs.zipWithIndex.map { case (attr, i) =>
        StructField(attr.name, attr.dataType, metadataOutputs(i).nullable, attr.metadata)
      })
      Some(ProjectingInternalRow(metadataSchema, metadataOrdinals.toIndexedSeq))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  // Aligns the source query to the target table positionally, applying the same store-assignment
  // policy (ANSI/strict/legacy) Spark uses for INSERT INTO ... SELECT. This rejects or permits casts
  // identically to a plain insert, enforces target nullability/char-varchar contracts, and gives the
  // aligned columns the target's types so the scope-matching joins compare like-typed values.
  private def alignSourceColumns(
      relation: DataSourceV2Relation,
      source: LogicalPlan): LogicalPlan = {
    TableOutputResolver.resolveOutputColumns(
      relation.name,
      relation.output,
      source,
      byName = false,
      conf)
  }

  // The source is aligned to the target schema before the CTE is built, so the insert branch only
  // needs to expose the target column names (no further casting happens here).
  private def alignedSourceData(
      source: CTERelationRef,
      rowAttrs: Seq[Attribute]): Seq[NamedExpression] = {
    rowAttrs.indices.map { i =>
      Alias(source.output(i), rowAttrs(i).name)()
    }
  }

  private def resolveScopeOrdinals(
      relation: DataSourceV2Relation,
      scopeColumns: Seq[Seq[String]]): Seq[Int] = {
    scopeColumns.map { nameParts =>
      val resolved = relation.resolve(nameParts, conf.resolver).getOrElse {
        throw analysisError(
          s"Cannot resolve scope column '${nameParts.mkString(".")}' in target table ${relation.name}")
      }
      val ordinal = relation.output.indexWhere(_.exprId == resolved.toAttribute.exprId)
      if (ordinal < 0) {
        throw analysisError(
          s"Scope column '${nameParts.mkString(".")}' is not a top-level column of ${relation.name}")
      }
      ordinal
    }
  }

  private def newSourceRef(sourceDef: CTERelationDef): CTERelationRef = {
    val ref = CTERelationRef(
      sourceDef.id,
      _resolved = true,
      sourceDef.child.output,
      sourceDef.child.isStreaming)
    ref.newInstance().asInstanceOf[CTERelationRef]
  }

  private def scopeEquality(
      targetOutput: Seq[Attribute],
      sourceOutput: Seq[Attribute],
      scopeOrdinals: Seq[Int]): Expression = {
    scopeOrdinals
      .map(ordinal => EqualNullSafe(targetOutput(ordinal), sourceOutput(ordinal)): Expression)
      .reduce(And)
  }

  private def scopeExists(
      targetOutput: Seq[Attribute],
      sourceRef: CTERelationRef,
      scopeOrdinals: Seq[Int]): Expression = {
    val cond = scopeOrdinals
      .map { ordinal =>
        EqualNullSafe(OuterReference(targetOutput(ordinal)), sourceRef.output(ordinal)): Expression
      }
      .reduce(And)
    val outerRefs = scopeOrdinals.map(ordinal => targetOutput(ordinal))
    Exists(Filter(cond, sourceRef), outerRefs)
  }

  private def analysisError(message: String): AnalysisException = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_ICEBERG_SCOPED_REPLACE",
      sqlState = null,
      messageTemplate = message,
      messageParameters = Map.empty[String, String],
      cause = None,
      message = Some(message))
  }
}
