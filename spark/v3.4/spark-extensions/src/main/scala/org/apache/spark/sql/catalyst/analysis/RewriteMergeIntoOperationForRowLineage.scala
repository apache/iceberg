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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Literal, NamedExpression, V2ExpressionUtils}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeIntoIcebergTable, MergeRows, WriteIcebergDelta}
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.OPERATION_COLUMN
import org.apache.spark.sql.catalyst.util.{CharVarcharUtils, WriteDeltaProjections}
import org.apache.spark.sql.connector.write.{DeltaWrite, RowLevelOperationTable, SupportsDelta}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}

object RewriteMergeIntoOperationForRowLineage extends RewriteRowLevelIcebergCommand {

  private val ROW_LINEAGE_ATTRIBUTES = Set(
    "_row_id", "_last_updated_sequence_number")

  override def apply(plan: LogicalPlan): LogicalPlan = {
    RewriteMergeIntoTable.apply(plan) resolveOperators {
      case m @ MergeIntoIcebergTable(_, _, _, _, _, Some(rewritePlan)) => {
        if (rowLineageMetadataAttributes(m.metadataOutput).isEmpty) {
          m
        } else {
          rewritePlan match {
            case writeIcebergDelta @ WriteIcebergDelta(_, _, _, _, _) =>
              m.copy(rewritePlan = Some(updateDeltaPlan(writeIcebergDelta)))
            case _ => throw new UnsupportedOperationException("CoW isn't supported at the moment")
          }
        }
      }
      // ToDo handle append cases/cow etc
      case p => p
    }
  }

  private def updateDeltaPlan(writeIcebergDelta: WriteIcebergDelta): LogicalPlan = {
    val updatedMergeRows = writeIcebergDelta.query match {
      case mergeRows @ MergeRows(_, _, _, matchedOutputs, _, notMatchedOutputs, _, _, _, _, _) => {
        // ToDo check against more than just the name, cehck that it's a metadata attribute
        val rowIdOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute] && attr.asInstanceOf[Attribute].name == "_row_id")
        val lastUpdatedOrdinal = matchedOutputs(0)(0)
          .indexWhere(attr => attr.isInstanceOf[Attribute]
            && attr.asInstanceOf[Attribute].name == "_last_updated_sequence_number")
        if (rowIdOrdinal == -1) {
          throw new IllegalStateException("Expected to find row ID")
        } else {
          // ToDo more defensive here on expectations
          val updatedMatchOutputs =
            Seq(
              Seq(matchedOutputs(0)(0),
                // Set the metadata attribute to itself so it's preserved
              matchedOutputs(0)(1).updated(rowIdOrdinal, matchedOutputs(0)(0)(rowIdOrdinal)))
            )
          val updatedNotMatchedOutputs = Seq(notMatchedOutputs(0)
            .updated(rowIdOrdinal, Literal(null)).updated(lastUpdatedOrdinal, Literal(null)))

          mergeRows.copy(matchedOutputs = updatedMatchOutputs, notMatchedOutputs = updatedNotMatchedOutputs)
        }
      }
      case p => throw new UnsupportedOperationException(s"Unsupported $p")
    }

    val projections = writeIcebergDelta.projections
    val rowLineageAttributes = writeIcebergDelta.originalTable.metadataOutput.filter(attr => attr.name == "_row_id"
      || attr.name == "_last_updated_sequence_number")
    val projectedAttrs = writeIcebergDelta.table.output ++ rowLineageAttributes
    val outputAttrs = updatedMergeRows.output
    val outputs = updatedMergeRows.matchedOutputs.flatten ++ updatedMergeRows.notMatchedOutputs
    val projectedOrdinals = projectedAttrs.map(attr => outputAttrs.indexWhere(_.name == attr.name))

    val structFields = projectedAttrs.zip(projectedOrdinals).map { case (attr, ordinal) =>
      // output attr is nullable if at least one output projection may produce null for that attr
      // but row ID and metadata attrs are projected only for update/delete records and
      // row attrs are projected only in insert/update records
      // that's why the projection schema must rely only on relevant outputs
      // instead of blindly inheriting the output attr nullability
      val nullable = outputs.exists(output => output(ordinal).nullable)
      StructField(attr.name, attr.dataType, nullable, attr.metadata)
    }
    val schema = StructType(structFields)

    val newProjection = ProjectingInternalRow(schema, projectedOrdinals)

    val updatedProjections = WriteDeltaProjections(Some(newProjection),
      projections.rowIdProjection, projections.metadataProjection)
    new WriteIcebergDeltaWithRowLineage(
      writeIcebergDelta.table,
      updatedMergeRows,
      writeIcebergDelta.originalTable,
      updatedProjections,
      writeIcebergDelta.write)
  }


  private def rowLineageMetadataAttributes(metadataAttributes: Seq[Attribute]): Seq[Attribute] = {
    metadataAttributes.filter(attr => ROW_LINEAGE_ATTRIBUTES.contains(attr.name))
  }

  class WriteIcebergDeltaWithRowLineage(override val table: NamedRelation,
                                             override val query: LogicalPlan,
                                             override val originalTable: NamedRelation,
                                             override val projections: WriteDeltaProjections,
                                             override val write: Option[DeltaWrite])
    extends WriteIcebergDelta(table, query, originalTable, projections, write) {
    override protected lazy val stringArgs: Iterator[Any] = Iterator(table, query, write)

    private def operationResolved: Boolean = {
      val attr = query.output.head
      attr.name == OPERATION_COLUMN && attr.dataType == IntegerType && !attr.nullable
    }

    private def operation: SupportsDelta = {
      EliminateSubqueryAliases(table) match {
        case DataSourceV2Relation(RowLevelOperationTable(_, operation), _, _, _, _) =>
          operation match {
            case supportsDelta: SupportsDelta =>
              supportsDelta
            case _ =>
              throw new AnalysisException(s"Operation $operation is not a delta operation")
          }
        case _ =>
          throw new AnalysisException(s"Cannot retrieve row-level operation from $table")
      }
    }

    private def rowAttrsResolved: Boolean = {
      table.skipSchemaResolution || (projections.rowProjection match {
        case Some(projection) =>
          if (projection.schema.exists(field => field.name == "_row_id")) {
            //ToDo: filter out explicitly row id/last updated sequence and validate the compatibility against that
            table.output.size == projection.schema.size - 2
          } else {
            table.output.size == projection.schema.size &&
              projection.schema.zip(table.output).forall { case (field, outAttr) =>
                isCompatible(field, outAttr)
              }
          }

        case None =>
          true
      })
    }

    private def rowIdAttrsResolved: Boolean = {
      val rowIdAttrs = V2ExpressionUtils.resolveRefs[AttributeReference](
        operation.rowId.toSeq,
        originalTable)

      projections.rowIdProjection.schema.forall { field =>
        rowIdAttrs.exists(rowIdAttr => isCompatible(field, rowIdAttr))
      }
    }

    private def metadataAttrsResolved: Boolean = {
      projections.metadataProjection match {
        case Some(projection) =>
          val metadataAttrs = V2ExpressionUtils.resolveRefs[AttributeReference](
            operation.requiredMetadataAttributes.toSeq,
            originalTable)

          projection.schema.forall { field =>
            metadataAttrs.exists(metadataAttr => isCompatible(field, metadataAttr))
          }
        case None =>
          true
      }
    }

    private def isCompatible(projectionField: StructField, outAttr: NamedExpression): Boolean = {
      val inType = CharVarcharUtils.getRawType(projectionField.metadata).getOrElse(outAttr.dataType)
      val outType = CharVarcharUtils.getRawType(outAttr.metadata).getOrElse(outAttr.dataType)
      // names and types must match, nullability must be compatible
      projectionField.name == outAttr.name &&
        DataType.equalsIgnoreCompatibleNullability(inType, outType) &&
        (outAttr.nullable || !projectionField.nullable)
    }

    override def outputResolved: Boolean = {
      assert(table.resolved && query.resolved,
        "`outputResolved` can only be called when `table` and `query` are both resolved.")

      operationResolved && rowAttrsResolved && rowIdAttrsResolved && metadataAttrsResolved
    }

    override protected def withNewChildInternal(newChild: LogicalPlan): WriteIcebergDeltaWithRowLineage = {
      new WriteIcebergDeltaWithRowLineage(
        table, newChild, originalTable, projections, write)
    }
  }
}
