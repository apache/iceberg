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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.OPERATION_COLUMN
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.DeltaWrite
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.connector.write.SupportsDelta
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructField

/**
 * Writes a delta of rows to an existing table.
 */
case class WriteIcebergDelta(
    table: NamedRelation,
    query: LogicalPlan,
    originalTable: NamedRelation,
    projections: WriteDeltaProjections,
    write: Option[DeltaWrite] = None) extends V2WriteCommandLike {

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
        table.output.size == projection.schema.size &&
          projection.schema.zip(table.output).forall { case (field, outAttr) =>
            isCompatible(field, outAttr)
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

  override protected def withNewChildInternal(newChild: LogicalPlan): WriteIcebergDelta = {
    copy(query = newChild)
  }
}
