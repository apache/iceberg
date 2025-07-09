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

package org.apache.spark.sql.catalyst.optimizer;

import org.apache.curator.shaded.com.google.common.base.Preconditions
import org.apache.spark.sql.catalyst.analysis.RewriteRowLevelCommand
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.EqualNullSafe
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.MetadataAttribute
import org.apache.spark.sql.catalyst.expressions.Not
import org.apache.spark.sql.catalyst.plans.logical.Expand
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.INSERT_OPERATION
import org.apache.spark.sql.catalyst.util.RowDeltaUtils.OPERATION_COLUMN
import org.apache.spark.sql.connector.catalog.SupportsDeleteV2
import org.apache.spark.sql.connector.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.write.RowLevelOperation.Command.MERGE
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.connector.write.SupportsDelta
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


object RewriteInsertReplaceWhere extends RewriteRowLevelCommand {
  override def apply(plan : LogicalPlan) : LogicalPlan = plan resolveOperators {

    case RewriteInsertReplaceWhere(r, tbl, deleteExpr, query) =>
      val table = buildOperationTable(tbl, MERGE, CaseInsensitiveStringMap.empty)
      table.operation match {
        case _: SupportsDelta =>
          buildWriteDeltaPlan(r, table, query, deleteExpr)
        case _ =>
          buildReplaceDataPlan(r, table, query, deleteExpr)
      }
  }

  type ReturnType = (DataSourceV2Relation, SupportsRowLevelOperations, Expression, LogicalPlan)
  def unapply(o:OverwriteByExpression):Option[ReturnType] = {
    o match {
      case OverwriteByExpression (r@DataSourceV2Relation (tbl: SupportsRowLevelOperations, _, _, _, _),
      deleteExpr, query, _, _, _, _) if o.resolved && !deleteExpr.equals(TrueLiteral) &&
        tbl.properties().containsKey("provider") && "iceberg".equals(tbl.properties().get("provider")) =>

        tbl match {
          case t:SupportsDeleteV2
            //This table can use metadata to operate delete operation, keep overwriteByExpression plan.
            if DataSourceV2Strategy.translateFilterV2(deleteExpr).exists(p => t.canDeleteWhere(Array(p))) => None
          case _ => Some(r, tbl, deleteExpr, query)
        }

      case _ => None
    }
  }

  def buildWriteDeltaPlan(relation: DataSourceV2Relation, operationTable: RowLevelOperationTable,
                          query: LogicalPlan, deleteExpr:Expression): WriteDelta = {
    //Plan for delete part
    val operation = operationTable.operation.asInstanceOf[SupportsDelta]
    val rowAttrs = relation.output
    val rowIdAttrs = resolveRowIdAttrs(relation, operation)
    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operation)

    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs, rowIdAttrs)
    val deletedRowPlan = Filter(deleteExpr, readRelation)
    val rowDeltaPlan = buildDeletes(deletedRowPlan, rowIdAttrs)

    val writeRelation = relation.copy(table = operationTable)
    val projections = buildWriteDeltaProjections(rowDeltaPlan, rowAttrs, rowIdAttrs, metadataAttrs)

    //Plan for insert part
    val insertPlan = buildInsert(query, readRelation)

    //Insert & delete plan
    val insertAndDeletePlan = Union(Seq(rowDeltaPlan, insertPlan))
    WriteDelta(writeRelation, deleteExpr, insertAndDeletePlan, relation, projections)
  }

  private def buildInsert(query: LogicalPlan, readRelation: LogicalPlan) : Expand = {
    val (metadataAttrs, rowAttrs) = readRelation.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    // rowAttrs = __row_operation + tbl.schema
    Preconditions.checkState(query.output.length == rowAttrs.length, "Insert cols num %s doesn't match target table %s",
      query.output.length.toString, rowAttrs.length.toString)

    val extraNullValues = metadataAttrs.map(e => Literal(null, e.dataType))
    val insertOutput = Seq(Literal(INSERT_OPERATION)) ++ query.output ++ extraNullValues
    val outputs = Seq(insertOutput)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: readRelation.output
    val expandOutput = generateExpandOutput(attrs, outputs)
    Expand(outputs, expandOutput, query)
  }

  private def buildDeletes(matchedRowsPlan: LogicalPlan, rowIdAttrs: Seq[Attribute]): Expand = {
    val (metadataAttrs, rowAttrs) = matchedRowsPlan.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    val deleteOutput = deltaDeleteOutput(rowAttrs, rowIdAttrs, metadataAttrs)
    val outputs = Seq(deleteOutput)
    val operationTypeAttr = AttributeReference(OPERATION_COLUMN, IntegerType, nullable = false)()
    val attrs = operationTypeAttr +: matchedRowsPlan.output
    val expandOutput = generateExpandOutput(attrs, outputs)
    Expand(outputs, expandOutput, matchedRowsPlan)
  }

  private def buildReplaceDataPlan(relation: DataSourceV2Relation, operationTable: RowLevelOperationTable,
                                   query: LogicalPlan, deleteExpr:Expression): ReplaceData = {

    val metadataAttrs = resolveRequiredMetadataAttrs(relation, operationTable.operation)

    val readRelation = buildRelationWithAttrs(relation, operationTable, metadataAttrs)

    val remainingRowsFilter = Not(EqualNullSafe(deleteExpr, TrueLiteral))
    val remainingRowsPlan = Filter(remainingRowsFilter, readRelation)

    //Insert data
    val insertPlan = buildReplaceDataInsertPlan(query, readRelation)
    Preconditions.checkState(insertPlan.output.length == remainingRowsPlan.output.length,
      "Insert cols num %s doesn't match target table %s",
      insertPlan.output.length.toString, remainingRowsPlan.output.length.toString)
    val insertAndRemainingRowsPlan = Union(Seq(remainingRowsPlan, insertPlan))
    val writeRelation = relation.copy(table = operationTable)

    ReplaceData(writeRelation, deleteExpr, insertAndRemainingRowsPlan, relation, Some(deleteExpr))
  }

  private def buildReplaceDataInsertPlan(query: LogicalPlan, readRelation: LogicalPlan): Expand = {
    val (metadataAttrs, rowAttrs) = readRelation.output.partition { attr =>
      MetadataAttribute.isValid(attr.metadata)
    }
    // rowAttrs = __row_operation + tbl.schema
    Preconditions.checkState(query.output.length == rowAttrs.length, "Insert cols num %s doesn't match target table %s",
      query.output.length.toString, rowAttrs.length.toString)

    val extraNullValues = metadataAttrs.map(e => Literal(null, e.dataType))
    val insertOutput = Seq(query.output ++ extraNullValues)
    val attrs = readRelation.output
    val expandOutput = generateExpandOutput(attrs, insertOutput)
    Expand(insertOutput, expandOutput, query)
  }
}
