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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

trait RewriteOperationForRowLineage extends RewriteRowLevelCommand {

  protected val ROW_ID_ATTRIBUTE_NAME = "_row_id"
  protected val LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME =
    "_last_updated_sequence_number"

  protected val ROW_LINEAGE_ATTRIBUTES =
    Set(ROW_ID_ATTRIBUTE_NAME, LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)

  protected def buildUpdatedWriteDeltaPlan(
      writeDeltaPlan: WriteDelta,
      updatedQuery: LogicalPlan,
      projections: WriteDeltaProjections
  ): WriteDelta = {
    val tableAsDsv2 = writeDeltaPlan.table.asInstanceOf[DataSourceV2Relation]
    val rowLineageAttributes = findRowLineageAttributes(
      writeDeltaPlan.originalTable.metadataOutput
    ).map(_._1)
    writeDeltaPlan.copy(
      table = updateDsv2RelationOutputWithRowLineage(
        tableAsDsv2,
        rowLineageAttributes
      ),
      query = updatedQuery,
      projections = projections
    )
  }

  protected def buildUpdatedReplaceDataPlan(
      replaceDataPlan: ReplaceData,
      updatedQuery: LogicalPlan
  ) = {
    val tableAsDsv2 = replaceDataPlan.table.asInstanceOf[DataSourceV2Relation]
    val rowLineageAttributes = findRowLineageAttributes(
      replaceDataPlan.originalTable.metadataOutput
    ).map(_._1)

    replaceDataPlan.copy(
      table = updateDsv2RelationOutputWithRowLineage(
        tableAsDsv2,
        rowLineageAttributes
      ),
      query = updatedQuery
    )
  }

  protected def updatePlanForRowLineage(writeCommand: V2WriteCommand) = {
    val tableSupportsRowLineage = writeCommand match {
      case writeDelta: WriteDelta =>
        writeDelta.originalTable.metadataOutput.exists(attr =>
          attr.name == "_row_id"
        )
      case replaceData: ReplaceData =>
        replaceData.originalTable.metadataOutput.exists(attr =>
          attr.name == "_row_id"
        )
      case _ =>
        writeCommand.table.metadataOutput.exists(attr => attr.name == "_row_id")
    }

    tableSupportsRowLineage && findRowLineageAttributes(
      writeCommand.table.output
    ).isEmpty
  }

  protected def findRowLineageAttributes(
      expressions: Seq[Expression]
  ): Seq[(Attribute, Int)] = {

    expressions.zipWithIndex
      .filter { case (expr, _) =>
        expr.isInstanceOf[Attribute] &&
          ROW_LINEAGE_ATTRIBUTES.contains(expr.asInstanceOf[Attribute].name) &&
          expr.asInstanceOf[Attribute].metadata.contains(METADATA_COL_ATTR_KEY)
      }
      .map(tup => (tup._1.asInstanceOf[Attribute], tup._2))
  }

  protected def findLineageAttribute(
      expressions: Seq[Expression],
      lineageAttribute: String
  ): Int = {
    findRowLineageAttributes(expressions).collectFirst {
      case (attr, ordinal) if attr.name == lineageAttribute => ordinal
    }.get
  }

  protected def updateDsv2RelationOutputWithRowLineage(
      relation: DataSourceV2Relation,
      rowLineageAttributes: Seq[Attribute]
  ): DataSourceV2Relation = {
    relation.copy(output =
      relation.output
        ++ rowLineageAttributes.map(_.asInstanceOf[AttributeReference])
    )
  }
}
