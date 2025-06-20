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

import org.apache.iceberg.MetadataColumns
import org.apache.iceberg.TableUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.METADATA_COL_ATTR_KEY
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.MetadataBuilder

trait RewriteOperationForRowLineage extends RewriteRowLevelCommand {

  protected val ROW_ID_ATTRIBUTE_NAME = MetadataColumns.ROW_ID.name()
  protected val LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME =
    MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()

  // The plan should only be updated if the underlying Iceberg table supports row lineage AND
  // lineage attributes are not already on the output of operation which indicates the rule already ran
  protected def shouldUpdatePlan(table: LogicalPlan): Boolean = {
    val supportsRowLineage = EliminateSubqueryAliases(table) match {
      case r: DataSourceV2Relation =>
        r.table match {
          case sparkTable: SparkTable =>
            TableUtil.supportsRowLineage(sparkTable.table())
        }
      case _ => false
    }

    val rowIdAbsentFromOutput = !table.output.exists(_.name == ROW_ID_ATTRIBUTE_NAME)

    supportsRowLineage && rowIdAbsentFromOutput
  }

  protected def findRowLineageAttributes(
      expressions: Seq[Expression]): Option[(AttributeReference, AttributeReference)] = {
    val rowIdAttr = expressions.collectFirst {
      case attr: AttributeReference
          if isMetadataColumn(attr) && attr.name == ROW_ID_ATTRIBUTE_NAME =>
        attr
    }

    val lastUpdatedAttr = expressions.collectFirst {
      case attr: AttributeReference
          if isMetadataColumn(attr) && attr.name == LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME =>
        attr
    }

    // Treat row lineage columns as data columns by removing the metadata attribute
    // This works around the logic in ExposesMetadataColumns,
    // which prevents surfacing other metadata columns when a single metadata column is in the output
    (rowIdAttr, lastUpdatedAttr) match {
      case (Some(rowId), Some(lastUpdated)) =>
        Some((removeMetadataColumnAttribute(rowId), removeMetadataColumnAttribute(lastUpdated)))
      case _ => None
    }
  }

  protected def removeMetadataColumnAttribute(attr: AttributeReference): AttributeReference = {
    attr.withMetadata(
      new MetadataBuilder()
        .withMetadata(attr.metadata)
        .remove(METADATA_COL_ATTR_KEY)
        .build())
  }

  private def isMetadataColumn(attributeReference: AttributeReference): Boolean = {
    attributeReference.metadata.contains(METADATA_COL_ATTR_KEY)
  }
}
