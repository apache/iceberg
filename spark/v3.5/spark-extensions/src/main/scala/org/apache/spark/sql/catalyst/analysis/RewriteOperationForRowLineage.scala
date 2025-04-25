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
  protected val LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()

  protected val ROW_LINEAGE_ATTRIBUTES =
    Set(ROW_ID_ATTRIBUTE_NAME, LAST_UPDATED_SEQUENCE_NUMBER_ATTRIBUTE_NAME)

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
      expressions: Seq[Expression]
  ): Seq[AttributeReference] = {
    expressions.collect {
      case attr: AttributeReference
        if ROW_LINEAGE_ATTRIBUTES.contains(attr.name) &&
          attr.metadata.contains(METADATA_COL_ATTR_KEY) => attr
    }
  }

  protected def removeMetadataColumnAttribute(attr: AttributeReference): AttributeReference = {
    attr.withMetadata(
      new MetadataBuilder()
        .withMetadata(attr.metadata)
        .remove(METADATA_COL_ATTR_KEY).build())
  }
}
