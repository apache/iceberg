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

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.ExprId
import org.apache.spark.sql.catalyst.expressions.ExtendedV2ExpressionUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.iceberg.catalog.SupportsRowLevelOperations
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation
import org.apache.spark.sql.connector.iceberg.write.RowLevelOperation.Command
import org.apache.spark.sql.connector.write.RowLevelOperationInfoImpl
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.collection.mutable

trait RewriteRowLevelCommand extends Rule[LogicalPlan] {

  protected def buildRowLevelOperation(
      table: SupportsRowLevelOperations,
      command: Command): RowLevelOperation = {
    val info = RowLevelOperationInfoImpl(command, CaseInsensitiveStringMap.empty())
    val builder = table.newRowLevelOperationBuilder(info)
    builder.build()
  }

  protected def dedupAttrs(attrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    val exprIds = mutable.Set.empty[ExprId]
    attrs.flatMap { attr =>
      if (exprIds.contains(attr.exprId)) {
        None
      } else {
        exprIds += attr.exprId
        Some(attr)
      }
    }
  }

  protected def resolveRequiredMetadataAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): Seq[AttributeReference] = {

    ExtendedV2ExpressionUtils.resolveRefs[AttributeReference](
      operation.requiredMetadataAttributes,
      relation)
  }
}
