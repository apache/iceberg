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

import org.apache.iceberg.spark.PathIdentifier
import org.apache.iceberg.spark.SparkTableUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RowLevelWrite
import org.apache.spark.sql.catalyst.plans.logical.V2WriteCommand
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.connector.write.RowLevelOperationInfoImpl
import org.apache.spark.sql.connector.write.RowLevelOperationTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.ExtractV2Table
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * A rule that resolves the target branch for Iceberg reads and writes.
 * <p>
 * The branch must be determined and pinned during analysis. The current DSv2 framework
 * doesn't provide access to all necessary options during the initial table loading,
 * forcing us to finalize the branch selection in a custom analyzer rule. Future Spark
 * versions will have a built-in mechanism to cleanly determine the target branch.
 */
case class ResolveBranch(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // row-level operations like DELETE, UPDATE, and MERGE
    case w @ IcebergRowLevelWrite(table, operation, options) =>
      val branch = SparkTableUtil.determineWriteBranch(spark, table, options)
      if (table.branch != branch) {
        val newTable = table.copyWithBranch(branch)
        val info = RowLevelOperationInfoImpl(operation.command, options)
        val newOperation = newTable.newRowLevelOperationBuilder(info).build()
        val newOperationTable = RowLevelOperationTable(newTable, newOperation)
        val newTarget = transformPreservingType(w.table) {
          case r @ ExtractV2Table(RowLevelOperationTable(_, _)) => r.copy(table = newOperationTable)
        }
        val newQuery = transformPreservingType(w.query) {
          case r @ ExtractV2Table(RowLevelOperationTable(_, _)) => r.copy(table = newOperationTable)
        }
        w.withNewTable(newTarget).withNewQuery(newQuery)
      } else {
        w
      }

    // batch write operations like append or overwrite
    case w: V2WriteCommand =>
      val newTarget = transformPreservingType(w.table) {
        case r @ DataSourceV2Relation(table: SparkTable, _, _, _, options, _) =>
          val branch = SparkTableUtil.determineWriteBranch(spark, table, options)
          if (table.branch != branch) r.copy(table = table.copyWithBranch(branch)) else r
      }
      w.withNewTable(newTarget)

    // scan operations
    // branch selector is added to identifier to ensure further refreshes point to correct branch
    case r @ DataSourceV2Relation(table: SparkTable, _, _, Some(ident), options, None) =>
      val branch = SparkTableUtil.determineReadBranch(spark, table, options)
      if (table.branch != branch) {
        val branchSelector = s"branch_$branch"
        val newIdent = ident match {
          case path: PathIdentifier if path.location.contains("#") =>
            new PathIdentifier(path.location + "," + branchSelector)
          case path: PathIdentifier =>
            new PathIdentifier(path.location + "#" + branchSelector)
          case _ =>
            Identifier.of(ident.namespace :+ ident.name, branchSelector)
        }
        r.copy(table = table.copyWithBranch(branch), identifier = Some(newIdent))
      } else {
        r
      }
  }

  private def transformPreservingType[T <: LogicalPlan](plan: T)(
      func: PartialFunction[LogicalPlan, LogicalPlan]): T = {
    plan.transform(func).asInstanceOf[T]
  }
}

// Iceberg specific extractor for row-level operations like DELETE, UPDATE, and MERGE
private object IcebergRowLevelWrite {
  def unapply(
      write: RowLevelWrite): Option[(SparkTable, RowLevelOperation, CaseInsensitiveStringMap)] = {
    EliminateSubqueryAliases(write.table) match {
      case DataSourceV2Relation(
            RowLevelOperationTable(table: SparkTable, operation),
            _,
            _,
            _,
            options,
            _) =>
        Some((table, operation, options))
      case _ => None
    }
  }
}
