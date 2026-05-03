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
import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.iceberg.spark.TimeTravel
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

case class ResolveDefaultTimestamp(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val sessionTimestamp = spark.conf.getOption(SparkSQLProperties.AS_OF_TIMESTAMP)
    if (sessionTimestamp.isEmpty) return plan

    val timestampMillis = sessionTimestamp.get.toLong

    plan resolveOperators {
      // SQL-level time travel (TIMESTAMP AS OF / VERSION AS OF branch) conflicts with session property
      case DataSourceV2Relation(table: SparkTable, _, _, _, _, Some(_)) if table.branch() != null =>
        throw new IllegalArgumentException(
          s"Cannot override ref, already set snapshot id=${table.snapshotId()}")

      // SQL-level time travel (TIMESTAMP AS OF / VERSION AS OF snapshot or tag) conflicts
      case DataSourceV2Relation(_: SparkTable, _, _, _, _, Some(_)) =>
        throw new IllegalArgumentException(
          "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan")

      // Catalog-level branch selection (e.g. branch_X identifier set by ResolveBranch) conflicts
      case DataSourceV2Relation(table: SparkTable, _, _, _, _, None) if table.branch() != null =>
        throw new IllegalArgumentException(
          s"Cannot override ref, already set snapshot id=${table.snapshotId()}")

      // Catalog-level snapshot/tag/timestamp selector (e.g. snapshot_id_X, tag_X) conflicts
      case DataSourceV2Relation(table: SparkTable, _, _, ident, _, None)
          if table.branch() == null && hasExplicitSelector(ident) =>
        throw new IllegalArgumentException(
          "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan")

      // Plain read: apply session timestamp and encode the resolved snapshot in the identifier
      case r @ DataSourceV2Relation(table: SparkTable, _, _, ident, _, None)
          if table.branch() == null =>
        val newTable = SparkTable.create(table.table(), TimeTravel.timestampMillis(timestampMillis))
        val snapshotSelector = s"snapshot_id_${newTable.snapshotId()}"
        val newIdent = ident.map {
          case path: PathIdentifier if path.location.contains("#") =>
            new PathIdentifier(path.location + "," + snapshotSelector)
          case path: PathIdentifier =>
            new PathIdentifier(path.location + "#" + snapshotSelector)
          case i =>
            Identifier.of(i.namespace() :+ i.name(), snapshotSelector)
        }
        r.copy(table = newTable, identifier = newIdent)
    }
  }

  private def hasExplicitSelector(ident: Option[Identifier]): Boolean =
    ident.exists { i =>
      val name = i.name()
      name.startsWith("snapshot_id_") || name.startsWith("at_timestamp_") || name.startsWith("tag_")
    }
}
