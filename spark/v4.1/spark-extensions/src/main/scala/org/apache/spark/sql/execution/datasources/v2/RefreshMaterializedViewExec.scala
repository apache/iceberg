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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.view.RefreshState
import org.apache.iceberg.view.RefreshStateParser
import org.apache.iceberg.view.SourceTableState
import org.apache.iceberg.view.SQLViewRepresentation
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.functions
import scala.jdk.CollectionConverters._

case class RefreshMaterializedViewExec(catalog: ViewCatalog, ident: Identifier)
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val sparkCatalog = catalog.asInstanceOf[SparkCatalog]
    val icebergCatalog = sparkCatalog.icebergCatalog()
    val icebergViewCatalog =
      icebergCatalog.asInstanceOf[org.apache.iceberg.catalog.ViewCatalog]
    val viewId = TableIdentifier.of(Namespace.of(ident.namespace(): _*), ident.name())
    val view = icebergViewCatalog.loadView(viewId)

    val storageTableId = view.currentVersion().storageTable()
    Preconditions.checkState(
      storageTableId != null,
      "Cannot refresh %s: not a materialized view (no storage table)",
      ident)

    // Extract the SQL query from the view's representations
    val sparkSql = view
      .currentVersion()
      .representations()
      .asScala
      .collect { case sql: SQLViewRepresentation if sql.dialect() == "spark" => sql.sql() }
      .headOption
      .getOrElse(throw new IllegalStateException(
        s"Cannot refresh $ident: no Spark SQL representation found"))

    val refreshStartTimestampMs = System.currentTimeMillis()

    // Execute the view's query to get the current result set
    val queryResult = session.sql(sparkSql)

    // Discover source tables from the query's logical plan and capture their current state
    val sourceStates = collectSourceTableStates(queryResult.queryExecution.analyzed)

    // Build refresh state
    val refreshState = new RefreshState(
      view.currentVersion().versionId(),
      sourceStates.asJava,
      refreshStartTimestampMs)
    val refreshStateJson = RefreshStateParser.toJson(refreshState)

    // Write results to storage table, replacing existing data
    val storageTableRef = String.format(
      "%s.%s.%s",
      sparkCatalog.name(),
      storageTableId.namespace().toString,
      storageTableId.name())
    try {
      queryResult
        .writeTo(storageTableRef)
        .option("snapshot-property." + RefreshState.REFRESH_STATE_SUMMARY_KEY, refreshStateJson)
        .overwrite(functions.lit(true))
    } catch {
      case e: NoSuchTableException =>
        throw new IllegalStateException(
          s"Storage table $storageTableRef not found during refresh",
          e)
    }

    Nil
  }

  private def collectSourceTableStates(
      plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)
      : List[org.apache.iceberg.view.SourceState] = {
    val sparkCatalog = catalog.asInstanceOf[SparkCatalog]
    val icebergCatalog =
      sparkCatalog.icebergCatalog().asInstanceOf[org.apache.iceberg.catalog.Catalog]
    val tables = scala.collection.mutable.LinkedHashSet.empty[String]
    val states = scala.collection.mutable.ListBuffer.empty[org.apache.iceberg.view.SourceState]

    plan.collectLeaves().foreach {
      case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
          if r.catalog.exists(_.name() == sparkCatalog.name()) =>
        val tableIdent = r.identifier.get
        val key = tableIdent.toString
        if (!tables.contains(key)) {
          tables.add(key)
          val icebergId =
            TableIdentifier.of(Namespace.of(tableIdent.namespace(): _*), tableIdent.name())
          try {
            val table = icebergCatalog.loadTable(icebergId)
            val snapshotId =
              if (table.currentSnapshot() != null) {
                table.currentSnapshot().snapshotId()
              } else {
                -1L
              }
            states += new SourceTableState(
              icebergId.name(),
              icebergId.namespace().levels().toList.asJava,
              null,
              table.uuid().toString,
              snapshotId,
              null)
          } catch {
            case _: Exception => // skip tables we can't load
          }
        }
      case _ => // skip non-iceberg leaves
    }

    states.toList
  }

  override def simpleString(maxFields: Int): String = {
    s"RefreshMaterializedViewExec: ${ident}"
  }
}
