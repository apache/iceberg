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
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.spark.source.SparkView
import org.apache.iceberg.view.RefreshState
import org.apache.iceberg.view.RefreshStateParser
import org.apache.iceberg.view.SourceTableState
import org.apache.iceberg.view.SourceViewState
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
    val icebergViewCatalog = sparkCatalog.icebergViewCatalog()
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

    // Execute the view's query to get the current result set. Each view column then takes its
    // values from the query column of the same name, not from the query column in the same
    // position, which is how the view itself is read: the names the query produced are recorded
    // when the view is created, and each view column is paired with the name recorded for it.
    val viewColumnNames = view.schema().columns().asScala.map(_.name()).toSeq
    val recordedQueryColumnNames =
      SparkView.toView(sparkCatalog.name(), view).queryColumnNames().toSeq
    Preconditions.checkState(
      recordedQueryColumnNames.isEmpty
        || recordedQueryColumnNames.length == viewColumnNames.length,
      "Cannot refresh %s: view has %s column(s) but %s query column name(s) are recorded",
      ident,
      Int.box(viewColumnNames.length),
      Int.box(recordedQueryColumnNames.length))

    // A view created outside Spark records no query column names, and such a view is read by
    // looking up its own column names in the query's output, so a refresh does the same.
    val queryColumnNames =
      if (recordedQueryColumnNames.isEmpty) viewColumnNames else recordedQueryColumnNames

    val rawQueryResult = session.sql(sparkSql)
    // Either set of names can be missing from the query's output. Recorded names go stale when a
    // source column is renamed or dropped: a view created as SELECT * FROM t over a table t of
    // (id, data) records id for its first column, so renaming t.id to ident makes the query
    // produce (ident, data), leaving no id column to read. The view's own names, used when none
    // were recorded, may never have matched the query's output at all. Spark reports an
    // incompatible schema change when reading a view in either state, so a refresh fails here
    // rather than materializing what the view cannot read.
    val missingColumns =
      queryColumnNames.filterNot(rawQueryResult.schema.fieldNames.toSet.contains)
    Preconditions.checkState(
      missingColumns.isEmpty,
      "Cannot refresh %s: query does not produce column(s) [%s] that the view reads. "
        + "Recreate the view to match the current query.",
      ident,
      missingColumns.mkString(", "))

    val queryResult = rawQueryResult.select(queryColumnNames.zip(viewColumnNames).map {
      case (queryColumn, viewColumn) => functions.col(queryColumn).as(viewColumn)
    }: _*)

    // Discover source tables and views from the query's logical plan and capture their
    // current state
    val sourceStates = collectSourceStates(queryResult.queryExecution.analyzed)

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

  /**
   * Returns the catalog name to record for a source, or null when the source is in the
   * materialized view's own catalog.
   *
   * <p>A null catalog keeps refresh state portable: a materialized view and its sources that live
   * in the same catalog stay resolvable when that catalog is registered under a different name.
   */
  private def sourceCatalogName(sourceCatalog: HasIcebergCatalog): String = {
    if (sourceCatalog.name() == catalog.name()) null else sourceCatalog.name()
  }

  private def collectSourceStates(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan)
      : List[org.apache.iceberg.view.SourceState] = {
    val seen = scala.collection.mutable.LinkedHashSet.empty[String]
    val states = scala.collection.mutable.ListBuffer.empty[org.apache.iceberg.view.SourceState]

    // Every leaf relation backed by an Iceberg catalog is a candidate, including relations from
    // catalogs other than the one holding the materialized view. Matching on HasIcebergCatalog
    // rather than a concrete catalog class also covers Iceberg tables reached through the session
    // catalog. Relations are deduplicated by catalog name and identifier so that a table
    // referenced more than once yields a single state.
    //
    // Only a SparkTable is recorded. Such a relation carries the snapshot that was resolved when
    // it was analyzed, and its scan is pinned to that snapshot, so the identity, snapshot, and
    // branch are read from the relation itself rather than from a second load of the table. The
    // other relations an Iceberg catalog can produce are left out on purpose, following the
    // strategy of recording only the Iceberg dependencies a producer can track: a V1Table reached
    // through the session catalog is not an Iceberg table, and a SparkChangelogTable reads a range
    // of snapshots rather than a single one, so neither has a snapshot id to record. Leaving a
    // dependency untracked means its changes do not make this materialized view stale.
    plan.collectLeaves().foreach {
      case r: org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
          if r.catalog.exists(_.isInstanceOf[HasIcebergCatalog]) && r.identifier.isDefined =>
        r.table match {
          case sparkTable: SparkTable =>
            val sourceCatalog = r.catalog.get.asInstanceOf[HasIcebergCatalog]
            val tableIdent = r.identifier.get
            val key = "table:" + sourceCatalog.name() + "." + tableIdent.toString
            if (seen.add(key)) {
              val icebergId = sourceCatalog.icebergIdentifier(tableIdent)
              val pinnedSnapshotId = sparkTable.snapshotId()
              states += new SourceTableState(
                icebergId.name(),
                icebergId.namespace().levels().toList.asJava,
                sourceCatalogName(sourceCatalog),
                sparkTable.table().uuid().toString,
                if (pinnedSnapshotId != null) {
                  pinnedSnapshotId.longValue()
                } else {
                  RefreshState.NO_SNAPSHOT_ID
                },
                sparkTable.branch())
            }

          case _ => // not an Iceberg table with a single snapshot, so not tracked
        }
      case _ => // skip non-iceberg leaves
    }

    // Spark's analyzer replaces every view reference with a View node wrapping the view's
    // expanded query, including transitively for view-of-view chains, so a single pass over
    // the whole plan (not just its leaves) discovers every source view at every nesting depth.
    // Matching View nodes rather than the SubqueryAlias that wraps them keeps tables out of
    // this pass, since Spark aliases table references the same way.
    plan
      .collect { case view: org.apache.spark.sql.catalyst.plans.logical.View => view.desc }
      .foreach { desc =>
        val viewIdent = desc.identifier
        viewIdent.catalog.foreach { catalogName =>
          val key = "view:" + catalogName + "." + viewIdent.unquotedString
          if (seen.add(key)) {
            val icebergId =
              TableIdentifier.of(Namespace.of(viewIdent.database.toList: _*), viewIdent.table)
            // The catalog is matched positively, mirroring the table pass: a catalog that is not
            // backed by Iceberg, or one that cannot serve views, has no view state to record. A
            // NoSuchViewException means the name resolves to something other than an Iceberg view,
            // such as a Spark view served by the session catalog, so it is not tracked either.
            // Any other failure to load an Iceberg view is left to propagate, because recording
            // an incomplete set of sources would make this materialized view look fresher than it
            // is rather than fail the refresh.
            session.sessionState.catalogManager.catalog(catalogName) match {
              case sourceCatalog: HasIcebergCatalog =>
                val icebergViewCatalog = sourceCatalog.icebergViewCatalog()
                if (icebergViewCatalog != null) {
                  try {
                    val view = icebergViewCatalog.loadView(icebergId)
                    states += new SourceViewState(
                      icebergId.name(),
                      icebergId.namespace().levels().toList.asJava,
                      sourceCatalogName(sourceCatalog),
                      view.uuid().toString,
                      view.currentVersion().versionId())
                  } catch {
                    case _: org.apache.iceberg.exceptions.NoSuchViewException => // not tracked
                  }
                }

              case _ => // not an Iceberg catalog, so not tracked
            }
          }
        }
      }

    states.toList
  }

  override def simpleString(maxFields: Int): String = {
    s"RefreshMaterializedViewExec: ${ident}"
  }
}
