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


import java.util.UUID
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
import org.apache.iceberg.spark.MaterializedViewUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

case class CreateMaterializedViewExec(
                                       catalog: ViewCatalog,
                                       ident: Identifier,
                                       queryText: String,
                                       viewSchema: StructType,
                                       columnAliases: Seq[String],
                                       columnComments: Seq[Option[String]],
                                       queryColumnNames: Seq[String],
                                       comment: Option[String],
                                       properties: Map[String, String],
                                       allowExisting: Boolean,
                                       replace: Boolean,
                                       storageTableIdentifier: Option[String]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {

    // Check if storageTableIdentifier is provided, if not, generate a default identifier
    val sparkStorageTableIdentifier = storageTableIdentifier match {
      case Some(identifier) => {
        val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(session, identifier)
        val storageTableCatalogName = catalogAndIdentifier.catalog().name()
        Preconditions.checkState(
          storageTableCatalogName.equals(catalog.name()),
          "Storage table identifier must be in the same catalog as the view." +
            " Found storage table in catalog: %s, expected: %s",
          Array[Object](storageTableCatalogName, catalog.name())
        )
        catalogAndIdentifier.identifier()
      }
      case None => MaterializedViewUtil.getDefaultMaterializedViewStorageTableIdentifier(ident)
    }

    // TODO: Add support for partitioning the storage table
    catalog.asInstanceOf[SparkCatalog].createTable(
      sparkStorageTableIdentifier,
      viewSchema, new Array[Transform](0), ImmutableMap.of[String, String]()
    )

    // Capture base table state before inserting into the storage table
    val baseTables = MaterializedViewUtil.extractBaseTables(queryText).asScala.toList
    val baseTableSnapshots = getBaseTableSnapshots(baseTables)
    val baseTableSnapshotsProperties = baseTableSnapshots.map{
      case (key, value) => (
        MaterializedViewUtil.MATERIALIZED_VIEW_BASE_SNAPSHOT_PROPERTY_KEY_PREFIX + key.toString
        ) -> value.toString
    }

    // Insert into the storage table
    session.sql("INSERT INTO " + sparkStorageTableIdentifier + " " + queryText)

    // Update the base table snapshots properties
    val baseTablePropertyChanges = baseTableSnapshotsProperties.map{
      case (key, value) => TableChange.setProperty(key, value)
    }.toArray

    catalog.asInstanceOf[SparkCatalog].alterTable(sparkStorageTableIdentifier, baseTablePropertyChanges:_*)
    createMaterializedView(sparkStorageTableIdentifier.toString)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateMaterializedViewExec: ${ident}"
  }

  private def createMaterializedView(storageTableIdentifier: String): Unit = {
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog = if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val newProperties = properties ++
      comment.map(ViewCatalog.PROP_COMMENT -> _) +
      (ViewCatalog.PROP_CREATE_ENGINE_VERSION -> engineVersion,
        ViewCatalog.PROP_ENGINE_VERSION -> engineVersion) +
      (MaterializedViewUtil.MATERIALIZED_VIEW_PROPERTY_KEY -> "true") +
      (MaterializedViewUtil.MATERIALIZED_VIEW_STORAGE_TABLE_PROPERTY_KEY -> storageTableIdentifier)

    if (replace) {
      // CREATE OR REPLACE VIEW
      if (catalog.viewExists(ident)) {
        catalog.dropView(ident)
      }
      // FIXME: replaceView API doesn't exist in Spark 3.5
      catalog.createView(
        ident,
        queryText,
        currentCatalog,
        currentNamespace,
        viewSchema,
        queryColumnNames.toArray,
        columnAliases.toArray,
        columnComments.map(c => c.orNull).toArray,
        newProperties.asJava)
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        catalog.createView(
          ident,
          queryText,
          currentCatalog,
          currentNamespace,
          viewSchema,
          queryColumnNames.toArray,
          columnAliases.toArray,
          columnComments.map(c => c.orNull).toArray,
          newProperties.asJava)
      } catch {
        // TODO: Make sure the existing view is also a materialized view
        case _: ViewAlreadyExistsException if allowExisting => // Ignore
      }
    }
  }

  private def getBaseTableSnapshots(baseTables: List[Table]): Map[UUID, Long] = {
    baseTables.map {
      case sparkTable: SparkTable =>
        val snapshot = Option(sparkTable.table().currentSnapshot())
        val snapshotId = snapshot.map(_.snapshotId().longValue()).getOrElse(0L)
        (sparkTable.table().uuid(), snapshotId)
      case _ =>
        throw new UnsupportedOperationException("Only Spark tables are supported")
    }.toMap
  }
}
