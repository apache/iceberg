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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
import org.apache.iceberg.spark.MaterializedViewUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.analysis.ViewUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

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
    storageTableIdentifier: Option[String])
    extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  /**
   * The columns of the materialized view, which are the columns of the query renamed by the column
   * aliases the statement declared. The query column names are recorded separately, so the aliases
   * name the view while the query keeps its own output names.
   */
  private lazy val outputSchema: StructType = {
    if (columnAliases.isEmpty) {
      viewSchema
    } else {
      StructType(viewSchema.fields.zipWithIndex.map { case (field, i) =>
        val renamed = field.copy(name = columnAliases(i))
        columnComments(i).map(renamed.withComment).getOrElse(renamed)
      })
    }
  }

  override protected def run(): Seq[InternalRow] = {
    // Replacing a materialized view has to decide what becomes of the storage table that the
    // previous definition materialized, and the view spec leaves that open: the storage table
    // identifier is recorded per view version, so a new version may keep the existing table or
    // point at a different one. Rather than settle that here, the statement is rejected. Drop
    // the materialized view and create it again to change its definition.
    if (replace) {
      throw new UnsupportedOperationException(
        s"Cannot replace materialized view: $ident. " +
          "Drop the materialized view and create it again to change its definition")
    }

    // Check if storageTableIdentifier is provided. If not, generate a default identifier.
    val sparkStorageTableIdentifier = storageTableIdentifier match {
      case Some(identifier) => {
        val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(session, identifier)
        val storageTableCatalogName = catalogAndIdentifier.catalog().name()
        Preconditions.checkState(
          storageTableCatalogName.equals(catalog.name()),
          "Storage table identifier must be in the same catalog as the view." +
            " Found storage table in catalog: %s, expected: %s.",
          Array[Object](storageTableCatalogName, catalog.name()))
        catalogAndIdentifier.identifier()
      }
      case None => MaterializedViewUtil.getDefaultMaterializedViewStorageTableIdentifier(ident)
    }

    // Step 1: Create the storage table BEFORE the MV view metadata.
    // Per spec: "The storage table must exist and be accessible before the
    // materialized view metadata is committed."
    // A newly created MV has a storage table with no snapshots until a refresh is performed.
    val sparkCatalog = catalog.asInstanceOf[SparkCatalog]
    sparkCatalog
      .createTable(
        sparkStorageTableIdentifier,
        outputSchema,
        new Array[Transform](0),
        ImmutableMap.of[String, String]())

    // Step 2: Create the MV view metadata with a storage-table reference
    try {
      createView(sparkStorageTableIdentifier.toString) match {
        case Some(_) => // success
        case None => // allowExisting and view already exists
      }
    } catch {
      case e: Exception =>
        try {
          sparkCatalog.dropTable(sparkStorageTableIdentifier)
        } catch {
          case _: Exception => // best effort cleanup
        }

        throw e
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateMaterializedViewExec: ${ident}"
  }

  private def createView(storageTableIdentifier: String): Option[View] = {
    val icebergSchema = SparkSchemaUtil.convert(outputSchema)
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog =
      if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    // The reserved properties that carry Spark view metadata are composed the same way as for a
    // plain view, so that a materialized view and a view are described by the same property keys.
    // Among them are the query's column names, which record the query column each view column
    // takes its values from, and which a refresh reads back to pair them up again by name.
    val sparkView = new View.Builder()
      .withQueryText(queryText)
      .withCurrentCatalog(currentCatalog)
      .withCurrentNamespace(currentNamespace)
      .withSchema(outputSchema)
      .withQueryColumnNames(queryColumnNames.toArray)
      .withSqlConfigs(ImmutableMap.of[String, String]())
      .withProperties((properties ++ comment.map(TableCatalog.PROP_COMMENT -> _)).asJava)
      .build()
    val newProperties = ViewUtil.createProperties(sparkView).asScala.toMap

    try {
      // CREATE VIEW [IF NOT EXISTS]
      val viewCatalog = catalog
        .asInstanceOf[SparkCatalog]
        .icebergViewCatalog()
      val icebergView = viewCatalog
        .buildView(Spark3Util.identifierToTableIdentifier(ident))
        .withDefaultCatalog(currentCatalog)
        .withDefaultNamespace(Namespace.of(currentNamespace: _*))
        .withQuery("spark", queryText)
        .withSchema(icebergSchema)
        .withLocation(properties.get("location").orNull)
        .withProperties(newProperties.asJava)
        .withStorageTableIdentifier(TableIdentifier.parse(storageTableIdentifier))
        .create()
      Some(SparkView.toView(catalog.name(), icebergView))
    } catch {
      // TODO: Make sure the existing view is also a materialized view
      case _: ViewAlreadyExistsException if allowExisting => None
    }
  }

}
