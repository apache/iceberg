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


import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap
import org.apache.iceberg.spark.{MaterializedViewUtil, Spark3Util, SparkCatalog, SparkSchemaUtil}
import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
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
                                       storageTableIdentifier: Option[String]) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {

    // Check if storageTableIdentifier is provided. If not, generate a default identifier.
    val sparkStorageTableIdentifier = storageTableIdentifier match {
      case Some(identifier) => {
        val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(session, identifier)
        val storageTableCatalogName = catalogAndIdentifier.catalog().name()
        Preconditions.checkState(
          storageTableCatalogName.equals(catalog.name()),
          "Storage table identifier must be in the same catalog as the view." +
            " Found storage table in catalog: %s, expected: %s.",
          Array[Object](storageTableCatalogName, catalog.name())
        )
        catalogAndIdentifier.identifier()
      }
      case None => MaterializedViewUtil.getDefaultMaterializedViewStorageTableIdentifier(ident)
    }

    // Step 1: Create the storage table BEFORE the MV view metadata.
    // Per spec: "The storage table must exist and be accessible before the
    // materialized view metadata is committed."
    // A newly created MV has a storage table with no snapshots until a refresh is performed.
    catalog.asInstanceOf[SparkCatalog].createTable(
      sparkStorageTableIdentifier,
      viewSchema, new Array[Transform](0), ImmutableMap.of[String, String]()
    )

    // Step 2: Create the MV view metadata with a storage-table reference
    try {
      createView(sparkStorageTableIdentifier.toString) match {
        case Some(_) => // success
        case None => // allowExisting and view already exists
      }
    } catch {
      case e: Exception =>
        // If view creation fails, clean up the storage table
        try {
          catalog.asInstanceOf[SparkCatalog].dropTable(sparkStorageTableIdentifier)
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
    val icebergSchema = SparkSchemaUtil.convert(viewSchema)
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog = if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val newProperties = properties ++
      comment.map(ViewCatalog.PROP_COMMENT -> _) +
      (ViewCatalog.PROP_CREATE_ENGINE_VERSION -> engineVersion,
        ViewCatalog.PROP_ENGINE_VERSION -> engineVersion) +
      ("queryColumnNames" -> queryColumnNames.mkString(","))


    if (replace) {
      // CREATE OR REPLACE VIEW
      if (catalog.viewExists(ident)) {
        catalog.dropView(ident)
      }
      // FIXME: replaceView API doesn't exist in Spark 3.5
      val icebergView = catalog.asInstanceOf[SparkCatalog].icebergCatalog().asInstanceOf[org.apache.iceberg.catalog.ViewCatalog].buildView(
        Spark3Util.identifierToTableIdentifier(ident))
        .withDefaultCatalog(currentCatalog)
        .withDefaultNamespace(Namespace.of(currentNamespace: _*))
        .withQuery("spark", queryText)
        .withSchema(icebergSchema)
        .withLocation(properties.get("location").orNull)
        .withProperties(newProperties.asJava)
        .withStorageTableIdentifier(TableIdentifier.parse(storageTableIdentifier))
        .create()
      Some(new SparkView(catalog.name(), icebergView))

    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        val icebergView = catalog.asInstanceOf[SparkCatalog].icebergCatalog().asInstanceOf[org.apache.iceberg.catalog.ViewCatalog].buildView(
          Spark3Util.identifierToTableIdentifier(ident))
          .withDefaultCatalog(currentCatalog)
          .withDefaultNamespace(Namespace.of(currentNamespace: _*))
          .withQuery("spark", queryText)
          .withSchema(icebergSchema)
          .withLocation(properties.get("location").orNull)
          .withProperties(newProperties.asJava)
          .withStorageTableIdentifier(TableIdentifier.parse(storageTableIdentifier))
          .create()
        Some(new SparkView(catalog.name(), icebergView))
      } catch {
        // TODO: Make sure the existing view is also a materialized view
        case _: ViewAlreadyExistsException if allowExisting => None
      }
    }
  }

}
