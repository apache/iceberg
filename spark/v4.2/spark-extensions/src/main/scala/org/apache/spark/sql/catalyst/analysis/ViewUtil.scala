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

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

object ViewUtil {
  val PROP_CREATE_ENGINE_VERSION = "create_engine_version"
  val PROP_ENGINE_VERSION = "engine_version"
  val RESERVED_PROPERTIES: Seq[String] =
    Seq(
      TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_OWNER,
      TableCatalog.PROP_TABLE_TYPE,
      PROP_CREATE_ENGINE_VERSION,
      PROP_ENGINE_VERSION)

  def loadView(catalog: CatalogPlugin, ident: Identifier): Option[View] = catalog match {
    case viewCatalog: ViewCatalog =>
      try {
        Option(viewCatalog.loadView(ident))
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }

  def isViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog]
  }

  def newView(
      queryText: String,
      currentCatalog: String,
      currentNamespace: Array[String],
      viewSchema: StructType,
      queryColumnNames: Seq[String],
      properties: Map[String, String]): View = {
    new View.Builder()
      .withQueryText(queryText)
      .withCurrentCatalog(currentCatalog)
      .withCurrentNamespace(currentNamespace)
      .withSchema(viewSchema)
      .withQueryColumnNames(queryColumnNames.toArray)
      .withProperties(properties.asJava)
      .build()
  }

  def withProperties(viewInfo: View, properties: Map[String, String]): View = {
    val builder = new View.Builder()
      .withQueryText(viewInfo.queryText)
      .withCurrentCatalog(viewInfo.currentCatalog)
      .withCurrentNamespace(viewInfo.currentNamespace)
      .withSchema(viewInfo.schema)
      .withSchemaMode(viewInfo.schemaMode)
      .withQueryColumnNames(viewInfo.queryColumnNames)
      .withViewDependencies(viewInfo.viewDependencies)
      .withProperties(properties.asJava)

    if (viewInfo.sqlConfigs != null) {
      builder.withSqlConfigs(viewInfo.sqlConfigs)
    }

    builder.build()
  }

  implicit class IcebergViewHelper(plugin: CatalogPlugin) {
    def asViewCatalog: ViewCatalog = plugin match {
      case viewCatalog: ViewCatalog =>
        viewCatalog
      case _ =>
        throw QueryCompilationErrors.missingCatalogViewsAbilityError(plugin)
    }
  }
}
