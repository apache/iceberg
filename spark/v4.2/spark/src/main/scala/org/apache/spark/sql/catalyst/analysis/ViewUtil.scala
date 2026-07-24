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

import org.apache.iceberg.catalog.{ViewCatalog => IcebergViewCatalog}
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V1View
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import scala.jdk.CollectionConverters._

object ViewUtil {
  val RESERVED_PROPERTIES: Seq[String] =
    Seq(
      TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_OWNER,
      TableCatalog.PROP_TABLE_TYPE,
      SparkView.PROP_CREATE_ENGINE_VERSION,
      SparkView.PROP_ENGINE_VERSION)

  def createProperties(view: View): java.util.Map[String, String] = {
    val properties = Spark3Util
      .rebuildCreateProperties(view.properties)
      .asScala
      .filterNot { case (key, _) => SparkView.isReservedProperty(key) }
    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    (properties ++ SparkView.internalProperties(view).asScala ++ Map(
      SparkView.PROP_CREATE_ENGINE_VERSION -> engineVersion,
      SparkView.PROP_ENGINE_VERSION -> engineVersion)).asJava
  }

  def loadView(catalog: CatalogPlugin, ident: Identifier): Option[View] = catalog match {
    case viewCatalog: ViewCatalog if catalog.isInstanceOf[HasIcebergCatalog] =>
      try {
        Option(viewCatalog.loadView(ident)).filter(isIcebergView)
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }

  def icebergViewCatalog(catalog: CatalogPlugin, ident: Identifier): Option[IcebergViewCatalog] =
    catalog match {
      case catalogWithIceberg: HasIcebergCatalog =>
        Option(catalogWithIceberg.icebergViewCatalog())
          .filter(_.viewExists(Spark3Util.identifierToTableIdentifier(ident)))
      case _ =>
        None
    }

  def isIcebergViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog] && catalog.isInstanceOf[HasIcebergCatalog]
  }

  private def isIcebergView(view: View): Boolean = {
    !view.isInstanceOf[V1View] &&
    "iceberg".equalsIgnoreCase(view.properties.get(TableCatalog.PROP_PROVIDER))
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
