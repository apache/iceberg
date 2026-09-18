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

import java.util
import org.apache.iceberg.catalog.LoadContext
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkSupportsLoadContext
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.source.SparkView
import org.apache.iceberg.view.{View => IcebergView}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V1View
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import scala.jdk.CollectionConverters._

object ViewUtil {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  val RESERVED_PROPERTIES: Seq[String] =
    Seq(
      TableCatalog.PROP_COMMENT,
      TableCatalog.PROP_COLLATION,
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

  def loadView(catalog: CatalogPlugin, ident: Identifier, context: LoadContext): Option[View] =
    catalog match {
      case supportsLoadContext: SparkSupportsLoadContext =>
        try {
          Option(supportsLoadContext.loadView(ident, context)).filter(isIcebergView)
        } catch {
          case _: NoSuchViewException => None
        }
      case viewCatalog: ViewCatalog if catalog.isInstanceOf[HasIcebergCatalog] =>
        try {
          Option(viewCatalog.loadView(ident)).filter(isIcebergView)
        } catch {
          case _: NoSuchViewException => None
        }
      case _ => None
    }

  def loadIcebergView(catalog: CatalogPlugin, ident: Identifier): Option[IcebergView] =
    catalog match {
      case catalogWithIceberg: HasIcebergCatalog =>
        val icebergIdent = catalogWithIceberg.icebergIdentifier(ident)
        Option(catalogWithIceberg.icebergViewCatalog())
          .filter(_.viewExists(icebergIdent))
          .map(_.loadView(icebergIdent))
      case _ =>
        None
    }

  /**
   * Build the referenced-by view chain from fully qualified view identifier parts.
   * Cross-catalog entries are omitted because LoadContext uses catalog-relative identifiers.
   */
  def buildReferencedByChain(
      viewChain: Seq[Seq[String]],
      targetCatalogName: String): java.util.List[TableIdentifier] = {
    viewChain.foreach { parts =>
      require(
        parts.size >= 3,
        s"View chain entry must be fully qualified [catalog, namespace..., name], got: " +
          parts.mkString("."))
    }

    val viewIdentifiers = viewChain.filter(_.headOption.contains(targetCatalogName)).map { parts =>
      TableIdentifier.of(Namespace.of(parts.drop(1).init: _*), parts.last)
    }

    new util.ArrayList[TableIdentifier](viewIdentifiers.asJava)
  }

  def qualifyParts(
      parts: Seq[String],
      catalogAndNamespace: Seq[String],
      isCatalog: String => Boolean): Seq[String] = {
    parts match {
      case Seq(name) => catalogAndNamespace :+ name
      case _ if !isCatalog(parts.head) => catalogAndNamespace.head +: parts
      case _ => parts
    }
  }

  def buildViewChain(
      nameParts: Seq[String],
      viewCatalogAndNamespace: Seq[String],
      existingChain: Seq[Seq[String]],
      isCatalog: String => Boolean): Seq[Seq[String]] = {
    existingChain :+ qualifyParts(nameParts, viewCatalogAndNamespace, isCatalog)
  }

  def loadTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      context: LoadContext,
      timeTravelVersion: Option[String] = None,
      timeTravelTimestamp: Option[Expression] = None): Table = {
    catalog match {
      case supportsLoadContext: SparkSupportsLoadContext =>
        loadTableWithTimeTravel(
          supportsLoadContext,
          ident,
          context,
          timeTravelVersion,
          timeTravelTimestamp)
      case c if c.asTableCatalog.isInstanceOf[SparkSupportsLoadContext] =>
        loadTableWithTimeTravel(
          c.asTableCatalog.asInstanceOf[SparkSupportsLoadContext],
          ident,
          context,
          timeTravelVersion,
          timeTravelTimestamp)
      case _ =>
        (timeTravelVersion, timeTravelTimestamp) match {
          case (Some(version), _) =>
            catalog.asTableCatalog.loadTable(ident, version)
          case (_, Some(timestamp)) =>
            catalog.asTableCatalog.loadTable(ident, timestamp.eval().asInstanceOf[Long])
          case _ =>
            catalog.asTableCatalog.loadTable(ident)
        }
    }
  }

  def isIcebergViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog] && catalog.isInstanceOf[HasIcebergCatalog]
  }

  private def loadTableWithTimeTravel(
      supportsLoadContext: SparkSupportsLoadContext,
      ident: Identifier,
      context: LoadContext,
      timeTravelVersion: Option[String],
      timeTravelTimestamp: Option[Expression]): Table = {
    (timeTravelVersion, timeTravelTimestamp) match {
      case (Some(version), _) =>
        supportsLoadContext.loadTable(ident, version, context)
      case (_, Some(timestamp)) =>
        supportsLoadContext.loadTable(ident, timestamp.eval().asInstanceOf[Long], context)
      case _ =>
        supportsLoadContext.loadTable(ident, context)
    }
  }

  private def isIcebergView(view: View): Boolean = {
    !view.isInstanceOf[V1View] &&
    "iceberg".equalsIgnoreCase(view.properties.get(TableCatalog.PROP_PROVIDER))
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
