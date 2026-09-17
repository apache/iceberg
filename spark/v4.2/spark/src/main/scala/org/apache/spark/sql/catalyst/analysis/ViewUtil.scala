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

import java.nio.charset.StandardCharsets
import java.util
import java.util.Base64

import org.apache.iceberg.catalog.LoadContext
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.HasIcebergCatalog
import org.apache.iceberg.spark.source.SparkView
import org.apache.iceberg.view.{View => IcebergView}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V1View
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import scala.jdk.CollectionConverters._

object ViewUtil {
  private val REFERENCED_BY_CONTEXT = "spark.sql.iceberg.referenced-by-context"
  private val ENTRY_SEPARATOR = "\u001e"
  private val PART_SEPARATOR = "\u001f"

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

  def loadContext(targetCatalogName: String): LoadContext = {
    LoadContext
      .builder()
      .referencedBy(buildReferencedByChain(currentViewChain, targetCatalogName))
      .build()
  }

  def withReferencedByContext(view: View, catalogName: String, ident: Identifier): View = {
    if (view == null || view.queryText() == null || view.columns() == null) {
      view
    } else {
      val viewChain = currentViewChain :+ qualifiedView(catalogName, ident)
      val sqlConfigs =
        Option(view.sqlConfigs()).map(_.asScala.toMap).getOrElse(Map.empty) +
          (REFERENCED_BY_CONTEXT -> encodeViewChain(viewChain))

      SparkView
        .applyOptionalFields(
          new View.Builder()
            .withColumns(view.columns())
            .withProperties(view.properties())
            .withQueryText(view.queryText())
            .withCurrentCatalog(view.currentCatalog())
            .withCurrentNamespace(view.currentNamespace())
            .withSqlConfigs(sqlConfigs.asJava)
            .withQueryColumnNames(view.queryColumnNames()),
          view.schemaMode(),
          sqlConfigs.asJava,
          view.viewDependencies())
        .build()
    }
  }

  /**
   * Build the referenced-by view chain from fully qualified view identifier parts.
   * Entries must belong to the same catalog as the loaded target.
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

    val crossCatalogViews = viewChain.filter(parts => !parts.headOption.contains(targetCatalogName))
    if (crossCatalogViews.nonEmpty) {
      throw new IllegalStateException(
        s"Cross-catalog view references are not supported with referenced-by enabled. " +
          s"Views from catalogs [${crossCatalogViews.map(_.head).distinct.mkString(", ")}] " +
          s"cannot reference entities in catalog [$targetCatalogName]")
    }

    val viewIdentifiers = viewChain.map { parts =>
      TableIdentifier.of(Namespace.of(parts.drop(1).init: _*), parts.last)
    }

    new util.ArrayList[TableIdentifier](viewIdentifiers.asJava)
  }

  def isIcebergViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog] && catalog.isInstanceOf[HasIcebergCatalog]
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

  private def currentViewChain: Seq[Seq[String]] = {
    val encoded = SQLConf.get.getConfString(REFERENCED_BY_CONTEXT, null)
    if (encoded == null || encoded.isEmpty) {
      Seq.empty
    } else {
      encoded
        .split(ENTRY_SEPARATOR, -1)
        .toIndexedSeq
        .filter(_.nonEmpty)
        .map(_.split(PART_SEPARATOR, -1).toIndexedSeq.map(decodePart))
    }
  }

  private def qualifiedView(catalogName: String, ident: Identifier): Seq[String] = {
    catalogName +: ident.namespace().toIndexedSeq :+ ident.name()
  }

  private def encodeViewChain(viewChain: Seq[Seq[String]]): String = {
    viewChain.map(_.map(encodePart).mkString(PART_SEPARATOR)).mkString(ENTRY_SEPARATOR)
  }

  private def encodePart(part: String): String = {
    Base64.getUrlEncoder
      .withoutPadding()
      .encodeToString(part.getBytes(StandardCharsets.UTF_8))
  }

  private def decodePart(part: String): String = {
    new String(Base64.getUrlDecoder.decode(part), StandardCharsets.UTF_8)
  }
}
