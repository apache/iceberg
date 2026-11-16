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

import org.apache.iceberg.catalog.LoadContext
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.iceberg.spark.SparkSupportsLoadContext
import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.catalyst.{TableIdentifier => V1TableIdentifier}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command.ViewHelper
import org.apache.spark.sql.internal.SQLConf
import scala.jdk.CollectionConverters._

object ViewUtil {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def loadView(catalog: CatalogPlugin, ident: Identifier): Option[View] = catalog match {
    case viewCatalog: ViewCatalog =>
      try {
        Option(viewCatalog.loadView(ident))
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }

  def loadView(catalog: CatalogPlugin, ident: Identifier, context: LoadContext): Option[View] =
    catalog match {
      case supportsLoadContext: SparkSupportsLoadContext =>
        try {
          Option(supportsLoadContext.loadView(ident, context))
        } catch {
          case _: NoSuchViewException => None
        }
      case _ => loadView(catalog, ident)
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
      TableIdentifier.of(parts.tail: _*)
    }

    viewIdentifiers.asJava
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

  def loadTable(
      catalog: SparkSupportsLoadContext,
      ident: Identifier,
      context: LoadContext,
      timeTravelSpec: Option[TimeTravelSpec]): Table = {
    timeTravelSpec match {
      case Some(AsOfVersion(version)) => catalog.loadTable(ident, version, context)
      case Some(AsOfTimestamp(micros)) => catalog.loadTable(ident, micros, context)
      case None => catalog.loadTable(ident, context)
    }
  }

  def referencedByEnabled(conf: SQLConf): Boolean = {
    java.lang.Boolean.parseBoolean(
      conf.getConfString(
        SparkSQLProperties.REFERENCED_BY_ENABLED,
        SparkSQLProperties.REFERENCED_BY_ENABLED_DEFAULT.toString))
  }

  def sqlConfigs(view: View): Map[String, String] = view match {
    case sparkView: SparkView => sparkView.sqlConfigs().asScala.toMap
    case _ => Map.empty
  }

  /** Returns the session SQL configs to store with a new view, using Spark's capture rules. */
  def capturedSqlConfigs(conf: SQLConf): Map[String, String] =
    ViewHelper.sqlConfigsToProps(conf, SparkView.SQL_CONFIG_PREFIX)

  private val nestedViewDepth = ThreadLocal.withInitial[Int](() => 0)

  /**
   * Runs the expansion of a view nested in the views currently being expanded, failing like
   * Spark when the nesting exceeds `maxNestedViewDepth`.
   */
  def withNestedView[T](viewIdent: Seq[String], maxNestedViewDepth: Int)(expand: => T): T = {
    val depth = nestedViewDepth.get() + 1
    if (depth > maxNestedViewDepth) {
      throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
        V1TableIdentifier(viewIdent.last, Some(viewIdent.init.tail.quoted), Some(viewIdent.head)),
        maxNestedViewDepth,
        UnresolvedRelation(viewIdent))
    }

    nestedViewDepth.set(depth)
    try {
      expand
    } finally {
      nestedViewDepth.set(depth - 1)
    }
  }

  def isViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog]
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
