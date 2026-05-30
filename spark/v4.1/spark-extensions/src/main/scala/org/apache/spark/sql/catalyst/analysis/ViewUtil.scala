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
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkSupportsReferencedBy
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
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

  def loadView(
      catalog: CatalogPlugin,
      ident: Identifier,
      referencedBy: java.util.List[TableIdentifier]): Option[View] = catalog match {
    case supportsReferencedBy: SparkSupportsReferencedBy =>
      try {
        Option(supportsReferencedBy.loadView(ident, referencedBy))
      } catch {
        case _: NoSuchViewException => None
      }
    case viewCatalog: ViewCatalog =>
      try {
        Option(viewCatalog.loadView(ident))
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }

  /**
   * Build the referenced-by view chain from a sequence of fully-qualified
   * view identifier parts. Validates that all entries are fully qualified and belong to the
   * same catalog as the target.
   */
  def buildReferencedByChain(
      viewChain: Seq[Seq[String]],
      targetCatalogName: String): java.util.List[TableIdentifier] = {
    viewChain.foreach { parts =>
      require(
        parts.size >= 3,
        s"View chain entry must be fully qualified [catalog, namespace..., name], got: ${parts.mkString(".")}")
    }
    val crossCatalogViews =
      viewChain.filter(parts => !parts.headOption.contains(targetCatalogName))
    if (crossCatalogViews.nonEmpty) {
      throw new IllegalStateException(
        s"Cross-catalog view references are not supported with referenced-by enabled. " +
          s"Views from catalogs [${crossCatalogViews.map(_.head).distinct.mkString(", ")}] " +
          s"cannot reference entities in catalog [$targetCatalogName]")
    }
    val viewIdentifiers = viewChain.map { parts =>
      val nsParts = parts.drop(1).init
      val ns = Namespace.of(nsParts: _*)
      TableIdentifier.of(ns, parts.last)
    }
    new util.ArrayList[TableIdentifier](viewIdentifiers.asJava)
  }

  /**
   * Build or extend the view chain by appending the current view's fully qualified identifier.
   *
   * @param nameParts the current view's identifier parts (may be 1, 2, or 3+ parts)
   * @param viewCatalogAndNamespace the view's catalog and namespace prefix
   * @param existingChain the existing view chain from outer views (empty for the outermost view)
   * @param isCatalog function to check if a name is a registered catalog
   */
  def buildViewChain(
      nameParts: Seq[String],
      viewCatalogAndNamespace: Seq[String],
      existingChain: Seq[Seq[String]],
      isCatalog: String => Boolean): Seq[Seq[String]] = {
    val currentViewParts = nameParts match {
      case Seq(name) =>
        viewCatalogAndNamespace :+ name
      case parts if !isCatalog(parts.head) =>
        viewCatalogAndNamespace.head +: parts
      case parts =>
        parts
    }
    existingChain :+ currentViewParts
  }

  /**
   * Load a table with referenced-by support, dispatching to the appropriate loadTable overload
   * based on whether the catalog supports referenced-by and whether time travel is requested.
   */
  def loadTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      referencedBy: java.util.List[TableIdentifier],
      timeTravelVersion: Option[String] = None,
      timeTravelTimestamp: Option[Expression] = None): Table = {
    catalog match {
      case supportsReferencedBy: SparkSupportsReferencedBy =>
        loadTableWithTimeTravel(
          supportsReferencedBy,
          ident,
          referencedBy,
          timeTravelVersion,
          timeTravelTimestamp)
      case c if c.asTableCatalog.isInstanceOf[SparkSupportsReferencedBy] =>
        loadTableWithTimeTravel(
          c.asTableCatalog.asInstanceOf[SparkSupportsReferencedBy],
          ident,
          referencedBy,
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

  private def loadTableWithTimeTravel(
      supportsReferencedBy: SparkSupportsReferencedBy,
      ident: Identifier,
      referencedBy: java.util.List[TableIdentifier],
      timeTravelVersion: Option[String],
      timeTravelTimestamp: Option[Expression]): Table = {
    (timeTravelVersion, timeTravelTimestamp) match {
      case (Some(version), _) =>
        supportsReferencedBy.loadTable(ident, version, referencedBy)
      case (_, Some(timestamp)) =>
        supportsReferencedBy.loadTable(ident, timestamp.eval().asInstanceOf[Long], referencedBy)
      case _ =>
        supportsReferencedBy.loadTable(ident, referencedBy)
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
