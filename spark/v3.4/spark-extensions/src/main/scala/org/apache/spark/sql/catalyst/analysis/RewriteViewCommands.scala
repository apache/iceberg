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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.CreateView
import org.apache.spark.sql.catalyst.plans.logical.DropView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ShowViews
import org.apache.spark.sql.catalyst.plans.logical.View
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.DropIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowIcebergViews
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.UNRESOLVED_FUNCTION
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog
import scala.collection.mutable

/**
 * ResolveSessionCatalog exits early for some v2 View commands,
 * thus they are pre-substituted here and then handled in ResolveViews
 */
case class RewriteViewCommands(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case DropView(ResolvedIdent(resolved), ifExists) =>
      DropIcebergView(resolved, ifExists)

    case CreateView(ResolvedIdent(resolved), userSpecifiedColumns, comment, properties,
    Some(queryText), query, allowExisting, replace) =>
      val q = CTESubstitution.apply(query)
      verifyTemporaryObjectsDontExist(resolved, q)
      CreateIcebergView(child = resolved,
        queryText = queryText,
        query = q,
        columnAliases = userSpecifiedColumns.map(_._1),
        columnComments = userSpecifiedColumns.map(_._2.orElse(Option.empty)),
        comment = comment,
        properties = properties,
        allowExisting = allowExisting,
        replace = replace)

    case ShowViews(UnresolvedNamespace(Seq()), pattern, output) =>
      if (ViewUtil.isViewCatalog(catalogManager.currentCatalog)) {
        ShowIcebergViews(ResolvedNamespace(catalogManager.currentCatalog, catalogManager.currentNamespace),
          pattern, output)
      } else {
        plan
      }

    case ShowViews(UnresolvedNamespace(CatalogAndNamespace(catalog, ns)), pattern, output)
      if ViewUtil.isViewCatalog(catalog) =>
      ShowIcebergViews(ResolvedNamespace(catalog, ns), pattern, output)

    // needs to be done here instead of in ResolveViews, so that a V2 view can be resolved before the Analyzer
    // tries to resolve it, which would result in an error, saying that V2 views aren't supported
    case u@UnresolvedView(ResolvedView(resolved), _, _, _) =>
      ViewUtil.loadView(resolved.catalog, resolved.identifier)
        .map(_ => ResolvedV2View(resolved.catalog.asViewCatalog, resolved.identifier))
        .getOrElse(u)
  }

  private def isTempView(nameParts: Seq[String]): Boolean = {
    catalogManager.v1SessionCatalog.isTempView(nameParts)
  }

  private def isTempFunction(nameParts: Seq[String]): Boolean = {
    if (nameParts.size > 1) {
      return false
    }
    catalogManager.v1SessionCatalog.isTemporaryFunction(nameParts.asFunctionIdentifier)
  }

  object ResolvedIdent {
    def unapply(unresolved: UnresolvedIdentifier): Option[ResolvedIdentifier] = unresolved match {
      case UnresolvedIdentifier(nameParts, true) if isTempView(nameParts) =>
        None

      case UnresolvedIdentifier(CatalogAndIdentifier(catalog, ident), _) if ViewUtil.isViewCatalog(catalog) =>
        Some(ResolvedIdentifier(catalog, ident))

      case _ =>
        None
    }
  }

  /**
   * Permanent views are not allowed to reference temp objects
   */
  private def verifyTemporaryObjectsDontExist(
    identifier: ResolvedIdentifier,
    child: LogicalPlan): Unit = {
    val tempViews = collectTemporaryViews(child)
    if (tempViews.nonEmpty) {
      throw invalidRefToTempObject(identifier, tempViews.map(v => v.quoted).mkString("[", ", ", "]"), "view")
    }

    val tempFunctions = collectTemporaryFunctions(child)
    if (tempFunctions.nonEmpty) {
      throw invalidRefToTempObject(identifier, tempFunctions.mkString("[", ", ", "]"), "function")
    }
  }

  private def invalidRefToTempObject(ident: ResolvedIdentifier, tempObjectNames: String, tempObjectType: String) = {
    new AnalysisException(String.format("Cannot create view %s.%s that references temporary %s: %s",
      ident.catalog.name(), ident.identifier, tempObjectType, tempObjectNames))
  }

  /**
   * Collect all temporary views and return the identifiers separately
   */
  private def collectTemporaryViews(child: LogicalPlan): Seq[Seq[String]] = {
    def collectTempViews(child: LogicalPlan): Seq[Seq[String]] = {
      child.flatMap {
        case unresolved: UnresolvedRelation if isTempView(unresolved.multipartIdentifier) =>
          Seq(unresolved.multipartIdentifier)
        case view: View if view.isTempView => Seq(view.desc.identifier.nameParts)
        case plan => plan.expressions.flatMap(_.flatMap {
          case e: SubqueryExpression => collectTempViews(e.plan)
          case _ => Seq.empty
        })
      }.distinct
    }

    collectTempViews(child)
  }

  private object ResolvedView {
    def unapply(identifier: Seq[String]): Option[ResolvedV2View] = identifier match {
      case nameParts if isTempView(nameParts) =>
        None

      case CatalogAndIdentifier(catalog, ident) if ViewUtil.isViewCatalog(catalog) =>
        ViewUtil.loadView(catalog, ident).flatMap(_ => Some(ResolvedV2View(catalog.asViewCatalog, ident)))

      case _ =>
        None
    }
  }

  /**
   * Collect the names of all temporary functions.
   */
  private def collectTemporaryFunctions(child: LogicalPlan): Seq[String] = {
    val tempFunctions = new mutable.HashSet[String]()
    child.resolveExpressionsWithPruning(_.containsAnyPattern(UNRESOLVED_FUNCTION)) {
      case f @ UnresolvedFunction(nameParts, _, _, _, _) if isTempFunction(nameParts) =>
        tempFunctions += nameParts.head
        f
      case e: SubqueryExpression =>
        tempFunctions ++= collectTemporaryFunctions(e.plan)
        e
    }
    tempFunctions.toSeq
  }
}
