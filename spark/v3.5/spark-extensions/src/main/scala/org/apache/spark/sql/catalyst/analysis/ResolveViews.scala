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
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.checkAnalysis
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.execute
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.CatalogTableViewDescription
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation
import org.apache.spark.sql.catalyst.plans.logical.IcebergView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RenameTable
import org.apache.spark.sql.catalyst.plans.logical.SetViewProperties
import org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable
import org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.UnsetViewProperties
import org.apache.spark.sql.catalyst.plans.logical.views.AlterV2View
import org.apache.spark.sql.catalyst.plans.logical.views.DescribeV2View
import org.apache.spark.sql.catalyst.plans.logical.views.RenameV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowCreateV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowV2ViewProperties
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{View => V2View}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.connector.catalog.ViewChange
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u@UnresolvedRelation(nameParts, _, _) if catalogManager.v1SessionCatalog.isTempView(nameParts) =>
      u

    case u@UnresolvedRelation(
    parts@NonSessionCatalogAndIdentifier(catalog, ident), _, _) if !isSQLOnFile(parts) =>
      loadView(catalog, ident)
        .map(createViewRelation(parts.quoted, _))
        .getOrElse(u)

    case ShowCreateTable(ResolvedV2View(_, ident, view), _, _) =>
      ShowCreateV2View(V2ViewDescription(ident.quoted, view))

    case DescribeRelation(ResolvedV2View(_, ident, view), _, isExtended, _) =>
      DescribeV2View(V2ViewDescription(ident.quoted, view), isExtended)

    case ShowTableProperties(ResolvedV2View(_, ident, view), propertyKeys, _) =>
      ShowV2ViewProperties(V2ViewDescription(ident.quoted, view), propertyKeys)

    case SetViewProperties(ResolvedV2View(catalog, ident, _), props) =>
      val changes = props.map {
        case (property, value) => ViewChange.setProperty(property, value)
      }.toSeq
      AlterV2View(catalog, ident, changes)

    case UnsetViewProperties(ResolvedV2View(catalog, ident, _), propertyKeys, ifExists) =>
      if (!ifExists) {
        val view = catalog.loadView(ident)
        propertyKeys.filterNot(view.properties.containsKey).foreach { property =>
          cannotUnsetNonExistentViewProperty(ident, property)
        }
      }
      val changes = propertyKeys.map(ViewChange.removeProperty)
      AlterV2View(catalog, ident, changes)

    case RenameTable(ResolvedV2View(oldCatalog, oldIdent, _),
    NonSessionCatalogAndIdentifier(newCatalog, newIdent), true) =>
      if (oldCatalog.name != newCatalog.name) {
        cannotMoveViewBetweenCatalogs(
          oldCatalog.name, newCatalog.name)
      }
      RenameV2View(oldCatalog, oldIdent, newIdent)

    case view@IcebergView(CatalogTableViewDescription(desc), isTempView, child) if !child.resolved =>
      // Resolve all the UnresolvedRelations and Views in the child.
      val newChild = AnalysisContext.withAnalysisContext(desc) {
        val nestedViewDepth = AnalysisContext.get.nestedViewDepth
        val maxNestedViewDepth = AnalysisContext.get.maxNestedViewDepth
        if (nestedViewDepth > maxNestedViewDepth) {
          throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
            desc.identifier, maxNestedViewDepth, view)
        }
        SQLConf.withExistingConf(IcebergView.effectiveSQLConf(desc.viewSQLConfigs, isTempView)) {
          execute(child)
        }
      }
      // Fail the analysis eagerly because outside AnalysisContext, the unresolved operators
      // inside a view maybe resolved incorrectly.
      checkAnalysis(newChild)
      view.copy(view.desc, view.isTempView, child = newChild)

    case p@SubqueryAlias(_, view: IcebergView) =>
      p.copy(child = resolveViews(view))

  }

  def cannotUnsetNonExistentViewProperty(ident: Identifier, property: String): Throwable =
    throw new AnalysisException(
      s"Attempted to unset non-existent property '$property' in view $ident")

  def cannotMoveViewBetweenCatalogs(oldCatalog: String, newCatalog: String): Throwable =
    throw new AnalysisException(
      s"Cannot move view between catalogs: from=$oldCatalog and to=$newCatalog")

  def lookupView(catalog: CatalogPlugin, ident: Identifier): Option[LogicalPlan] =
    loadView(catalog, ident).map {
      case view if CatalogV2Util.isSessionCatalog(catalog) =>
        ResolvedPersistentView(catalog, ident, view.schema)
      case view =>
        ResolvedV2View(CatalogHelper(catalog).asViewCatalog, ident, view)
    }

  // The current catalog and namespace may be different from when the view was created, we must
  // resolve the view logical plan here, with the catalog and namespace stored in view metadata.
  // This is done by keeping the catalog and namespace in `AnalysisContext`, and analyzer will
  // look at `AnalysisContext.catalogAndNamespace` when resolving relations with single-part name.
  // If `AnalysisContext.catalogAndNamespace` is non-empty, analyzer will expand single-part names
  // with it, instead of current catalog and namespace.
  def resolveViews(plan: LogicalPlan): LogicalPlan = plan match {
    // The view's child should be a logical plan parsed from the `desc.viewText`, the variable
    // `viewText` should be defined, or else we throw an error on the generation of the View
    // operator.
//    case view@IcebergView(CatalogTableViewDescription(desc), isTempView, child) if !child.resolved =>
    case view@IcebergView(CatalogTableViewDescription(desc), isTempView, child) =>
      // Resolve all the UnresolvedRelations and Views in the child.
      val newChild = AnalysisContext.withAnalysisContext(desc) {
        val nestedViewDepth = AnalysisContext.get.nestedViewDepth
        val maxNestedViewDepth = AnalysisContext.get.maxNestedViewDepth
        if (nestedViewDepth > maxNestedViewDepth) {
          throw QueryCompilationErrors.viewDepthExceedsMaxResolutionDepthError(
            desc.identifier, maxNestedViewDepth, view)
        }
        SQLConf.withExistingConf(IcebergView.effectiveSQLConf(desc.viewSQLConfigs, isTempView)) {
          execute(child)
        }
      }
      // Fail the analysis eagerly because outside AnalysisContext, the unresolved operators
      // inside a view maybe resolved incorrectly.
      checkAnalysis(newChild)
      view.copy(child = newChild)
    case p@SubqueryAlias(_, view: IcebergView) =>
      p.copy(child = resolveViews(view))
    case _ => plan
  }


  private def isSQLOnFile(parts: Seq[String]): Boolean = parts match {
    case Seq(_, path) if path.contains("/") => true
    case _ => false
  }

  private def createViewRelation(name: String, view: V2View): LogicalPlan = {
    if (!SparkSession.active.sessionState.catalogManager.isCatalogRegistered(view.currentCatalog)) {
      throw new AnalysisException(
        s"Invalid current catalog '${view.currentCatalog}' in view '$name'")
    }

    val child = parseViewText(name, view.query)
    val desc = V2ViewDescription(name, view)
    val qualifiedChild = desc.viewCatalogAndNamespace match {
      case Seq() =>
        // Views from Spark 2.2 or prior do not store catalog or namespace,
        // however its sql text should already be fully qualified.
        child
      case catalogAndNamespace =>
        // Substitute CTEs within the view before qualifying table identifiers
        qualifyTableIdentifiers(CTESubstitution.apply(child), catalogAndNamespace)
    }

    // The relation is a view, so we wrap the relation by:
    // 1. Add a [[View]] operator over the relation to keep track of the view desc;
    // 2. Wrap the logical plan in a [[SubqueryAlias]] which tracks the name of the view.
    SubqueryAlias(name, IcebergView(desc, false, qualifiedChild))
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    try {
      SparkSession.active.sessionState.sqlParser.parsePlan(viewText)
    } catch {
      case _: ParseException =>
        throw QueryCompilationErrors.invalidViewText(viewText, name)
    }
  }

  def loadView(catalog: CatalogPlugin, ident: Identifier): Option[View] = catalog match {
    case viewCatalog: ViewCatalog =>
      try {
        Option(viewCatalog.loadView(ident))
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }


  /**
   * Qualify table identifiers with default catalog and namespace if necessary.
   */
  private def qualifyTableIdentifiers(
                                       child: LogicalPlan,
                                       catalogAndNamespace: Seq[String]): LogicalPlan =
    child transform {
      case u@UnresolvedRelation(Seq(table), _, _) =>
        u.copy(multipartIdentifier = catalogAndNamespace :+ table)
      case u@UnresolvedRelation(parts, _, _)
        if !SparkSession.active.sessionState.catalogManager.isCatalogRegistered(parts.head) =>
        u.copy(multipartIdentifier = catalogAndNamespace.head +: parts)
    }


  implicit class CatalogHelper(plugin: CatalogPlugin) {
    def asViewCatalog: ViewCatalog = plugin match {
      case viewCatalog: ViewCatalog =>
        viewCatalog
      case _ =>
        throw QueryCompilationErrors.missingCatalogAbilityError(plugin, "views")
    }
  }
}
