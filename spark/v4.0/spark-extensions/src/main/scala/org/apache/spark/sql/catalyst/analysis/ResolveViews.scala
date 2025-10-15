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

import org.apache.iceberg.spark.ContextAwareTableCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.expressions.UpCast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.UnResolvedRelationFromView
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.MetadataBuilder

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = applyWithViewChain(plan, Seq.empty)

  private def applyWithViewChain(
      plan: LogicalPlan,
      currentViewChain: Seq[Seq[String]]): LogicalPlan = plan resolveOperators {
    case u@UnresolvedRelation(nameParts, _, _)
      if catalogManager.v1SessionCatalog.isTempView(nameParts) =>
      u

    case u@UnresolvedRelation(parts@CatalogAndIdentifier(catalog, ident), _, _) =>
      ViewUtil.loadView(catalog, ident)
        .map(view => createViewRelation(parts, view, currentViewChain))
        .getOrElse(u)

    case u@UnresolvedTableOrView(CatalogAndIdentifier(catalog, ident), _, _) =>
      ViewUtil.loadView(catalog, ident)
        .map(_ => ResolvedV2View(catalog.asViewCatalog, ident))
        .getOrElse(u)

    case u @ UnResolvedRelationFromView(
      tableParts @ CatalogAndIdentifier(catalog, tableIdent), viewChain, options, isStreaming) =>
      
      // Check for recursive cycles before attempting to load the table
      // Normalize both the current table and view chain for comparison
      def normalize(parts: Seq[String]): String = {
        parts match {
          case Seq(catalog, namespace, table) => s"$catalog.$namespace.$table"
          case Seq(namespace, table) => s"$namespace.$table"
          case Seq(table) => table
          case _ => parts.mkString(".")
        }
      }
      
      val normalizedTable = normalize(tableParts)
      val matchesViewInChain = viewChain.exists { viewParts =>
        val normalizedView = normalize(viewParts)
        normalizedView == normalizedTable ||
        normalizedView.endsWith("." + normalizedTable) ||
        normalizedTable.endsWith("." + normalizedView)
      }
      
      if (matchesViewInChain) {
        val currentView = normalize(viewChain.head)
        val cycleStart = normalizedTable
        val cyclePath = (viewChain.tail.map(normalize) :+ cycleStart).mkString(" -> ")
        throw new IcebergAnalysisException(
          s"Recursive cycle in view detected: $currentView (cycle: $currentView -> $cyclePath)")
      }
      
      val context = new java.util.HashMap[String, Object]()
      val viewIdentifierList = viewChain.map(_.mkString(".")).mkString(",")
      context.put(
        org.apache.iceberg.catalog.ContextAwareTableCatalog.VIEW_IDENTIFIER_KEY, viewIdentifierList)
      
      // First check if this is a view reference
      val maybeView = ViewUtil.loadView(catalog, tableIdent)
      if (maybeView.isDefined) {
        // It's a view, not a table - recursively resolve it
        val view = maybeView.get
        val viewPlan = createViewRelation(tableParts, view, viewChain)
        viewPlan
      } else {
        // It's a table reference, try to load it
        val table = catalog match {
          case contextAwareCatalog: ContextAwareTableCatalog =>
            contextAwareCatalog.loadTableWithContext(tableIdent, context)
          case catalog if catalog.asTableCatalog.isInstanceOf[ContextAwareTableCatalog] =>
            catalog.asTableCatalog.asInstanceOf[ContextAwareTableCatalog]
              .loadTableWithContext(tableIdent, context)
          case _ =>
            catalog.asTableCatalog.loadTable(tableIdent)
        }
        DataSourceV2Relation.create(table, Some(catalog), Some(tableIdent), options)
      }

    case c@CreateIcebergView(ResolvedIdentifier(_, _), _, query, columnAliases, columnComments, _, _, _, _, _, _)
      if query.resolved && !c.rewritten =>
      val aliased = aliasColumns(query, columnAliases, columnComments)
      c.copy(query = aliased, queryColumnNames = query.schema.fieldNames, rewritten = true)
  }

  private def aliasColumns(
    plan: LogicalPlan,
    columnAliases: Seq[String],
    columnComments: Seq[Option[String]]): LogicalPlan = {
    if (columnAliases.isEmpty || columnAliases.length != plan.output.length) {
      plan
    } else {
      val projectList = plan.output.zipWithIndex.map { case (attr, pos) =>
        if (columnComments.apply(pos).isDefined) {
          val meta = new MetadataBuilder().putString("comment", columnComments.apply(pos).get).build()
          Alias(attr, columnAliases.apply(pos))(explicitMetadata = Some(meta))
        } else {
          Alias(attr, columnAliases.apply(pos))()
        }
      }
      Project(projectList, plan)
    }
  }


  private def createViewRelation(
      nameParts: Seq[String],
      view: View,
      currentViewChain: Seq[Seq[String]]): LogicalPlan = {
    val parsed = parseViewText(nameParts.quoted, view.query)

    // Apply any necessary rewrites to preserve correct resolution
    val viewCatalogAndNamespace: Seq[String] = view.currentCatalog +: view.currentNamespace.toSeq
    val qualifiedNameParts = Seq(view.currentNamespace.mkString("."), nameParts.last)
    val updatedViewChain = currentViewChain :+ nameParts
    val rewritten = rewriteIdentifiers(parsed, viewCatalogAndNamespace, Some(qualifiedNameParts), updatedViewChain)

    // Apply the field aliases and column comments
    // This logic differs from how Spark handles views in SessionCatalog.fromCatalogTable.
    // This is more strict because it doesn't allow resolution by field name.
    val aliases = view.schema.fields.zipWithIndex.map { case (expected, pos) =>
      val attr = GetColumnByOrdinal(pos, expected.dataType)
      Alias(UpCast(attr, expected.dataType), expected.name)(explicitMetadata = Some(expected.metadata))
    }

    SubqueryAlias(nameParts, Project(aliases, rewritten))
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    val origin = Origin(
      objectType = Some("VIEW"),
      objectName = Some(name)
    )

    try {
      CurrentOrigin.withOrigin(origin) {
        spark.sessionState.sqlParser.parseQuery(viewText)
      }
    } catch {
      case _: ParseException =>
        throw QueryCompilationErrors.invalidViewNameError(name)
    }
  }

  private def rewriteIdentifiers(
      plan: LogicalPlan,
      catalogAndNamespace: Seq[String],
      viewIdentifier: Option[Seq[String]] = None,
      viewChain: Seq[Seq[String]] = Seq.empty): LogicalPlan = {
    // Substitute CTEs and Unresolved Ordinals within the view, then rewrite unresolved functions and relations
    val substituted = SubstituteUnresolvedOrdinals.apply(CTESubstitution.apply(plan))
    val withQualifiedFunctions = qualifyFunctionIdentifiers(substituted, catalogAndNamespace)
    val withQualifiedTables = qualifyTableIdentifiers(
      withQualifiedFunctions,
      catalogAndNamespace,
      viewIdentifier,
      viewChain)
    // Recursively apply resolution with the current view chain
    applyWithViewChain(withQualifiedTables, viewChain)
  }

  private def qualifyFunctionIdentifiers(
    plan: LogicalPlan,
    catalogAndNamespace: Seq[String]): LogicalPlan = plan transformExpressions {
    case u@UnresolvedFunction(Seq(name), _, _, _, _, _, _) =>
      if (!isBuiltinFunction(name)) {
        u.copy(nameParts = catalogAndNamespace :+ name)
      } else {
        u
      }
    case u@UnresolvedFunction(parts, _, _, _, _, _, _) if !isCatalog(parts.head) =>
      u.copy(nameParts = catalogAndNamespace.head +: parts)
  }

  /**
   * Qualify table identifiers with default catalog and namespace if necessary.
   */
  private def qualifyTableIdentifiers(
      child: LogicalPlan,
      catalogAndNamespace: Seq[String],
      viewIdentifier: Option[Seq[String]],
      viewChain: Seq[Seq[String]]): LogicalPlan = {
    child transform {
      case u @ UnresolvedRelation(parts, options, isStreaming) =>
        val qualifiedTableId = parts match {
          case Seq(table) => catalogAndNamespace :+ table
          case _ if !isCatalog(parts.head) => catalogAndNamespace.head +: parts
          case _ => parts // fallback for other cases
        }

        viewIdentifier match {
          case Some(viewId) =>
            val fullViewChain = viewChain :+ viewId
            UnResolvedRelationFromView(qualifiedTableId, fullViewChain, options, isStreaming)
          case _ =>
            u.copy(multipartIdentifier = qualifiedTableId)
        }
      case other =>
        other.transformExpressions {
          case subquery: SubqueryExpression =>
            subquery.withNewPlan(qualifyTableIdentifiers(subquery.plan, catalogAndNamespace, viewIdentifier, viewChain))
        }
    }
  }

  private def isCatalog(name: String): Boolean = {
    catalogManager.isCatalogRegistered(name)
  }

  private def isBuiltinFunction(name: String): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(FunctionIdentifier(name))
  }
}
