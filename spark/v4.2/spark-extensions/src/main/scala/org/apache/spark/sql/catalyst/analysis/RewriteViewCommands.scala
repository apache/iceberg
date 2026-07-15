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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.plans.logical.CreateView
import org.apache.spark.sql.catalyst.plans.logical.DescribeRelation
import org.apache.spark.sql.catalyst.plans.logical.DropView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.RenameTable
import org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable
import org.apache.spark.sql.catalyst.plans.logical.ShowTableProperties
import org.apache.spark.sql.catalyst.plans.logical.ShowViews
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.DropIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowIcebergViews
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog

/**
 * ResolveSessionCatalog exits early for some v2 View commands,
 * thus they are pre-substituted here and then handled in ResolveViews
 */
case class RewriteViewCommands(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case DropView(ResolvedIdent(resolved), ifExists) =>
      DropIcebergView(resolved, ifExists)

    case RenameTable(ResolvedViewPlan(resolved), newName, isView @ true) =>
      RenameTable(resolved, newName, isView)

    case DescribeRelation(ResolvedViewPlan(resolved), isExtended, output) =>
      DescribeRelation(resolved, isExtended, output)

    case ShowCreateTable(ResolvedViewPlan(resolved), asSerde, output) =>
      ShowCreateTable(resolved, asSerde, output)

    case ShowTableProperties(ResolvedViewPlan(resolved), propertyKey, output) =>
      ShowTableProperties(resolved, propertyKey, output)

    case CreateView(
          ResolvedIdent(resolved),
          userSpecifiedColumns,
          comment,
          collation,
          properties,
          Some(queryText),
          query,
          allowExisting,
          replace,
          viewSchemaMode,
          _,
          _) =>
      val q = CTESubstitution.apply(query)
      CreateIcebergView(
        child = resolved,
        queryText = queryText,
        query = q,
        columnAliases = userSpecifiedColumns.map(_._1),
        columnComments = userSpecifiedColumns.map(_._2.orElse(Option.empty)),
        comment = comment,
        collation = collation,
        properties = properties,
        allowExisting = allowExisting,
        replace = replace,
        viewSchemaMode = viewSchemaMode)

    case view @ ShowViews(CurrentNamespace, pattern, output) =>
      if (ViewUtil.isIcebergViewCatalog(catalogManager.currentCatalog)) {
        ShowIcebergViews(
          ResolvedNamespace(
            catalogManager.currentCatalog,
            catalogManager.currentNamespace.toIndexedSeq),
          pattern,
          output)
      } else {
        view
      }

    case ShowViews(UnresolvedNamespace(CatalogAndNamespace(catalog, ns), _), pattern, output)
        if ViewUtil.isIcebergViewCatalog(catalog) =>
      ShowIcebergViews(ResolvedNamespace(catalog, ns), pattern, output)

    // Resolve the view before Spark rewrites session-catalog commands through the V1 path.
    case u @ UnresolvedView(ResolvedView(resolved), _, _, _) =>
      ViewUtil
        .loadView(resolved.catalog, resolved.identifier)
        .map(_ => ResolvedV2View(resolved.catalog.asViewCatalog, resolved.identifier))
        .getOrElse(u)
  }

  private def isTempView(nameParts: Seq[String]): Boolean = {
    catalogManager.v1SessionCatalog.isTempView(nameParts)
  }

  private object ResolvedIdent {
    def unapply(unresolved: UnresolvedIdentifier): Option[ResolvedIdentifier] = unresolved match {
      case UnresolvedIdentifier(nameParts, true) if isTempView(nameParts) =>
        None

      case UnresolvedIdentifier(CatalogAndIdentifier(catalog, ident), _)
          if ViewUtil.isIcebergViewCatalog(catalog) =>
        Some(ResolvedIdentifier(catalog, ident))

      case _ =>
        None
    }
  }

  private object ResolvedView {
    def unapply(identifier: Seq[String]): Option[ResolvedV2View] = identifier match {
      case nameParts if isTempView(nameParts) =>
        None

      case CatalogAndIdentifier(catalog, ident) if ViewUtil.isIcebergViewCatalog(catalog) =>
        ViewUtil
          .loadView(catalog, ident)
          .flatMap(_ => Some(ResolvedV2View(catalog.asViewCatalog, ident)))

      case _ =>
        None
    }
  }

  private object ResolvedViewPlan {
    def unapply(plan: LogicalPlan): Option[ResolvedV2View] = plan match {
      case UnresolvedTableOrView(ResolvedView(resolved), _, _, _) =>
        Some(resolved)

      case UnresolvedIdentifier(ResolvedView(resolved), _) =>
        Some(resolved)

      case _ =>
        None
    }
  }

}
