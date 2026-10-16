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
import org.apache.iceberg.spark.SparkSupportsLoadContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.expressions.UpCast
import org.apache.spark.sql.catalyst.plans.logical.{View => ViewNode}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.views.UnResolvedRelationFromView
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.DelegatingTable
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Expands Iceberg views when referenced-by is enabled, so that the tables and views they read are
 * loaded with the referencing view chain.
 *
 * Spark 4.2 expands views itself without passing the view identity to the catalog, so with
 * referenced-by enabled Iceberg catalogs leave views unresolved for this rule.
 */
case class ResolveReferencedByViews(spark: SparkSession)
    extends Rule[LogicalPlan]
    with LookupCatalog {

  import ResolveReferencedByViews.CheckedForView
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (ViewUtil.referencedByEnabled) {
      resolveWithReferencedBy(plan)
    } else {
      plan
    }
  }

  private def resolveWithReferencedBy(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UnresolvedRelation(nameParts, _, _) if isTempView(nameParts) =>
      u

    case u @ UnresolvedRelation(CatalogAndIdentifier(catalog, ident), _, true)
        if u.getTagValue(CheckedForView).isEmpty =>
      ViewUtil.loadView(catalog, ident).foreach { _ =>
        throw QueryCompilationErrors.permanentViewNotSupportedByStreamingReadingAPIError(
          ident.quoted)
      }
      markCheckedForView(u)

    case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog, ident), options, _)
        if !options.containsKey(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES) &&
          u.getTagValue(CheckedForView).isEmpty =>
      ViewUtil
        .loadView(catalog, ident)
        .map(createViewRelation(parts, catalog, ident, _))
        .getOrElse(markCheckedForView(u))

    case u @ UnResolvedRelationFromView(
          tableParts @ CatalogAndIdentifier(catalog, tableIdent),
          _,
          _,
          viewChain,
          options,
          isStreaming,
          timeTravelVersion,
          timeTravelTimestamp) if timeTravelTimestamp.forall(_.resolved) =>
      val referencedBy = ViewUtil.buildReferencedByChain(viewChain, catalog.name())
      val context = LoadContext.builder().referencedBy(referencedBy).build()
      val timeTravelSpec =
        TimeTravelSpec.create(timeTravelTimestamp, timeTravelVersion, conf.sessionLocalTimeZone)
      try {
        val table = ViewUtil.loadTable(catalog, tableIdent, context, timeTravelSpec)
        table match {
          case _: V1Table | _: DelegatingTable =>
            val relation = markCheckedForView(UnresolvedRelation(tableParts, options, isStreaming))
            if (timeTravelSpec.isEmpty) {
              relation
            } else {
              RelationTimeTravel(relation, timeTravelTimestamp, timeTravelVersion)
            }

          case _ =>
            val relationCatalog = catalog match {
              case contextual: TableCatalog with SparkSupportsLoadContext
                  if timeTravelSpec.isEmpty =>
                LoadContextTableCatalog(contextual, context)
              case _ => catalog
            }
            SubqueryAlias(
              tableIdent.toQualifiedNameParts(catalog),
              DataSourceV2Relation.create(
                table,
                Some(relationCatalog),
                Some(tableIdent),
                options,
                timeTravelSpec))
        }
      } catch {
        case _: NoSuchTableException =>
          ViewUtil
            .loadView(catalog, tableIdent, context)
            .map(view => createViewRelation(tableParts, catalog, tableIdent, view, viewChain))
            .getOrElse(throw tableOrViewNotFound(u))
      }
  }

  private def createViewRelation(
      nameParts: Seq[String],
      catalog: CatalogPlugin,
      ident: Identifier,
      view: View,
      existingChain: Seq[Seq[String]] = Seq.empty): LogicalPlan = {
    val desc = V1Table.toCatalogTable(catalog, ident, view)
    val parsed = parseViewText(desc.qualifiedName, view.queryText())
    val viewQueryCatalogAndNamespace = queryCatalogAndNamespace(catalog, ident, view)
    val viewChain = existingChain :+ ident.toQualifiedNameParts(catalog)
    val rewritten = rewriteIdentifiers(parsed, viewQueryCatalogAndNamespace, viewChain)
    val aliases = view
      .schema()
      .fields
      .zipWithIndex
      .map { case (expected, pos) =>
        val attr = GetColumnByOrdinal(pos, expected.dataType)
        val cast = if (isSchemaCompensation(view.schemaMode())) {
          Cast(attr, expected.dataType, ansiEnabled = true)
        } else {
          UpCast(attr, expected.dataType)
        }
        Alias(cast, expected.name)(explicitMetadata = Some(expected.metadata))
      }
      .toIndexedSeq

    SubqueryAlias(nameParts, ViewNode(desc, isTempView = false, Project(aliases, rewritten)))
  }

  private def queryCatalogAndNamespace(
      catalog: CatalogPlugin,
      ident: Identifier,
      view: View): Seq[String] = {
    val currentCatalog = Option(view.currentCatalog()).filter(_.nonEmpty).getOrElse(catalog.name())
    val currentNamespace = Option(view.currentNamespace()).map(_.toIndexedSeq).getOrElse {
      ident.namespace().toIndexedSeq
    }
    currentCatalog +: currentNamespace
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    val origin = CurrentOrigin.get.copy(objectType = Some("VIEW"), objectName = Some(name))
    CurrentOrigin.withOrigin(origin) {
      spark.sessionState.sqlParser.parseQuery(viewText)
    }
  }

  private def rewriteIdentifiers(
      plan: LogicalPlan,
      catalogAndNamespace: Seq[String],
      viewChain: Seq[Seq[String]]): LogicalPlan = {
    qualifyTableIdentifiers(
      qualifyFunctionIdentifiers(CTESubstitution.apply(plan), catalogAndNamespace),
      catalogAndNamespace,
      viewChain)
  }

  private def qualifyFunctionIdentifiers(
      plan: LogicalPlan,
      catalogAndNamespace: Seq[String]): LogicalPlan = plan transformExpressions {
    case u @ UnresolvedFunction(Seq(name), _, _, _, _, _, _) =>
      if (!isBuiltinFunction(name)) {
        u.copy(nameParts = catalogAndNamespace :+ name)
      } else {
        u
      }
    case u @ UnresolvedFunction(parts, _, _, _, _, _, _) if !isCatalog(parts.head) =>
      u.copy(nameParts = catalogAndNamespace.head +: parts)
  }

  private def qualifyTableIdentifiers(
      child: LogicalPlan,
      catalogAndNamespace: Seq[String],
      viewChain: Seq[Seq[String]]): LogicalPlan = {
    child transform {
      case UnresolvedRelation(parts, options, isStreaming) =>
        val qualifiedTableId = ViewUtil.qualifyParts(parts, catalogAndNamespace, isCatalog)
        UnResolvedRelationFromView(
          qualifiedTableId,
          parts,
          catalogAndNamespace,
          viewChain,
          options,
          isStreaming)
      case RelationTimeTravel(
            UnresolvedRelation(parts, options, isStreaming),
            timestampOpt,
            versionOpt) =>
        val qualifiedTableId = ViewUtil.qualifyParts(parts, catalogAndNamespace, isCatalog)
        UnResolvedRelationFromView(
          qualifiedTableId,
          parts,
          catalogAndNamespace,
          viewChain,
          options,
          isStreaming,
          timeTravelVersion = versionOpt,
          timeTravelTimestamp = timestampOpt)
      case other =>
        other.transformExpressions { case subquery: SubqueryExpression =>
          subquery.withNewPlan(
            qualifyTableIdentifiers(subquery.plan, catalogAndNamespace, viewChain))
        }
    }
  }

  private def tableOrViewNotFound(relation: UnResolvedRelationFromView): Throwable = {
    val catalogAndNamespace = relation.viewCatalogAndNamespace
    val searchPath = catalogManager
      .sqlResolutionPathEntries(catalogAndNamespace.head, catalogAndNamespace.tail)
      .map(QueryCompilationErrors.toSQLId)
    QueryCompilationErrors.tableOrViewNotFoundWithSearchPath(
      relation.nameParts,
      searchPath,
      relation.origin)
  }

  private def isSchemaCompensation(mode: String): Boolean = {
    mode != null && mode.equalsIgnoreCase(SchemaCompensation.toString)
  }

  private def markCheckedForView(relation: UnresolvedRelation): UnresolvedRelation = {
    relation.setTagValue(CheckedForView, ())
    relation
  }

  private def isTempView(nameParts: Seq[String]): Boolean = {
    catalogManager.v1SessionCatalog.isTempView(nameParts)
  }

  private def isCatalog(name: String): Boolean = {
    catalogManager.isCatalogRegistered(name)
  }

  private def isBuiltinFunction(name: String): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(name)
  }
}

object ResolveReferencedByViews {
  private val CheckedForView = TreeNodeTag[Unit]("iceberg.referenced-by.checked-for-view")
}
