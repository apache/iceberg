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

import java.util.Locale
import org.apache.iceberg.catalog.LoadContext
import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.iceberg.spark.SparkSupportsLoadContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.expressions.UpCast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.{View => ViewNode}
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.UnResolvedRelationFromView
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder
import scala.collection.mutable

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UnresolvedRelation(nameParts, _, _)
        if catalogManager.v1SessionCatalog.isTempView(nameParts) =>
      u

    case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog, ident), _, _) =>
      ViewUtil
        .loadView(catalog, ident)
        .map(createViewRelation(parts, catalog, ident, _))
        .getOrElse(u)

    case u @ UnResolvedRelationFromView(CatalogAndIdentifier(catalog, ident), _, _, _, _, ts)
        if ts.forall(_.resolved) =>
      resolveRelationFromView(u, catalog, ident)

    case u @ UnresolvedTableOrView(CatalogAndIdentifier(catalog, ident), _, _) =>
      ViewUtil
        .loadView(catalog, ident)
        .map(_ => ResolvedV2View(catalog.asViewCatalog, ident))
        .getOrElse(u)

    case c @ CreateIcebergView(
          ResolvedIdentifier(_, _),
          _,
          query,
          columnAliases,
          columnComments,
          _,
          _,
          _,
          _,
          _,
          _,
          _) if query.resolved && !c.rewritten =>
      val aliased = aliasColumns(query, columnAliases, columnComments)
      c.copy(
        query = aliased,
        queryColumnNames = query.schema.fieldNames.toIndexedSeq,
        rewritten = true)
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
          val meta =
            new MetadataBuilder().putString("comment", columnComments.apply(pos).get).build()
          Alias(attr, columnAliases.apply(pos))(explicitMetadata = Some(meta))
        } else {
          Alias(attr, columnAliases.apply(pos))()
        }
      }
      Project(projectList, plan)
    }
  }

  private def resolveRelationFromView(
      relation: UnResolvedRelationFromView,
      catalog: CatalogPlugin,
      ident: Identifier): LogicalPlan = {
    val parts = relation.tableMultipartIdentifier
    val referencedBy = ViewUtil.buildReferencedByChain(relation.viewChain, catalog.name())
    catalog match {
      case contextual: TableCatalog with SparkSupportsLoadContext if !relation.isStreaming =>
        val context = LoadContext.builder().referencedBy(referencedBy).build()
        val timeTravelSpec = TimeTravelSpec.create(
          relation.timeTravelTimestamp,
          relation.timeTravelVersion,
          conf.sessionLocalTimeZone)
        try {
          ViewUtil.loadTable(contextual, ident, context, timeTravelSpec) match {
            case _: V1Table =>
              toUnresolvedRelation(relation)
            case table =>
              SubqueryAlias(
                catalog.name +: ident.asMultipartIdentifier,
                DataSourceV2Relation.create(table, Some(catalog), Some(ident), relation.options))
          }
        } catch {
          case _: NoSuchTableException =>
            ViewUtil
              .loadView(catalog, ident, context)
              .map(createViewRelation(parts, catalog, ident, _, relation.viewChain))
              .getOrElse(toUnresolvedRelation(relation))
        }

      case _ =>
        toUnresolvedRelation(relation)
    }
  }

  private def toUnresolvedRelation(relation: UnResolvedRelationFromView): LogicalPlan = {
    val unresolved =
      UnresolvedRelation(relation.tableMultipartIdentifier, relation.options, relation.isStreaming)
    if (relation.timeTravelVersion.isEmpty && relation.timeTravelTimestamp.isEmpty) {
      unresolved
    } else {
      RelationTimeTravel(unresolved, relation.timeTravelTimestamp, relation.timeTravelVersion)
    }
  }

  private def createViewRelation(
      nameParts: Seq[String],
      catalog: CatalogPlugin,
      ident: Identifier,
      view: View,
      existingChain: Seq[Seq[String]] = Seq.empty): LogicalPlan = {
    // Session settings are read before switching to the view's captured SQL configs, which
    // don't include them
    val sessionConf = spark.sessionState.conf
    val viewIdent = catalog.name +: ident.asMultipartIdentifier
    val viewChain = if (ViewUtil.referencedByEnabled(sessionConf)) {
      existingChain :+ viewIdent
    } else {
      Seq.empty
    }
    val compensate = viewSchemaMode(sessionConf) == SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION

    // Like Spark's own views, the view body is parsed and analyzed with the SQL configs captured
    // at creation, and nested views are expanded within the outer view's analysis
    ViewUtil.withNestedView(viewIdent, sessionConf.maxNestedViewDepth) {
      val viewConf = ViewNode.effectiveSQLConf(ViewUtil.sqlConfigs(view), isTempView = false)
      SQLConf.withExistingConf(viewConf) {
        val parsed = parseViewText(nameParts.quoted, view.query)

        // Apply any necessary rewrites to preserve correct resolution
        val viewCatalogAndNamespace: Seq[String] =
          view.currentCatalog +: view.currentNamespace.toSeq
        val rewritten = rewriteIdentifiers(parsed, viewCatalogAndNamespace, viewChain)

        val columns = viewColumns(nameParts.quoted, view, viewConf, compensate)
        val analyzed = spark.sessionState.analyzer.execute(Project(columns, rewritten))
        spark.sessionState.analyzer.checkAnalysis(analyzed)
        SubqueryAlias(nameParts, analyzed)
      }
    }
  }

  private def viewColumns(
      viewName: String,
      view: View,
      viewConf: SQLConf,
      compensate: Boolean): Seq[NamedExpression] = {
    val queryColumnNames = view.queryColumnNames.toIndexedSeq
    val bindByName = queryColumnNames.length == view.schema.length
    val normalizeColName: String => String =
      if (viewConf.caseSensitiveAnalysis) identity else _.toLowerCase(Locale.ROOT)
    val nameToCounts = queryColumnNames.groupBy(normalizeColName).map { case (name, names) =>
      name -> names.length
    }
    val nameToCurrentOrdinal = mutable.HashMap.empty[String, Int]

    view.schema.fields.zipWithIndex.map { case (expected, pos) =>
      val attr = if (bindByName) {
        val name = queryColumnNames(pos)
        val normalizedName = normalizeColName(name)
        val ordinal = nameToCurrentOrdinal.getOrElse(normalizedName, 0)
        nameToCurrentOrdinal(normalizedName) = ordinal + 1
        GetViewColumnByNameAndOrdinal(viewName, name, ordinal, nameToCounts(normalizedName), None)
      } else {
        GetColumnByOrdinal(pos, expected.dataType)
      }

      val cast = if (compensate) {
        Cast(attr, expected.dataType, ansiEnabled = true)
      } else {
        UpCast(attr, expected.dataType)
      }
      Alias(cast, expected.name)(explicitMetadata = Some(expected.metadata))
    }.toIndexedSeq
  }

  // Read on every resolution rather than cached, so that SET takes effect within a session.
  private def viewSchemaMode(sessionConf: SQLConf): String =
    parseSchemaBindingMode(
      sessionConf.getConfString(
        SparkSQLProperties.VIEW_SCHEMA_BINDING_MODE,
        SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING))

  private def parseSchemaBindingMode(mode: String): String = {
    val normalized = mode.trim
    if (normalized.equalsIgnoreCase(SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING)) {
      SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING
    } else if (normalized.equalsIgnoreCase(SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION)) {
      SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION
    } else {
      throw new IllegalArgumentException(
        s"Invalid value for ${SparkSQLProperties.VIEW_SCHEMA_BINDING_MODE}: $mode, expected " +
          s"${SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING} or " +
          s"${SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION}")
    }
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    val origin = Origin(objectType = Some("VIEW"), objectName = Some(name))

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
      viewChain: Seq[Seq[String]]): LogicalPlan = {
    // Substitute CTEs and Unresolved Ordinals within the view, then rewrite unresolved functions and relations
    qualifyTableIdentifiers(
      qualifyFunctionIdentifiers(
        SubstituteUnresolvedOrdinals.apply(CTESubstitution.apply(plan)),
        catalogAndNamespace),
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

  /**
   * Qualify table identifiers with default catalog and namespace if necessary. With a non-empty
   * view chain, relations are also tagged with the chain so they load with referenced-by.
   */
  private def qualifyTableIdentifiers(
      child: LogicalPlan,
      catalogAndNamespace: Seq[String],
      viewChain: Seq[Seq[String]]): LogicalPlan =
    child transform {
      case RelationTimeTravel(UnresolvedRelation(parts, options, isStreaming), timestamp, version)
          if viewChain.nonEmpty =>
        UnResolvedRelationFromView(
          ViewUtil.qualifyParts(parts, catalogAndNamespace, isCatalog),
          viewChain,
          options,
          isStreaming,
          timeTravelVersion = version,
          timeTravelTimestamp = timestamp)
      case UnresolvedRelation(parts, options, isStreaming) if viewChain.nonEmpty =>
        UnResolvedRelationFromView(
          ViewUtil.qualifyParts(parts, catalogAndNamespace, isCatalog),
          viewChain,
          options,
          isStreaming)
      case t @ RelationTimeTravel(u @ UnresolvedRelation(parts, _, _), _, _) =>
        t.copy(relation = u.copy(multipartIdentifier =
          ViewUtil.qualifyParts(parts, catalogAndNamespace, isCatalog)))
      case u @ UnresolvedRelation(Seq(table), _, _) =>
        u.copy(multipartIdentifier = catalogAndNamespace :+ table)
      case u @ UnresolvedRelation(parts, _, _) if !isCatalog(parts.head) =>
        u.copy(multipartIdentifier = catalogAndNamespace.head +: parts)
      case other =>
        other.transformExpressions { case subquery: SubqueryExpression =>
          subquery.withNewPlan(
            qualifyTableIdentifiers(subquery.plan, catalogAndNamespace, viewChain))
        }
    }

  private def isCatalog(name: String): Boolean = {
    catalogManager.isCatalogRegistered(name)
  }

  private def isBuiltinFunction(name: String): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(FunctionIdentifier(name))
  }
}
