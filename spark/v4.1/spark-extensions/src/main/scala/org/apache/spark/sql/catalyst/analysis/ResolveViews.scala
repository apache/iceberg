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

import org.apache.iceberg.spark.SparkSQLProperties
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.ViewUtil.IcebergViewHelper
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.expressions.UpCast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.MetadataBuilder

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  // Spark's own view schema binding confs, SQLConf.VIEW_SCHEMA_BINDING_ENABLED and
  // VIEW_SCHEMA_COMPENSATION. Referenced by name because they were added in Spark 4.0 and this
  // rule is also compiled against Spark 3.5. Both default to true.
  private val sparkViewSchemaBindingMode = "spark.sql.legacy.viewSchemaBindingMode"
  private val sparkViewSchemaCompensation = "spark.sql.legacy.viewSchemaCompensation"

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u @ UnresolvedRelation(nameParts, _, _)
        if catalogManager.v1SessionCatalog.isTempView(nameParts) =>
      u

    case u @ UnresolvedRelation(parts @ CatalogAndIdentifier(catalog, ident), _, _) =>
      ViewUtil
        .loadView(catalog, ident)
        .map(createViewRelation(parts, _))
        .getOrElse(u)

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

  private def createViewRelation(nameParts: Seq[String], view: View): LogicalPlan = {
    val parsed = parseViewText(nameParts.quoted, view.query)

    // Apply any necessary rewrites to preserve correct resolution
    val viewCatalogAndNamespace: Seq[String] = view.currentCatalog +: view.currentNamespace.toSeq
    val rewritten = rewriteIdentifiers(parsed, viewCatalogAndNamespace);

    // Apply the field aliases and column comments
    // This logic differs from how Spark handles views in SessionCatalog.fromCatalogTable.
    // BINDING is more strict because it doesn't allow resolution by field name. COMPENSATION and
    // TYPE_EVOLUTION coerce as SessionCatalog.castColToType does for those modes. Every mode keeps
    // the stored name and metadata; only the coercion differs.
    val mode = viewSchemaMode
    val aliases = view.schema.fields.zipWithIndex.map { case (expected, pos) =>
      val attr = GetColumnByOrdinal(pos, expected.dataType)
      val coerced =
        if (mode == SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION) {
          Cast(attr, expected.dataType, ansiEnabled = true)
        } else if (mode == SparkSQLProperties.VIEW_SCHEMA_MODE_TYPE_EVOLUTION) {
          attr
        } else {
          UpCast(attr, expected.dataType)
        }
      Alias(coerced, expected.name)(explicitMetadata = Some(expected.metadata))
    }.toIndexedSeq

    SubqueryAlias(nameParts, Project(aliases, rewritten))
  }

  /**
   * How a view's stored schema is applied to the columns its SQL produces.
   *
   * Read on every resolution rather than cached, so that SET takes effect within a session.
   */
  private def viewSchemaMode: String = {
    spark.conf.getOption(SparkSQLProperties.VIEW_SCHEMA_BINDING_MODE) match {
      case Some(mode) =>
        parseSchemaBindingMode(mode)
      case None =>
        // Mirror SessionCatalog.castColToType: turning binding mode off selects SchemaUnsupported,
        // which compensates with an ANSI cast unless compensation is turned off as well. Neither conf
        // can select TYPE_EVOLUTION: in Spark that mode is requested per view, with
        // CREATE or ALTER VIEW ... WITH SCHEMA TYPE EVOLUTION, and stored on the view itself.
        if (isExplicitlyFalse(sparkViewSchemaBindingMode) &&
          !isExplicitlyFalse(sparkViewSchemaCompensation)) {
          SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION
        } else {
          SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING
        }
    }
  }

  // Spark spells this mode "TYPE EVOLUTION" in its WITH SCHEMA clause, so accept a space as well as
  // an underscore. Preconditions.checkArgument is avoided: with this many message arguments the call
  // is an ambiguous overload under Scala 2.12, which spark/v3.5 is cross-built against.
  private def parseSchemaBindingMode(mode: String): String = {
    val normalized = mode.trim.replace(' ', '_')
    if (normalized.equalsIgnoreCase(SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING)) {
      SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING
    } else if (normalized.equalsIgnoreCase(SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION)) {
      SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION
    } else if (normalized.equalsIgnoreCase(SparkSQLProperties.VIEW_SCHEMA_MODE_TYPE_EVOLUTION)) {
      SparkSQLProperties.VIEW_SCHEMA_MODE_TYPE_EVOLUTION
    } else {
      throw new IllegalArgumentException(
        s"Invalid value for ${SparkSQLProperties.VIEW_SCHEMA_BINDING_MODE}: $mode, expected " +
          s"${SparkSQLProperties.VIEW_SCHEMA_MODE_BINDING}, " +
          s"${SparkSQLProperties.VIEW_SCHEMA_MODE_COMPENSATION} or " +
          s"${SparkSQLProperties.VIEW_SCHEMA_MODE_TYPE_EVOLUTION}")
    }
  }

  private def isExplicitlyFalse(key: String): Boolean =
    spark.conf.getOption(key).exists(_.trim.equalsIgnoreCase("false"))

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
      catalogAndNamespace: Seq[String]): LogicalPlan = {
    // Rewrite unresolved functions and relations
    qualifyTableIdentifiers(
      qualifyFunctionIdentifiers(CTESubstitution.apply(plan), catalogAndNamespace),
      catalogAndNamespace)
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
   * Qualify table identifiers with default catalog and namespace if necessary.
   */
  private def qualifyTableIdentifiers(
      child: LogicalPlan,
      catalogAndNamespace: Seq[String]): LogicalPlan =
    child transform {
      case u @ UnresolvedRelation(Seq(table), _, _) =>
        u.copy(multipartIdentifier = catalogAndNamespace :+ table)
      case u @ UnresolvedRelation(parts, _, _) if !isCatalog(parts.head) =>
        u.copy(multipartIdentifier = catalogAndNamespace.head +: parts)
      case other =>
        other.transformExpressions { case subquery: SubqueryExpression =>
          subquery.withNewPlan(qualifyTableIdentifiers(subquery.plan, catalogAndNamespace))
        }
    }

  private def isCatalog(name: String): Boolean = {
    catalogManager.isCatalogRegistered(name)
  }

  private def isBuiltinFunction(name: String): Boolean = {
    catalogManager.v1SessionCatalog.isBuiltinFunction(FunctionIdentifier(name))
  }
}
