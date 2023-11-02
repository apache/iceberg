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
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.AlterViewAs
import org.apache.spark.sql.catalyst.plans.logical.CreateView
import org.apache.spark.sql.catalyst.plans.logical.IcebergView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.CreateV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.errors.QueryCompilationErrors.toSQLId
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.util.SchemaUtils

/**
 * Resolve views in CREATE VIEW and ALTER VIEW AS plans and convert them to logical plans.
 */
case class CreateViewAnalysis(spark: SparkSession)
  extends Rule[LogicalPlan] with LookupCatalog {

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private lazy val isTempView =
    (nameParts: Seq[String]) => catalogManager.v1SessionCatalog.isTempView(nameParts)
  private lazy val isTemporaryFunction = catalogManager.v1SessionCatalog.isTemporaryFunction _

  def asViewCatalog(plugin: CatalogPlugin): ViewCatalog = plugin match {
    case viewCatalog: ViewCatalog =>
      viewCatalog
    case _ =>
      throw QueryCompilationErrors.missingCatalogAbilityError(plugin, "views")
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case CreateIcebergView(ResolvedIdentifier(catalog, nameParts), userSpecifiedColumns, comment,
    properties, originalText, child, allowExisting, replace) =>
      convertCreateView(
        catalog = asViewCatalog(catalog),
        viewInfo = ViewInfo(
          ident = nameParts,
          userSpecifiedColumns = userSpecifiedColumns,
          comment = comment,
          properties = properties,
          originalText = originalText),
        child = child,
        allowExisting = allowExisting,
        replace = replace)

    case AlterViewAs(ResolvedV2View(catalog, ident, _), originalText, query) =>
      convertCreateView(
        catalog = catalog,
        viewInfo = ViewInfo(
          ident = ident,
          userSpecifiedColumns = Seq.empty,
          comment = None,
          properties = Map.empty,
          originalText = Option(originalText)),
        child = query,
        allowExisting = false,
        replace = true)
  }

  private case class ViewInfo(ident: Identifier,
                      userSpecifiedColumns: Seq[(String, Option[String])],
                      comment: Option[String],
                      properties: Map[String, String],
                      originalText: Option[String])

  /**
   * Convert [[CreateView]] or [[AlterViewAs]] to logical plan [[CreateV2View]].
   */
  private def convertCreateView(
                                 catalog: ViewCatalog,
                                 viewInfo: ViewInfo,
                                 child: LogicalPlan,
                                 allowExisting: Boolean,
                                 replace: Boolean): LogicalPlan = {

    val qe = new QueryExecution(spark, child, mode = CommandExecutionMode.SKIP)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (viewInfo.userSpecifiedColumns.nonEmpty &&
      viewInfo.userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${viewInfo.userSpecifiedColumns.length}`).")
    }

    verifyTemporaryObjectsNotExists(viewInfo.ident, child)

    val queryOutput = analyzedPlan.schema.fieldNames
    // Generate the query column names,
    // throw an AnalysisException if there exists duplicate column names.
    SchemaUtils.checkColumnNameDuplication(queryOutput, SQLConf.get.resolver)

    viewInfo.userSpecifiedColumns.map(_._1).zip(queryOutput).foreach { case (n1, n2) =>
      if (n1 != n2) {
        throw new AnalysisException(s"Renaming columns is not supported: $n1 != $n2")
      }
    }

    if (replace) {
      // Detect cyclic view reference on CREATE OR REPLACE VIEW or ALTER VIEW AS.
      val parts = (catalog.name +: viewInfo.ident.asMultipartIdentifier).quoted
      checkCyclicViewReference(analyzedPlan, Seq(parts), parts)
    }

    val sql = viewInfo.originalText.getOrElse {
      throw QueryCompilationErrors.createPersistedViewFromDatasetAPINotAllowedError()
    }

    val viewSchema = aliasPlan(analyzedPlan, viewInfo.userSpecifiedColumns).schema
    val columnAliases = viewInfo.userSpecifiedColumns.map(_._1).toArray
    val columnComments = viewInfo.userSpecifiedColumns.map(_._2.getOrElse(null)).toArray

    CreateV2View(
      view = viewInfo.ident.asMultipartIdentifier,
      sql = sql,
      comment = None,
      viewSchema = viewSchema,
      queryOutput,
      columnAliases = columnAliases,
      columnComments = columnComments,
      properties = viewInfo.properties,
      allowExisting = allowExisting,
      replace = replace)
  }

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
   */
  private def aliasPlan(
                         analyzedPlan: LogicalPlan,
                         userSpecifiedColumns: Seq[(String, Option[String])]): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      new QueryExecution(spark, Project(projectList, analyzedPlan), mode = CommandExecutionMode.SKIP).analyzed
    }
  }

  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views
   */
  private def verifyTemporaryObjectsNotExists(
                                               name: Identifier,
                                               child: LogicalPlan): Unit = {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    // This func traverses the unresolved plan `child`. Below are the reasons:
    // 1) Analyzer replaces unresolved temporary views by a SubqueryAlias with the corresponding
    // logical plan. After replacement, it is impossible to detect whether the SubqueryAlias is
    // added/generated from a temporary view.
    // 2) The temp functions are represented by multiple classes. Most are inaccessible from this
    // package (e.g., HiveGenericUDF).
    child.collect {
      // Disallow creating permanent views based on temporary views.
      case UnresolvedRelation(nameParts, _, _) if isTempView(nameParts) =>
        throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
          s"referencing a temporary view ${nameParts.quoted}. " +
          "Please create a temp view instead by CREATE TEMP VIEW")
      case other if !other.resolved => other.expressions.flatMap(_.collect {
        // Disallow creating permanent views based on temporary UDFs.
        case UnresolvedFunction(Seq(funcName), _, _, _, _)
          if isTemporaryFunction(FunctionIdentifier(funcName)) =>
          throw new AnalysisException(s"Not allowed to create a permanent view $name by " +
            s"referencing a temporary function $funcName")
      })
    }
  }

  /**
   * Recursively search the logical plan to detect cyclic view references, throw an
   * AnalysisException if cycle detected.
   *
   * A cyclic view reference is a cycle of reference dependencies, for example, if the following
   * statements are executed:
   * CREATE VIEW testView AS SELECT id FROM tbl
   * CREATE VIEW testView2 AS SELECT id FROM testView
   * ALTER VIEW testView AS SELECT * FROM testView2
   * The view `testView` references `testView2`, and `testView2` also references `testView`,
   * therefore a reference cycle (testView -> testView2 -> testView) exists.
   *
   * @param plan      the logical plan we detect cyclic view references from.
   * @param path      the path between the altered view and current node.
   * @param viewIdent the table identifier of the altered view, we compare two views by the
   *                  `desc.identifier`.
   */
  private def checkCyclicViewReference(
                                plan: LogicalPlan,
                                path: Seq[String],
                                viewIdent: String): Unit = {
    plan match {
      case v: IcebergView =>
        val ident = v.desc.identifier
        val newPath = path :+ ident
        // If the table identifier equals to the `viewIdent`, current view node is the same with
        // the altered view. We detect a view reference cycle, should throw an AnalysisException.
        if (ident == viewIdent) {
          throw recursiveViewDetectedError(viewIdent, newPath)
        } else {
          v.children.foreach { child =>
            checkCyclicViewReference(child, newPath, viewIdent)
          }
        }
      case _ =>
        plan.children.foreach(child => checkCyclicViewReference(child, path, viewIdent))
    }

    // Detect cyclic references from subqueries.
    plan.expressions.foreach { expr =>
      expr match {
        case s: SubqueryExpression =>
          checkCyclicViewReference(s.plan, path, viewIdent)
        case _ => // Do nothing.
      }
    }
  }

  private def recursiveViewDetectedError(
                                  viewIdent: String,
                                  newPath: Seq[String]): Throwable = {
    new AnalysisException(
      errorClass = "RECURSIVE_VIEW",
      messageParameters = Map(
        "viewIdent" -> toSQLId(viewIdent),
        "newPath" -> newPath.map(p => toSQLId(p)).mkString(" -> ")))
  }
}
