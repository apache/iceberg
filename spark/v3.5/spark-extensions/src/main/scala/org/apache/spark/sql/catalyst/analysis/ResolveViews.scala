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
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{View => V2View}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.LookupCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors

case class ResolveViews(spark: SparkSession) extends Rule[LogicalPlan] with LookupCatalog {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  protected lazy val catalogManager: CatalogManager = spark.sessionState.catalogManager

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u@UnresolvedRelation(nameParts, _, _)
      if catalogManager.v1SessionCatalog.isTempView(Seq(nameParts.asIdentifier.name())) =>
      u

    case u@UnresolvedRelation(parts@CatalogAndIdentifier(catalog, ident), _, _) =>
      loadView(catalog, ident)
        .map(createViewRelation(parts.quoted, _))
        .getOrElse(u)
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

  private def createViewRelation(name: String, view: V2View): LogicalPlan = {
    val child = parseViewText(name, view.query)

    val viewCatalogAndNamespace: Seq[String] = view.currentCatalog +: view.currentNamespace.toSeq
    // Substitute CTEs within the view before qualifying table identifiers
    SubqueryAlias(name, qualifyTableIdentifiers(CTESubstitution.apply(child), viewCatalogAndNamespace))
  }

  private def parseViewText(name: String, viewText: String): LogicalPlan = {
    try {
      SparkSession.active.sessionState.sqlParser.parsePlan(viewText)
    } catch {
      case _: ParseException =>
        throw QueryCompilationErrors.invalidViewText(viewText, name)
    }
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
}
