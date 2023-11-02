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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.SparkSessionCatalog
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.analysis.ResolvedNamespace
import org.apache.spark.sql.catalyst.analysis.V2ViewDescription
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
import org.apache.spark.sql.catalyst.plans.logical.Call
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceBranch
import org.apache.spark.sql.catalyst.plans.logical.CreateOrReplaceTag
import org.apache.spark.sql.catalyst.plans.logical.DropBranch
import org.apache.spark.sql.catalyst.plans.logical.DropIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField
import org.apache.spark.sql.catalyst.plans.logical.DropTag
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OrderAwareCoalesce
import org.apache.spark.sql.catalyst.plans.logical.ReplacePartitionField
import org.apache.spark.sql.catalyst.plans.logical.SetIdentifierFields
import org.apache.spark.sql.catalyst.plans.logical.SetWriteDistributionAndOrdering
import org.apache.spark.sql.catalyst.plans.logical.ShowCreateTable
import org.apache.spark.sql.catalyst.plans.logical.ShowViews
import org.apache.spark.sql.catalyst.plans.logical.views.AlterV2View
import org.apache.spark.sql.catalyst.plans.logical.views.CreateV2View
import org.apache.spark.sql.catalyst.plans.logical.views.DescribeV2View
import org.apache.spark.sql.catalyst.plans.logical.views.DropIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.RenameV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowCreateV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowIcebergViews
import org.apache.spark.sql.catalyst.plans.logical.views.ShowV2ViewProperties
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.OrderAwareCoalesceExec
import org.apache.spark.sql.execution.SparkPlan
import scala.jdk.CollectionConverters._

case class ExtendedDataSourceV2Strategy(spark: SparkSession) extends Strategy with PredicateHelper {
  val catalogManager: CatalogManager = spark.sessionState.catalogManager

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c @ Call(procedure, args) =>
      val input = buildInternalRow(args)
      CallExec(c.output, procedure, input) :: Nil

    case AddPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform, name) =>
      AddPartitionFieldExec(catalog, ident, transform, name) :: Nil

    case CreateOrReplaceBranch(
        IcebergCatalogAndIdentifier(catalog, ident), branch, branchOptions, create, replace, ifNotExists) =>
      CreateOrReplaceBranchExec(catalog, ident, branch, branchOptions, create, replace, ifNotExists) :: Nil

    case CreateOrReplaceTag(
    IcebergCatalogAndIdentifier(catalog, ident), tag, tagOptions, create, replace, ifNotExists) =>
      CreateOrReplaceTagExec(catalog, ident, tag, tagOptions, create, replace, ifNotExists) :: Nil

    case DropBranch(IcebergCatalogAndIdentifier(catalog, ident), branch, ifExists) =>
      DropBranchExec(catalog, ident, branch, ifExists) :: Nil

    case DropTag(IcebergCatalogAndIdentifier(catalog, ident), tag, ifExists) =>
      DropTagExec(catalog, ident, tag, ifExists) :: Nil

    case DropPartitionField(IcebergCatalogAndIdentifier(catalog, ident), transform) =>
      DropPartitionFieldExec(catalog, ident, transform) :: Nil

    case ReplacePartitionField(IcebergCatalogAndIdentifier(catalog, ident), transformFrom, transformTo, name) =>
      ReplacePartitionFieldExec(catalog, ident, transformFrom, transformTo, name) :: Nil

    case SetIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      SetIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case DropIdentifierFields(IcebergCatalogAndIdentifier(catalog, ident), fields) =>
      DropIdentifierFieldsExec(catalog, ident, fields) :: Nil

    case SetWriteDistributionAndOrdering(
        IcebergCatalogAndIdentifier(catalog, ident), distributionMode, ordering) =>
      SetWriteDistributionAndOrderingExec(catalog, ident, distributionMode, ordering) :: Nil

    case OrderAwareCoalesce(numPartitions, coalescer, child) =>
      OrderAwareCoalesceExec(numPartitions, coalescer, planLater(child)) :: Nil

    case CreateV2View(
      IcebergViewCatalogAndIdentifier(catalog, ident), sql, comment, viewSchema, queryColumnNames,
    columnAliases, columnComments, properties, allowExisting, replace) =>
      CreateV2ViewExec(catalog, ident, sql, catalogManager.currentCatalog.name,
        catalogManager.currentNamespace, comment, viewSchema, queryColumnNames,
        columnAliases, columnComments, properties, allowExisting, replace) :: Nil

    case d@DescribeV2View(desc, isExtended) =>
      DescribeV2ViewExec(d.output, desc, isExtended) :: Nil

    case show@ShowCreateV2View(view) =>
      ShowCreateV2ViewExec(show.output, view) :: Nil

    case show@ShowCreateTable(ResolvedV2View(_, ident, view), _, _) =>
      ShowCreateV2ViewExec(show.output, V2ViewDescription(ident.quoted, view)) :: Nil

    case show@ShowV2ViewProperties(view, propertyKey) =>
      ShowV2ViewPropertiesExec(show.output, view, propertyKey) :: Nil

    case ShowIcebergViews(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowV2ViewsExec(output, asViewCatalog(catalog), ns, pattern) :: Nil

    case ShowViews(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowV2ViewsExec(output, asViewCatalog(catalog), ns, pattern) :: Nil

    case DropIcebergView(ResolvedIdentifier(catalog, ident), ifExists) =>
      DropV2ViewExec(asViewCatalog(catalog), ident, ifExists) :: Nil

    case RenameV2View(catalog, oldIdent, newIdent) =>
      RenameV2ViewExec(catalog, oldIdent, newIdent) :: Nil

    case AlterV2View(catalog, ident, changes) =>
      AlterV2ViewExec(catalog, ident, changes) :: Nil

    case _ => Nil
  }

  private def asViewCatalog(plugin: CatalogPlugin): ViewCatalog = plugin match {
    case viewCatalog: ViewCatalog =>
      viewCatalog
    case _ =>
      throw QueryCompilationErrors.missingCatalogAbilityError(plugin, "views")
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }

  private object IcebergCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(TableCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: SparkCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case _ =>
          None
      }
    }
  }

  private object IcebergViewCatalogAndIdentifier {
    def unapply(identifier: Seq[String]): Option[(ViewCatalog, Identifier)] = {
      val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(spark, identifier.asJava)
      catalogAndIdentifier.catalog match {
        case icebergCatalog: SparkCatalog =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case icebergCatalog: SparkSessionCatalog[_] =>
          Some((icebergCatalog, catalogAndIdentifier.identifier))
        case _ =>
          None
      }
    }
  }
}
