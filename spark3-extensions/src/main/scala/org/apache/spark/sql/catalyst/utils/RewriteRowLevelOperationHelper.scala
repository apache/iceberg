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
package org.apache.spark.sql.catalyst.utils

import java.util.UUID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.PushDownUtils
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait RewriteRowLevelOperationHelper extends PredicateHelper with Logging {
  protected val FILE_NAME_COL = "_file"
  protected val ROW_POS_COL = "_pos"

  def resolver: Resolver

  protected def buildScanPlan(
      table: Table,
      tableAttrs: Seq[AttributeReference],
      mergeBuilder: MergeBuilder,
      cond: Expression,
      matchingRowsPlanBuilder: DataSourceV2ScanRelation => LogicalPlan): LogicalPlan = {

    val scanBuilder = mergeBuilder.asScanBuilder

    pushFilters(scanBuilder, cond, tableAttrs)

    val scan = scanBuilder.build()
    val outputAttrs = toOutputAttrs(scan.readSchema(), tableAttrs)
    val scanRelation = DataSourceV2ScanRelation(table, scan, outputAttrs)

    scan match {
      case filterable: SupportsFileFilter =>
        val matchingFilePlan = buildFileFilterPlan(matchingRowsPlanBuilder(scanRelation))
        DynamicFileFilter(scanRelation, matchingFilePlan, filterable)
      case _ =>
        scanRelation
    }
  }

  private def pushFilters(
      scanBuilder: ScanBuilder,
      cond: Expression,
      tableAttrs: Seq[AttributeReference]): Unit = {

    val tableAttrSet = AttributeSet(tableAttrs)
    val predicates = splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
    if (predicates.nonEmpty) {
      val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, tableAttrs)
      PushDownUtils.pushFilters(scanBuilder, normalizedPredicates)
    }
  }

  protected def toDataSourceFilters(predicates: Seq[Expression]): Array[sources.Filter] = {
    predicates.flatMap { p =>
      val translatedFilter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (translatedFilter.isEmpty) {
        logWarning(s"Cannot translate expression to source filter: $p")
      }
      translatedFilter
    }.toArray
  }

  protected def newWriteInfo(schema: StructType): LogicalWriteInfo = {
    val uuid = UUID.randomUUID()
    LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
  }

  private def buildFileFilterPlan(matchingRowsPlan: LogicalPlan): LogicalPlan = {
    val fileAttr = findOutputAttr(matchingRowsPlan, FILE_NAME_COL)
    val agg = Aggregate(Seq(fileAttr), Seq(fileAttr), matchingRowsPlan)
    Project(Seq(findOutputAttr(agg, FILE_NAME_COL)), agg)
  }

  protected def findOutputAttr(plan: LogicalPlan, attrName: String): Attribute = {
    plan.output.find(attr => resolver(attr.name, attrName)).getOrElse {
      throw new AnalysisException(s"Cannot find $attrName in ${plan.output}")
    }
  }

  protected def toOutputAttrs(schema: StructType, attrs: Seq[AttributeReference]): Seq[AttributeReference] = {
    val nameToAttr = attrs.map(_.name).zip(attrs).toMap
    schema.toAttributes.map {
      a => nameToAttr.get(a.name) match {
        case Some(ref) =>
          // keep the attribute id if it was present in the relation
          a.withExprId(ref.exprId)
        case _ =>
          // if the field is new, create a new attribute
          AttributeReference(a.name, a.dataType, a.nullable, a.metadata)()
      }
    }
  }
}
