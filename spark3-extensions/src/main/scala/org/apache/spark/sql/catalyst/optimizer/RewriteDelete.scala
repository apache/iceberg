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

package org.apache.spark.sql.catalyst.optimizer

import java.util.UUID
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{sources, AnalysisException}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, EqualNullSafe, Expression, InputFileName, Literal, Not, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DeleteFromTable, DynamicFileFilter, Filter, LogicalPlan, Project, ReplaceData}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{ExtendedSupportsDelete, Table}
import org.apache.spark.sql.connector.read.SupportsFileFilter
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, LogicalWriteInfoImpl, MergeBuilder}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation, PushDownUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// TODO: should be part of early scan push down after the delete condition is optimized
object RewriteDelete extends Rule[LogicalPlan] with PredicateHelper with Logging {

  import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits._

  private val FILE_NAME_COL = "_file"

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    // don't rewrite deletes that can be answered by passing filters to deleteWhere in SupportsDelete
    case d @ DeleteFromTable(r: DataSourceV2Relation, Some(cond)) if isDeleteWhereCase(r, cond) =>
      d

    // rewrite all operations that require reading the table to delete records
    case DeleteFromTable(r: DataSourceV2Relation, Some(cond)) =>
      // TODO: do a switch based on whether we get BatchWrite or DeltaBatchWrite
      val writeInfo = newWriteInfo(r.schema)
      val mergeBuilder = r.table.asMergeable.newMergeBuilder(writeInfo)

      val scanPlan = buildScanPlan(r.table, r.output, mergeBuilder, cond)

      val remainingRowFilter = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
      val remainingRowsPlan = Filter(remainingRowFilter, scanPlan)

      val batchWrite = mergeBuilder.asWriteBuilder.buildForBatch()

      // TODO: group by something so that we can write back
      // Option 1: repartition/sort by partition values
      // Option 2: repartition by _file and local sort by partition values (how to deal with 1 GB files?)
      ReplaceData(r, batchWrite, remainingRowsPlan)
  }

  private def buildScanPlan(
      table: Table,
      output: Seq[AttributeReference],
      mergeBuilder: MergeBuilder,
      cond: Expression): LogicalPlan = {

    val scanBuilder = mergeBuilder.asScanBuilder

    val predicates = splitConjunctivePredicates(cond)
    val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, output)
    PushDownUtils.pushFilters(scanBuilder, normalizedPredicates)

    val scan = scanBuilder.build()
    val scanRelation = DataSourceV2ScanRelation(table, scan, output)

    scan match {
      case _: SupportsFileFilter =>
        val matchingFilePlan = buildFileFilterPlan(cond, scanRelation)
        val dynamicFileFilter = DynamicFileFilter(scanRelation, matchingFilePlan)
        dynamicFileFilter
      case _ =>
        scanRelation
    }
  }

  private def isDeleteWhereCase(relation: DataSourceV2Relation, cond: Expression): Boolean = {
    relation.table match {
      case t: ExtendedSupportsDelete if !SubqueryExpression.hasSubquery(cond) =>
        val predicates = splitConjunctivePredicates(cond)
        val normalizedPredicates = DataSourceStrategy.normalizeExprs(predicates, relation.output)
        val dataSourceFilters = toDataSourceFilters(normalizedPredicates)
        val allPredicatesTranslated = normalizedPredicates.size == dataSourceFilters.length
        allPredicatesTranslated && t.canDeleteWhere(dataSourceFilters)
      case _ => false
    }
  }

  private def toDataSourceFilters(predicates: Seq[Expression]): Array[sources.Filter] = {
    predicates.flatMap { p =>
      val translatedFilter = DataSourceStrategy.translateFilter(p, supportNestedPredicatePushdown = true)
      if (translatedFilter.isEmpty) {
        logWarning(s"Cannot translate expression to source filter: $p")
      }
      translatedFilter
    }.toArray
  }

  private def newWriteInfo(schema: StructType): LogicalWriteInfo = {
    val uuid = UUID.randomUUID()
    LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
  }

  private def buildFileFilterPlan(cond: Expression, scanRelation: DataSourceV2ScanRelation): LogicalPlan = {
    val fileNameExpr = Alias(InputFileName(), FILE_NAME_COL)()
    val fileNameProjection = Project(scanRelation.output :+ fileNameExpr, scanRelation)
    val matchingFilter = Filter(cond, fileNameProjection)
    val fileAttr = findOutputAttr(matchingFilter, FILE_NAME_COL)
    Aggregate(Seq(fileAttr), Seq(fileAttr), matchingFilter)
  }

  private def findOutputAttr(plan: LogicalPlan, attrName: String): Attribute = {
    val resolver = SQLConf.get.resolver
    plan.output.find(attr => resolver(attr.name, attrName)).getOrElse {
      throw new AnalysisException(s"Cannot find $attrName in ${plan.output}")
    }
  }
}
