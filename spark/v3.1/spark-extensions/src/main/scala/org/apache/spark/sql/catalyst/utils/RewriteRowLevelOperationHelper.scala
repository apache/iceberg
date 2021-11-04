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
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.AccumulateFiles
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GreaterThan
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.aggregate.Complete
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilter
import org.apache.spark.sql.catalyst.plans.logical.DynamicFileFilterWithCardinalityCheck
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.logical.Repartition
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.iceberg.distributions.OrderedDistribution
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.connector.write.LogicalWriteInfoImpl
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Implicits
import org.apache.spark.sql.execution.datasources.v2.PushDownUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait RewriteRowLevelOperationHelper extends PredicateHelper with Logging {

  import DataSourceV2Implicits._
  import RewriteRowLevelOperationHelper._
  import ExtendedDataSourceV2Implicits.ScanBuilderHelper

  protected def spark: SparkSession
  def conf: SQLConf
  protected lazy val resolver: Resolver = conf.resolver

  protected def buildSimpleScanPlan(
      relation: DataSourceV2Relation,
      cond: Expression): LogicalPlan = {

    val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)

    pushFilters(scanBuilder, cond, relation.output)

    val scan = scanBuilder.asIceberg.withMetadataColumns(FILE_NAME_COL, ROW_POS_COL).build()
    val outputAttrs = toOutputAttrs(scan.readSchema(), relation.output)
    val predicates = extractFilters(cond, relation.output).reduceLeftOption(And)
    val scanRelation = DataSourceV2ScanRelation(relation, scan, outputAttrs)

    predicates.map(Filter(_, scanRelation)).getOrElse(scanRelation)
  }

  protected def buildDynamicFilterScanPlan(
      spark: SparkSession,
      relation: DataSourceV2Relation,
      tableAttrs: Seq[AttributeReference],
      mergeBuilder: MergeBuilder,
      cond: Expression,
      matchingRowsPlanBuilder: DataSourceV2ScanRelation => LogicalPlan,
      runCardinalityCheck: Boolean = false): LogicalPlan = {

    val scanBuilder = mergeBuilder.asScanBuilder

    pushFilters(scanBuilder, cond, tableAttrs)

    val scan = scanBuilder.asIceberg.withMetadataColumns(FILE_NAME_COL, ROW_POS_COL).build()
    val outputAttrs = toOutputAttrs(scan.readSchema(), tableAttrs)
    val scanRelation = DataSourceV2ScanRelation(relation, scan, outputAttrs)

    scan match {
      case filterable: SupportsFileFilter if runCardinalityCheck =>
        val affectedFilesAcc = new SetAccumulator[String]()
        spark.sparkContext.register(affectedFilesAcc, AFFECTED_FILES_ACC_NAME)

        val matchingRowsPlan = matchingRowsPlanBuilder(scanRelation)
        val matchingFilesPlan = buildFileFilterPlan(affectedFilesAcc, matchingRowsPlan)

        DynamicFileFilterWithCardinalityCheck(
          scanRelation,
          matchingFilesPlan,
          filterable,
          affectedFilesAcc)

      case filterable: SupportsFileFilter =>
        val matchingRowsPlan = matchingRowsPlanBuilder(scanRelation)
        val matchingFilesPlan = buildFileFilterPlan(scanRelation.output, matchingRowsPlan)
        DynamicFileFilter(scanRelation, matchingFilesPlan, filterable)

      case _ =>
        scanRelation
    }
  }

  private def extractFilters(cond: Expression, tableAttrs: Seq[AttributeReference]): Seq[Expression] = {
    val tableAttrSet = AttributeSet(tableAttrs)
    splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
  }

  private def pushFilters(
      scanBuilder: ScanBuilder,
      cond: Expression,
      tableAttrs: Seq[AttributeReference]): Unit = {
    val predicates = extractFilters(cond, tableAttrs)
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

  private def buildFileFilterPlan(tableAttrs: Seq[AttributeReference], matchingRowsPlan: LogicalPlan): LogicalPlan = {
    val fileAttr = findOutputAttr(tableAttrs, FILE_NAME_COL)
    val agg = Aggregate(Seq(fileAttr), Seq(fileAttr), matchingRowsPlan)
    Project(Seq(findOutputAttr(agg.output, FILE_NAME_COL)), agg)
  }

  private def buildFileFilterPlan(
      filesAccumulator: SetAccumulator[String],
      prunedTargetPlan: LogicalPlan): LogicalPlan = {
    val fileAttr = findOutputAttr(prunedTargetPlan.output, FILE_NAME_COL)
    val rowPosAttr = findOutputAttr(prunedTargetPlan.output, ROW_POS_COL)
    val accumulatorExpr = Alias(AccumulateFiles(filesAccumulator, fileAttr), AFFECTED_FILES_ACC_ALIAS_NAME)()
    val projectList = Seq(fileAttr, rowPosAttr, accumulatorExpr)
    val projectPlan = Project(projectList, prunedTargetPlan)
    val affectedFilesAttr = findOutputAttr(projectPlan.output, AFFECTED_FILES_ACC_ALIAS_NAME)
    val aggSumCol = Alias(AggregateExpression(Sum(affectedFilesAttr), Complete, false), SUM_ROW_ID_ALIAS_NAME)()
    // Group by the rows by row id while collecting the files that need to be over written via accumulator.
    val aggPlan = Aggregate(Seq(fileAttr, rowPosAttr), Seq(aggSumCol), projectPlan)
    val sumAttr = findOutputAttr(aggPlan.output, SUM_ROW_ID_ALIAS_NAME)
    val havingExpr = GreaterThan(sumAttr, Literal(1L))
    // Identifies ambiguous row in the target.
    Filter(havingExpr, aggPlan)
  }

  protected def findOutputAttr(attrs: Seq[Attribute], attrName: String): Attribute = {
    attrs.find(attr => resolver(attr.name, attrName)).getOrElse {
      throw new AnalysisException(s"Cannot find $attrName in $attrs")
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

  protected def buildWritePlan(childPlan: LogicalPlan, table: Table): LogicalPlan = {
    val icebergTable = Spark3Util.toIcebergTable(table)
    val distribution = Spark3Util.buildRequiredDistribution(icebergTable)
    val ordering = Spark3Util.buildRequiredOrdering(distribution, icebergTable)
    // range partitioning in Spark triggers a skew estimation job prior to shuffling
    // we insert a round-robin partitioning to avoid executing the merge join twice
    val newChildPlan = distribution match {
      case _: OrderedDistribution =>
        val numShufflePartitions = conf.numShufflePartitions
        Repartition(numShufflePartitions, shuffle = true, childPlan)
      case _ =>
        childPlan
    }
    DistributionAndOrderingUtils.prepareQuery(distribution, ordering, newChildPlan, conf)
  }
}

object RewriteRowLevelOperationHelper {
  final val FILE_NAME_COL = "_file"
  final val ROW_POS_COL = "_pos"

  // `internal.metrics` prefix ensures the accumulator state is not tracked by Spark UI
  private final val AFFECTED_FILES_ACC_NAME = "internal.metrics.merge.affectedFiles"
  private final val AFFECTED_FILES_ACC_ALIAS_NAME = "_affectedFiles_"
  private final val SUM_ROW_ID_ALIAS_NAME = "_sum_"
}
