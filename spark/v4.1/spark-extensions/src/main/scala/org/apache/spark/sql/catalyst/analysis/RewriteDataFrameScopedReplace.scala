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

import org.apache.iceberg.relocated.com.google.common.base.Splitter
import org.apache.iceberg.spark.SparkTableUtil
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.AppendData
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.OverwriteByExpression
import org.apache.spark.sql.catalyst.plans.logical.OverwritePartitionsDynamic
import org.apache.spark.sql.catalyst.plans.logical.ReplaceScopedData
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import scala.jdk.CollectionConverters._

/**
 * Rewrites the DataFrameWriterV2 scoped-replace API into the shared logical command.
 *
 * The API is carried by a write option on `overwrite(true)`, so Spark resolves the query using the
 * standard DataFrameWriterV2 by-name write path before this rule runs. Normal Iceberg write options
 * are moved onto the relation because Spark's row-level write planning reads options from the
 * relation after [[ReplaceScopedData]] is lowered to [[org.apache.spark.sql.catalyst.plans.logical.ReplaceData]]
 * or [[org.apache.spark.sql.catalyst.plans.logical.WriteDelta]].
 */
case class RewriteDataFrameScopedReplace(spark: SparkSession) extends Rule[LogicalPlan] {

  import RewriteDataFrameScopedReplace._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case overwrite @ OverwriteByExpression(
          r @ DataSourceV2Relation(table: SparkTable, _, _, _, relationOptions, _),
          deleteExpr,
          query,
          writeOptions,
          _,
          None,
          _) if overwrite.resolved && replaceUsing(writeOptions).isDefined =>

      if (!deleteExpr.semanticEquals(TrueLiteral)) {
        throw analysisError(
          s"Scoped replace must be invoked with overwrite(true), found: ${deleteExpr.sql}")
      }

      if (query.isStreaming) {
        throw analysisError("Scoped replace is not supported for streaming DataFrames")
      }

      val mergedOptions = writeOptionsWithoutReplaceUsing(writeOptions, relationOptions)
      val targetBranch = SparkTableUtil.determineWriteBranch(spark, table, mergedOptions)
      val targetTable =
        if (table.branch() == targetBranch) table else table.copyWithBranch(targetBranch)
      val scopeColumns = parseScopeColumns(replaceUsing(writeOptions).get)

      ReplaceScopedData(r.copy(table = targetTable, options = mergedOptions), scopeColumns, query)

    case append @ AppendData(
          DataSourceV2Relation(_: SparkTable, _, _, _, _, _),
          _,
          writeOptions,
          _,
          _,
          _) if append.resolved && replaceUsing(writeOptions).isDefined =>
      throw analysisError("Scoped replace must be invoked with overwrite(true), not append()")

    case overwritePartitions @ OverwritePartitionsDynamic(
          DataSourceV2Relation(_: SparkTable, _, _, _, _, _),
          _,
          writeOptions,
          _,
          _) if overwritePartitions.resolved && replaceUsing(writeOptions).isDefined =>
      throw analysisError(
        "Scoped replace must be invoked with overwrite(true), not overwritePartitions()")
  }

  private def writeOptionsWithoutReplaceUsing(
      writeOptions: Map[String, String],
      relationOptions: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    val relationMap = relationOptions.asCaseSensitiveMap.asScala.toMap
    val merged = removeReplaceUsing(relationMap) ++ removeReplaceUsing(writeOptions)
    new CaseInsensitiveStringMap(merged.asJava)
  }

  private def removeReplaceUsing(options: Map[String, String]): Map[String, String] = {
    options.filterNot { case (key, _) => key.equalsIgnoreCase(ReplaceUsingOption) }
  }

  private def replaceUsing(options: Map[String, String]): Option[String] = {
    Option(new CaseInsensitiveStringMap(options.asJava).get(ReplaceUsingOption))
  }

  // `replace-using` follows the standard Iceberg comma-separated option convention, so the list is
  // split on commas and each token is parsed as a multi-part identifier by the session parser (the
  // same parser the SQL `REPLACE USING` path delegates to). A consequence of splitting first is that
  // a column whose quoted name itself contains a comma cannot be expressed through this option; such
  // names are not representable in a comma-delimited option and must use the SQL `REPLACE USING` form.
  private def parseScopeColumns(columns: String): Seq[Seq[String]] = {
    Splitter
      .on(',')
      .trimResults()
      .splitToList(columns)
      .asScala
      .map { column =>
        spark.sessionState.sqlParser.parseMultipartIdentifier(column)
      }
      .toSeq
  }

  private def analysisError(message: String): AnalysisException = {
    new AnalysisException(
      errorClass = "_LEGACY_ERROR_TEMP_ICEBERG_SCOPED_REPLACE",
      sqlState = null,
      messageTemplate = message,
      messageParameters = Map.empty[String, String],
      cause = None,
      message = Some(message))
  }
}

object RewriteDataFrameScopedReplace {
  private val ReplaceUsingOption = "replace-using"
}
