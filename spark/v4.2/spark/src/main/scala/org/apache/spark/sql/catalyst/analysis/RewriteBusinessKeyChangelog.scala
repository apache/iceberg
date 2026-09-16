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

import org.apache.iceberg.spark.ChangelogIterator
import org.apache.iceberg.spark.source.SparkChangelogTable
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.OrderUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.classic.{SparkSession => ClassicSparkSession}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connector.catalog.ChangelogContext
import org.apache.spark.sql.connector.catalog.ChangelogContext.DeduplicationMode
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.ChangelogTable
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.lower
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.functions.when
import scala.jdk.CollectionConverters._

/** Applies the changelog view's value/key semantics to an explicitly requested CDC batch read. */
case class RewriteBusinessKeyChangelog(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUp {
    case rel: DataSourceV2Relation if businessKeyTable(rel.table).isDefined =>
      rewrite(rel, rel.table.asInstanceOf[ChangelogTable])

    case rel: StreamingRelationV2 if businessKeyTable(rel.table).isDefined =>
      throw new UnsupportedOperationException(
        "Business-key CDC currently supports batch reads only")
  }

  private def businessKeyTable(table: Table): Option[SparkChangelogTable] = table match {
    case wrapper: ChangelogTable =>
      wrapper.changelog match {
        case iceberg: SparkChangelogTable if iceberg.identifierColumns().nonEmpty => Some(iceberg)
        case _ => None
      }
    case _ => None
  }

  private def rewrite(rel: DataSourceV2Relation, wrapper: ChangelogTable): LogicalPlan = {
    val iceberg = businessKeyTable(wrapper).get
    val rawContext =
      new ChangelogContext(wrapper.changelogContext.range(), DeduplicationMode.NONE, false)
    val raw = rel.copy(table = wrapper.copy(
      changelog = iceberg.rawChangelog(),
      changelogContext = rawContext,
      resolved = true))
    val input = Dataset
      .ofRows(spark.asInstanceOf[ClassicSparkSession], raw)
      .drop("_row_id", "_last_updated_sequence_number")
      .withColumn("_change_type", upper(col("_change_type")))
    val schema = input.schema
    val computeUpdates = wrapper.changelogContext.computeUpdates()
    val identifiers = iceberg.identifierColumns() :+ "_commit_version"
    val groupNames =
      if (computeUpdates) identifiers else schema.fieldNames.filterNot(_ == "_change_type")
    require(
      groupNames.forall(name => OrderUtils.isOrderable(schema(name).dataType)),
      "Business-key CDC grouping columns must be orderable")
    val groupColumns = groupNames.map(name => col("`" + name.replace("`", "``") + "`"))
    val sorted = input
      .repartition(groupColumns.toIndexedSeq: _*)
      .sortWithinPartitions((groupColumns :+ col("_change_type")).toIndexedSeq: _*)
    val processed = sorted
      .mapPartitions { rows: Iterator[Row] =>
        val result = if (computeUpdates) {
          ChangelogIterator.computeUpdates(rows.asJava, schema, identifiers)
        } else {
          ChangelogIterator.removeCarryovers(rows.asJava, schema)
        }
        result.asScala
      }(Encoders.row(schema))
      .withColumn(
        "_change_type",
        when(col("_change_type") === "UPDATE_BEFORE", "update_preimage")
          .when(col("_change_type") === "UPDATE_AFTER", "update_postimage")
          .otherwise(lower(col("_change_type"))))
      .withColumn("_row_id", lit(null).cast("long"))
      .withColumn("_last_updated_sequence_number", lit(null).cast("long"))
      .queryExecution
      .analyzed
    val output = rel.output.map { expected =>
      val actual = processed.output.find(_.name == expected.name).get
      Alias(actual, expected.name)(
        exprId = expected.exprId,
        explicitMetadata = Some(expected.metadata))
    }
    Project(output, processed)
  }
}
