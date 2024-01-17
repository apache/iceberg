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

package org.apache.spark.sql.execution.datasources

import org.apache.iceberg.spark.SparkV2Filters
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Strategy

object SparkExpressionConverter {

  def convertToIcebergExpression(sparkExpression: Expression): org.apache.iceberg.expressions.Expression = {
    // Currently, it is a double conversion as we are converting Spark expression to Spark predicate
    // and then converting Spark predicate to Iceberg expression.
    // But these two conversions already exist and well tested. So, we are going with this approach.
    DataSourceV2Strategy.translateFilterV2(sparkExpression) match {
      case Some(filter) =>
        val convertedExprTuple = SparkV2Filters.convert(filter)
        if (convertedExprTuple == null) {
          throw new IllegalArgumentException(s"Cannot convert Spark filter: $filter to Iceberg expression")
        }

        convertedExprTuple.getElement2
      case _ =>
        throw new IllegalArgumentException(s"Cannot translate Spark expression: $sparkExpression to data source filter")
    }
  }

  @throws[AnalysisException]
  def collectResolvedSparkExpression(session: SparkSession, tableName: String, where: String): Expression = {
    val tableAttrs = session.table(tableName).queryExecution.analyzed.output
    val unresolvedExpression = session.sessionState.sqlParser.parseExpression(where)
    val filter = Filter(unresolvedExpression, DummyRelation(tableAttrs))
    val optimizedLogicalPlan = session.sessionState.executePlan(filter).optimizedPlan
    optimizedLogicalPlan.collectFirst {
      case filter: Filter => filter.condition
      case dummyRelation: DummyRelation => Literal.TrueLiteral
      case localRelation: LocalRelation => Literal.FalseLiteral
    }.getOrElse(throw new AnalysisException("Failed to find filter expression"))
  }

  case class DummyRelation(output: Seq[Attribute]) extends LeafNode
}
