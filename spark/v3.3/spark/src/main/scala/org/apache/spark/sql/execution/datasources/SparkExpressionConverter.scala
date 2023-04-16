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

import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.spark.SparkFilters
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LeafNode

object SparkExpressionConverter {

  def convertToIcebergExpression(sparkExpression: Expression): org.apache.iceberg.expressions.Expression = {
    // Currently, it is a double conversion as we are converting Spark expression to Spark filter
    // and then converting Spark filter to Iceberg expression.
    // But these two conversions already exist and well tested. So, we are going with this approach.
    SparkFilters.convert(DataSourceStrategy.translateFilter(sparkExpression, supportNestedPredicatePushdown = true).get)
  }

  @throws[AnalysisException]
  def collectSparkExpressionOption(session: SparkSession,
                                        tableName: String, where: String): Option[Expression] = {
    // used to obtain an optimized logical plan of a spark expression
    val tableAttrs = session.table(tableName).queryExecution.analyzed.output
    val unresolvedExpression = session.sessionState.sqlParser.parseExpression(where)
    val filter = Filter(unresolvedExpression, DummyRelation(tableAttrs))
    val optimizedLogicalPlan = session.sessionState.executePlan(filter).optimizedPlan
    optimizedLogicalPlan.collectFirst {
      case filter: Filter => Some(filter.condition)
    }.flatten
  }

  @throws[AnalysisException]
  def collectResolvedIcebergExpression(session: SparkSession,
                                       tableName: String,
                                       where: String): org.apache.iceberg.expressions.Expression = {
    val option = collectSparkExpressionOption(session, tableName, where)
    option match {
      case None  =>
        val tableAttrs = session.table(tableName).queryExecution.analyzed.output
        val firstColumnName = tableAttrs.head.name
        val testWhere = s"$firstColumnName is null and $where"
        val testOption = collectSparkExpressionOption(session, tableName, testWhere)
        testOption match {
          case None => Expressions.alwaysFalse()
          case _ => Expressions.alwaysTrue()
        }
      case _ => convertToIcebergExpression(option.get)
    }
  }

  case class DummyRelation(output: Seq[Attribute]) extends LeafNode
}
