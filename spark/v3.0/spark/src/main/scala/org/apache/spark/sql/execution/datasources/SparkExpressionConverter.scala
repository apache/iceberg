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

import org.apache.iceberg.spark.SparkFilters
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Filter

object SparkExpressionConverter {

  def convertToIcebergExpression(sparkExpression: Expression): org.apache.iceberg.expressions.Expression = {
    // Currently, it is a double conversion as we are converting Spark expression to Spark filter
    // and then converting Spark filter to Iceberg expression.
    // But these two conversions already exist and well tested. So, we are going with this approach.
    SparkFilters.convert(DataSourceStrategy.translateFilter(sparkExpression, supportNestedPredicatePushdown = true).get)
  }

  @throws[AnalysisException]
  def collectResolvedSparkExpression(session: SparkSession, tableName: String, where: String): Expression = {
    var expression: Expression = null
    // Add a dummy prefix linking to the table to collect the resolved spark expression from optimized plan.
    val prefix = String.format("SELECT 42 from %s where ", tableName)
    val logicalPlan = session.sessionState.sqlParser.parsePlan(prefix + where)
    val optimizedLogicalPlan = session.sessionState.executePlan(logicalPlan).optimizedPlan
    optimizedLogicalPlan.collectFirst {
      case filter: Filter =>
        expression = filter.expressions.head
    }
    expression
  }
}
