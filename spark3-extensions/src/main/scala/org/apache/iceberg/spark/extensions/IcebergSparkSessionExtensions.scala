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

package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import org.apache.spark.sql.catalyst.analysis.{DeleteFromTablePredicateCheck, ProcedureArgumentCoercion, ResolveProcedures}
import org.apache.spark.sql.catalyst.optimizer.{OptimizeConditionsInRowLevelOperations, PullupCorrelatedPredicatesInRowLevelOperations, RewriteDelete}
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy

class IcebergSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectCheckRule { _ => DeleteFromTablePredicateCheck }
    // TODO: RewriteDelete should be executed after the operator optimization batch
    extensions.injectOptimizerRule { _ => OptimizeConditionsInRowLevelOperations }
    // TODO: PullupCorrelatedPredicates should handle row-level operations
    extensions.injectOptimizerRule { _ => PullupCorrelatedPredicatesInRowLevelOperations }
    extensions.injectOptimizerRule { _ => RewriteDelete }
    extensions.injectPlannerStrategy { _ => ExtendedDataSourceV2Strategy }
  }
}
