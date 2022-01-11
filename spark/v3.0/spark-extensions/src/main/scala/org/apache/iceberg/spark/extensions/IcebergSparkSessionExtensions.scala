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
import org.apache.spark.sql.catalyst.analysis.AlignRowLevelOperations
import org.apache.spark.sql.catalyst.analysis.ProcedureArgumentCoercion
import org.apache.spark.sql.catalyst.analysis.ResolveProcedures
import org.apache.spark.sql.catalyst.analysis.RowLevelOperationsPredicateCheck
import org.apache.spark.sql.catalyst.optimizer.OptimizeConditionsInRowLevelOperations
import org.apache.spark.sql.catalyst.optimizer.PullupCorrelatedPredicatesInRowLevelOperations
import org.apache.spark.sql.catalyst.optimizer.RewriteDelete
import org.apache.spark.sql.catalyst.optimizer.RewriteMergeInto
import org.apache.spark.sql.catalyst.optimizer.RewriteUpdate
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy

class IcebergSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // analyzer extensions
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectPostHocResolutionRule { spark => AlignRowLevelOperations }
    extensions.injectCheckRule { _ => RowLevelOperationsPredicateCheck }

    // optimizer extensions
    extensions.injectOptimizerRule { _ => OptimizeConditionsInRowLevelOperations }
    extensions.injectOptimizerRule { _ => PullupCorrelatedPredicatesInRowLevelOperations }
    extensions.injectOptimizerRule { spark => RewriteDelete(spark) }
    extensions.injectOptimizerRule { spark => RewriteUpdate(spark) }
    extensions.injectOptimizerRule { spark => RewriteMergeInto(spark) }

    // planner extensions
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
  }
}
