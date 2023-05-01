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
import org.apache.spark.sql.catalyst.analysis.AlignedRowLevelIcebergCommandCheck
import org.apache.spark.sql.catalyst.analysis.AlignRowLevelCommandAssignments
import org.apache.spark.sql.catalyst.analysis.CheckMergeIntoTableConditions
import org.apache.spark.sql.catalyst.analysis.MergeIntoIcebergTableResolutionCheck
import org.apache.spark.sql.catalyst.analysis.ProcedureArgumentCoercion
import org.apache.spark.sql.catalyst.analysis.ResolveMergeIntoTableReferences
import org.apache.spark.sql.catalyst.analysis.ResolveProcedures
import org.apache.spark.sql.catalyst.analysis.RewriteMergeIntoTable
import org.apache.spark.sql.catalyst.analysis.RewriteUpdateTable
import org.apache.spark.sql.catalyst.optimizer.ExtendedReplaceNullWithFalseInPredicate
import org.apache.spark.sql.catalyst.optimizer.ExtendedSimplifyConditionalsInPredicate
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSparkSqlExtensionsParser
import org.apache.spark.sql.execution.datasources.v2.ExtendedDataSourceV2Strategy
import org.apache.spark.sql.execution.datasources.v2.ExtendedV2Writes
import org.apache.spark.sql.execution.datasources.v2.ReplaceRewrittenRowLevelCommand
import org.apache.spark.sql.execution.datasources.v2.RowLevelCommandScanRelationPushDown
import org.apache.spark.sql.execution.dynamicpruning.RowLevelCommandDynamicPruning

class IcebergSparkSessionExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // parser extensions
    extensions.injectParser { case (_, parser) => new IcebergSparkSqlExtensionsParser(parser) }

    // analyzer extensions
    extensions.injectResolutionRule { spark => ResolveProcedures(spark) }
    extensions.injectResolutionRule { spark => ResolveMergeIntoTableReferences(spark) }
    extensions.injectResolutionRule { _ => CheckMergeIntoTableConditions }
    extensions.injectResolutionRule { _ => ProcedureArgumentCoercion }
    extensions.injectResolutionRule { _ => AlignRowLevelCommandAssignments }
    extensions.injectResolutionRule { _ => RewriteUpdateTable }
    extensions.injectResolutionRule { _ => RewriteMergeIntoTable }
    extensions.injectCheckRule { _ => MergeIntoIcebergTableResolutionCheck }
    extensions.injectCheckRule { _ => AlignedRowLevelIcebergCommandCheck }

    // optimizer extensions
    extensions.injectOptimizerRule { _ => ExtendedSimplifyConditionalsInPredicate }
    extensions.injectOptimizerRule { _ => ExtendedReplaceNullWithFalseInPredicate }
    // pre-CBO rules run only once and the order of the rules is important
    // - dynamic filters should be added before replacing commands with rewrite plans
    // - scans must be planned before building writes
    extensions.injectPreCBORule { _ => RowLevelCommandScanRelationPushDown }
    extensions.injectPreCBORule { _ => ExtendedV2Writes }
    extensions.injectPreCBORule { spark => RowLevelCommandDynamicPruning(spark) }
    extensions.injectPreCBORule { _ => ReplaceRewrittenRowLevelCommand }

    // planner extensions
    extensions.injectPlannerStrategy { spark => ExtendedDataSourceV2Strategy(spark) }
  }
}
