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

package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.{AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.{CallStatement, DynamicFileFilter, LogicalPlan, OverwriteFiles}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

object ExtendedDataSourceV2Strategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case _: CallStatement =>
      throw new AnalysisException("CALL statements are not currently supported")
    case DynamicFileFilter(scanRelation, fileFilterPlan) =>
      val scanExec = ExtendedBatchScanExec(scanRelation.output, scanRelation.scan)
      val dynamicFileFilter = DynamicFileFilterExec(scanExec, planLater(fileFilterPlan))
      if (scanExec.supportsColumnar) {
        dynamicFileFilter :: Nil
      } else {
        // add a projection to ensure we have UnsafeRows required by some operations
        ProjectExec(scanRelation.output, dynamicFileFilter) :: Nil
      }
    case OverwriteFiles(_, batchWrite, query) =>
      OverwriteFilesExec(batchWrite, planLater(query)) :: Nil
    case _ => Nil
  }
}
