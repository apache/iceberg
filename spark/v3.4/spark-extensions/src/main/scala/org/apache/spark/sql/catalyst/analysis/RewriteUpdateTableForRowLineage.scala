/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData
import org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta

object RewriteUpdateTableForRowLineage extends RewriteOperationForRowLineage {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators  {
      case updateTable@UpdateIcebergTable(_, _, _, Some(rewritePlan))
        if updatePlanForRowLineage(updateTable, rewritePlan) => {
        rewritePlan match {
          case writeIcebergDelta@WriteIcebergDelta(_, _, _, _, _) =>
            updateTable.copy(rewritePlan = Some(updateDeltaPlan(writeIcebergDelta)))
          case replaceIcebergData@ReplaceIcebergData(_, _, _, _) =>
            updateTable.copy(rewritePlan = Some(updateReplacePlan(replaceIcebergData)))
          case _: WriteIcebergDelta | _: ReplaceIcebergData => {
            updateTable
          }
        }
      }
    }
  }
}
