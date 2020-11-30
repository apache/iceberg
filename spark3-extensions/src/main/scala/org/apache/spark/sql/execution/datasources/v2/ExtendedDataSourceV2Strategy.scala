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

import org.apache.spark.sql.Strategy
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.{Call, DynamicFileFilter, LogicalPlan, ReplaceData}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

object ExtendedDataSourceV2Strategy extends Strategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case c @ Call(procedure, args) =>
      val input = buildInternalRow(args)
      CallExec(c.output, procedure, input) :: Nil
    case DynamicFileFilter(scanRelation, fileFilterPlan) =>
      // we don't use planLater here as we need ExtendedBatchScanExec, not BatchScanExec
      val scanExec = ExtendedBatchScanExec(scanRelation.output, scanRelation.scan)
      val dynamicFileFilter = DynamicFileFilterExec(scanExec, planLater(fileFilterPlan))
      if (scanExec.supportsColumnar) {
        dynamicFileFilter :: Nil
      } else {
        // add a projection to ensure we have UnsafeRows required by some operations
        ProjectExec(scanRelation.output, dynamicFileFilter) :: Nil
      }
    case ReplaceData(_, batchWrite, query) =>
      ReplaceDataExec(batchWrite, planLater(query)) :: Nil
    case _ => Nil
  }

  private def buildInternalRow(exprs: Seq[Expression]): InternalRow = {
    val values = new Array[Any](exprs.size)
    for (index <- exprs.indices) {
      values(index) = exprs(index).eval()
    }
    new GenericInternalRow(values)
  }
}
