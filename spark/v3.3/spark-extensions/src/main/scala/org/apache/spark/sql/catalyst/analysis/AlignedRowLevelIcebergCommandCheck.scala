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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateIcebergTable

object AlignedRowLevelIcebergCommandCheck extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {
    println("AlignedRowLevelIcebergCommandCheck : plan", plan)
    applyX(plan)
  }

  def applyX(plan: LogicalPlan): Unit = {
    plan foreach {
      case m: MergeIntoIcebergTable if !m.aligned =>
        throw new AnalysisException(s"Could not align Iceberg MERGE INTO: $m")
      case u: UpdateIcebergTable if !u.aligned =>
        throw new AnalysisException(s"Could not align Iceberg UPDATE: $u")
      case _ => // OK
    }
  }
}
