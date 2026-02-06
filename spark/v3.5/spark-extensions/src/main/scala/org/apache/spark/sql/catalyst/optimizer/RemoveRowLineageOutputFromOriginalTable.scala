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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.iceberg.MetadataColumns
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.WriteDelta
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * RemoveRowLineageOutputFromOriginalTable removes row lineage outputs from Dsv2 write's
 * originalTable so that downstream behaviors like relation caching just work, without having to
 * modify physical planning strategies.
 */
object RemoveRowLineageOutputFromOriginalTable extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transform {
      case writeDelta @ WriteDelta(_, _, _, originalTable, _, _) =>
        writeDelta.copy(originalTable = removeRowLineageOutput(originalTable))
      case replaceData @ ReplaceData(_, _, _, originalTable, _, _) =>
        replaceData.copy(originalTable = removeRowLineageOutput(originalTable))
    }
  }

  private def removeRowLineageOutput(table: NamedRelation): DataSourceV2Relation = {
    table match {
      case dsv2Relation @ DataSourceV2Relation(_, _, _, _, _) =>
        dsv2Relation.copy(output = dsv2Relation.output.filterNot(attr =>
          attr.name == MetadataColumns.ROW_ID.name() ||
            attr.name == MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()))
    }
  }
}
