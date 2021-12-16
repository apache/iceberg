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

package org.apache.spark.sql.catalyst.planning

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ReplaceData
import org.apache.spark.sql.catalyst.plans.logical.RowLevelCommand
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation

/**
 * An extractor for operations such as DELETE and MERGE that require rewriting data.
 *
 * This class extracts the following entities:
 *  - the row-level command (such as DeleteFromIcebergTable);
 *  - the read relation in the rewrite plan that can be either DataSourceV2Relation or
 *  DataSourceV2ScanRelation depending on whether the planning has already happened;
 *  - the current rewrite plan.
 */
object RewrittenRowLevelCommand {
  type ReturnType = (RowLevelCommand, LogicalPlan, LogicalPlan)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case c: RowLevelCommand if c.rewritePlan.nonEmpty =>
      val rewritePlan = c.rewritePlan.get

      // both ReplaceData and WriteDelta reference a write relation
      // but the corresponding read relation should be at the bottom of the write plan
      // both the write and read relations will share the same RowLevelOperationTable object
      // that's why it is safe to use reference equality to find the needed read relation

      rewritePlan match {
        case rd @ ReplaceData(DataSourceV2Relation(table, _, _, _, _), query, _, _) =>
          val readRelation = findReadRelation(table, query)
          readRelation.map((c, _, rd))
        case _ =>
          None
      }

    case _ =>
      None
  }

  private def findReadRelation(
      table: Table,
      plan: LogicalPlan): Option[LogicalPlan] = {

    val readRelations = plan.collect {
      case r: DataSourceV2Relation if r.table eq table => r
      case r: DataSourceV2ScanRelation if r.relation.table eq table => r
    }

    // in some cases, the optimizer replaces the v2 read relation with a local relation
    // for example, there is no reason to query the table if the condition is always false
    // that's why it is valid not to find the corresponding v2 read relation

    readRelations match {
      case relations if relations.isEmpty =>
        None

      case Seq(relation) =>
        Some(relation)

      case relations =>
        throw new AnalysisException(s"Expected only one row-level read relation: $relations")
    }
  }
}
