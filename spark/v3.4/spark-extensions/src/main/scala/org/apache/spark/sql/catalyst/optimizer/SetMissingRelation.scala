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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSource

/**
 * When running structured streaming using `.start()` instead of `.toTable(...)`,
 * the relation is empty, and the catalog functions will be missing.
 * The catalog functions are required when a table is partitioned with a transform.
 */
case class SetMissingRelation(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan match {
    case p: WriteToMicroBatchDataSource if p.relation.isEmpty =>
      import spark.sessionState.analyzer.CatalogAndIdentifier
      val originalMultipartIdentifier = spark.sessionState.sqlParser
        .parseMultipartIdentifier(p.table.name())
      val CatalogAndIdentifier(catalog, identifier) = originalMultipartIdentifier
      WriteToMicroBatchDataSource(
        Some(DataSourceV2Relation.create(p.table, Some(catalog), Some(identifier))),
        p.table,
        p.query,
        p.queryId,
        p.writeOptions,
        p.outputMode,
        p.batchId
      )
    case _ => plan
  }
}
