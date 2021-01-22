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

package org.apache.spark.sql.catalyst.utils

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

object IcebergTable {
  def unapply(relation: DataSourceV2Relation): Option[DataSourceV2Relation] = relation.table match {
    case _: SparkTable => Some(relation)
    case _ => None
  }
}

object AliasedIcebergTable {
  def checkRelation(relation: DataSourceV2Relation): Option[DataSourceV2Relation] = relation.table match {
    case _: SparkTable => Some(relation)
    case _ => None
  }

  def unapply(relation: LogicalPlan): Option[LogicalPlan] = relation match {
    case s: SubqueryAlias => unapply(s.child)
    case r: DataSourceV2Relation => checkRelation(r)
    case _ => None
  }
}
