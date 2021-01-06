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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet}
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter

// TODO: fix stats (ignore the fact it is a binary node and report only scanRelation stats)
case class DynamicFileFilter(
    scanPlan: LogicalPlan,
    fileFilterPlan: LogicalPlan,
    filterable: SupportsFileFilter) extends BinaryNode {

  @transient
  override lazy val references: AttributeSet = AttributeSet(fileFilterPlan.output)

  override def left: LogicalPlan = scanPlan
  override def right: LogicalPlan = fileFilterPlan
  override def output: Seq[Attribute] = scanPlan.output
}
