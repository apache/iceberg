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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.plans.physical.KeyedPartitioning
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.physical.PartitioningCollection

case class ChangelogDistributionExec(keys: Seq[Expression], child: SparkPlan)
    extends UnaryExecNode {
  override def output: Seq[Attribute] = child.output

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    Seq(
      ClusteredDistribution(keys, requireAllClusterKeys = !safeGrouping(child.outputPartitioning)))

  override protected def doExecute(): RDD[InternalRow] = child.execute()

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)

  private def safeGrouping(partitioning: Partitioning): Boolean = partitioning match {
    case p: KeyedPartitioning =>
      val partitionKeys = ExpressionSet(p.expressions.flatMap(_.collectLeaves()))
      partitionKeys.subsetOf(ExpressionSet(keys))
    case p: PartitioningCollection => p.partitionings.forall(safeGrouping)
    case _ => true
  }
}
