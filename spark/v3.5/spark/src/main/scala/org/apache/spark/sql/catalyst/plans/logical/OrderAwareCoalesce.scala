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

import org.apache.spark.rdd.PartitionCoalescer
import org.apache.spark.rdd.PartitionGroup
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute

// this node doesn't extend RepartitionOperation on purpose to keep this logic isolated
// and ignore it in optimizer rules such as CollapseRepartition
case class OrderAwareCoalesce(
    numPartitions: Int,
    coalescer: PartitionCoalescer,
    child: LogicalPlan) extends OrderPreservingUnaryNode {

  override def output: Seq[Attribute] = child.output

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = {
    copy(child = newChild)
  }
}

class OrderAwareCoalescer(val groupSize: Int) extends PartitionCoalescer with Serializable {

  override def coalesce(maxPartitions: Int, parent: RDD[_]): Array[PartitionGroup] = {
    val partitionBins = parent.partitions.grouped(groupSize)
    partitionBins.map { partitions =>
      val group = new PartitionGroup()
      group.partitions ++= partitions
      group
    }.toArray
  }
}
