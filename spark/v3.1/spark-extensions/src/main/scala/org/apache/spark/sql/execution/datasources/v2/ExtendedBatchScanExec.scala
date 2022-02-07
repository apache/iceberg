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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.read.PartitionReaderFactory
import org.apache.spark.sql.connector.read.Scan

// The only reason we need this class and cannot reuse BatchScanExec is because
// BatchScanExec caches input partitions and we cannot apply file filtering before execution
// Spark calls supportsColumnar during physical planning which, in turn, triggers split planning
// We must ensure the result is not cached so that we can push down file filters later
// The only difference compared to BatchScanExec is that we are using def instead of lazy val for splits
case class ExtendedBatchScanExec(
    output: Seq[AttributeReference],
    @transient scan: Scan) extends DataSourceV2ScanExecBase {

  @transient private lazy val batch = scan.toBatch

  // TODO: unify the equal/hashCode implementation for all data source v2 query plans.
  override def equals(other: Any): Boolean = other match {
    case other: ExtendedBatchScanExec => this.batch == other.batch
    case _ => false
  }

  override def hashCode(): Int = batch.hashCode()

  override def partitions: Seq[InputPartition] = batch.planInputPartitions()

  override lazy val readerFactory: PartitionReaderFactory = batch.createReaderFactory()

  override def inputRDD: RDD[InternalRow] = {
    new DataSourceRDD(sparkContext, partitions, readerFactory, supportsColumnar)
  }

  override def doCanonicalize(): ExtendedBatchScanExec = {
    this.copy(output = output.map(QueryPlan.normalizeExpressions(_, output)))
  }
}
