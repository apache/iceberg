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
package org.apache.iceberg.spark.vendor

import org.apache.iceberg.io.FileIO
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode

case class BatchApply(
    child: LogicalPlan,
    function: String,
    functionClass: String,
    batchSize: Int,
    output: Seq[Attribute],
    io: FileIO)
    extends UnaryNode {

  override def producedAttributes: AttributeSet = AttributeSet(output)

  // every child column is passed to the function, so none of them may be pruned away
  override def references: AttributeSet = child.outputSet

  override protected def withNewChildInternal(newChild: LogicalPlan): BatchApply = {
    copy(child = newChild)
  }

  override def simpleString(maxFields: Int): String = {
    s"BatchApply $function batchSize=$batchSize"
  }
}
