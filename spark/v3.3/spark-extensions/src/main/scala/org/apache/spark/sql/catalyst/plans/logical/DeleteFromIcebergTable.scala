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

import org.apache.spark.sql.catalyst.expressions.Expression

case class DeleteFromIcebergTable(
    table: LogicalPlan,
    condition: Option[Expression],
    rewritePlan: Option[LogicalPlan] = None) extends RowLevelCommand {

  override def children: Seq[LogicalPlan] = if (rewritePlan.isDefined) {
    table :: rewritePlan.get :: Nil
  } else {
    table :: Nil
  }

  override def withNewRewritePlan(newRewritePlan: LogicalPlan): RowLevelCommand = {
    copy(rewritePlan = Some(newRewritePlan))
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): DeleteFromIcebergTable = {
    if (newChildren.size == 1) {
      copy(table = newChildren.head, rewritePlan = None)
    } else {
      require(newChildren.size == 2, "DeleteFromIcebergTable expects either one or two children")
      val Seq(newTable, newRewritePlan) = newChildren.take(2)
      copy(table = newTable, rewritePlan = Some(newRewritePlan))
    }
  }
}
