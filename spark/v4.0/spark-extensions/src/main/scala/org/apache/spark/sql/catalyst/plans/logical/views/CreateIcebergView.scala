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
package org.apache.spark.sql.catalyst.plans.logical.views

import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.plans.logical.AnalysisOnlyCommand
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

// Align Iceberg's CreateIcebergView with Spark’s CreateViewCommand by extending AnalysisOnlyCommand.
// The command’s children are analyzed then hidden, so the optimizer/planner won’t traverse the view body.
case class CreateIcebergView(
    child: LogicalPlan,
    queryText: String,
    query: LogicalPlan,
    columnAliases: Seq[String],
    columnComments: Seq[Option[String]],
    queryColumnNames: Seq[String] = Seq.empty,
    comment: Option[String],
    properties: Map[String, String],
    allowExisting: Boolean,
    replace: Boolean,
    rewritten: Boolean = false,
    isAnalyzed: Boolean = false)
    extends AnalysisOnlyCommand {

  override def childrenToAnalyze: Seq[LogicalPlan] = child :: query :: Nil

  override def markAsAnalyzed(analysisContext: AnalysisContext): LogicalPlan = {
    copy(isAnalyzed = true)
  }

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): LogicalPlan = {
    assert(!isAnalyzed)
    copy(child = newChildren.head, query = newChildren.last)
  }
}
