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

import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet

// a node similar to V2WriteCommand in Spark but does not extend Command
// as ReplaceData and WriteDelta that extend this trait are nested within other commands
trait V2WriteCommandLike extends UnaryNode {
  def table: NamedRelation
  def query: LogicalPlan
  def outputResolved: Boolean

  override lazy val resolved: Boolean = table.resolved && query.resolved && outputResolved

  override def child: LogicalPlan = query
  override def output: Seq[Attribute] = Seq.empty
  override def producedAttributes: AttributeSet = outputSet
  // Commands are eagerly executed. They will be converted to LocalRelation after the DataFrame
  // is created. That said, the statistics of a command is useless. Here we just return a dummy
  // statistics to avoid unnecessary statistics calculation of command's children.
  override def stats: Statistics = Statistics.DUMMY
}
