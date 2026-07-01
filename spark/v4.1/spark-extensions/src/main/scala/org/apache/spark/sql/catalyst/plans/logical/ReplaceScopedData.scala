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

import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Logical node for the scoped-replace command:
 * {{{INSERT INTO t REPLACE USING (c1, .., cn) <query>}}}
 *
 * Semantics: delete target rows whose scope columns match a source row, then append the full
 * source in the same write. This node records the parsed command; the rewrite rule asks the table's
 * row-level operation builder to choose the configured implementation, such as group replacement
 * for copy-on-write tables or row deltas for merge-on-read tables.
 *
 * Scope columns are kept as raw multi-part names rather than resolved expressions so that they can
 * be resolved explicitly against the (resolved) target relation in the rewrite rule, avoiding
 * cross-child ambiguity when a name is present in both the target and the source.
 *
 * @param table the target relation
 * @param scopeColumns the replace-scope columns, as raw multi-part identifiers over the target
 * @param query the source plan whose rows are appended
 */
case class ReplaceScopedData(table: LogicalPlan, scopeColumns: Seq[Seq[String]], query: LogicalPlan)
    extends BinaryCommand {

  override def left: LogicalPlan = table

  override def right: LogicalPlan = query

  override def output: Seq[Attribute] = Nil

  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan,
      newRight: LogicalPlan): ReplaceScopedData =
    copy(table = newLeft, query = newRight)
}
