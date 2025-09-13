/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.spark.sql.catalyst.plans.logical.views

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Represents an unresolved table/relation that was referenced from within a view.
 * This node carries both the table identifier and the view identifier that
 * referenced it, enabling view-aware authorization and security mechanisms.
 *
 * This allows catalogs to implement different security models such as:
 * - Definer rights: Use view creator's permissions to access the underlying table
 * - Invoker rights: Use current user's permissions to access the underlying table
 *
 * When passed to REST catalogs, the view identifier is encoded using unit separator
 * character (0x1F) for nested namespaces in the "referenced-by" query parameter.
 *
 * @param tableMultipartIdentifier The multipart identifier for the target table
 * @param viewMultipartIdentifier The multipart identifier for the view that references this table
 * @param options Options passed to the table (similar to UnresolvedRelation)
 * @param isStreaming Whether this is a streaming relation
 */
case class UnResolvedRelationFromView(
  tableMultipartIdentifier: Seq[String],
  viewMultipartIdentifier: Seq[String],
  options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
  override val isStreaming: Boolean = false) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  def tableName: String = tableMultipartIdentifier.map(quoteIfNeeded).mkString(".")

  def viewName: String = viewMultipartIdentifier.map(quoteIfNeeded).mkString(".")

  override def simpleString(maxFields: Int): String = {
    s"'UnresolvedRelationFromView [table=$tableName, view=$viewName, " +
      s"${if (isStreaming) "streaming=true, " else ""}options=$options]"
  }

  override def toString: String = {
    s"UnresolvedRelationFromView([${tableMultipartIdentifier.mkString(", ")}], " +
      s"[${viewMultipartIdentifier.mkString(", ")}], $isStreaming)"
  }
}
