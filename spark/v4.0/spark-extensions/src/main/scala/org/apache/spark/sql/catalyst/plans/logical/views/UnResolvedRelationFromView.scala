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
 * This node carries both the table identifier and the chain of view identifiers that
 * referenced it, enabling view-aware authorization and security mechanisms for nested views.
 *
 * This allows catalogs to implement different security models such as:
 * - Definer rights: Use view creator's permissions to access the underlying table
 * - Invoker rights: Use current user's permissions to access the underlying table
 *
 * When passed to REST catalogs, the view chain is encoded as a comma-separated list
 * with unit separator character (0x1F) for nested namespaces in the "referenced-by" query parameter.
 * The chain is ordered from outermost to innermost view.
 *
 * @param tableMultipartIdentifier The multipart identifier for the target table
 * @param viewMultipartIdentifierChain The chain of view identifiers (outermost first) referencing this table
 * @param options Options passed to the table (similar to UnresolvedRelation)
 * @param isStreaming Whether this is a streaming relation
 */
case class UnResolvedRelationFromView(
  tableMultipartIdentifier: Seq[String],
  viewMultipartIdentifierChain: Seq[Seq[String]],
  options: CaseInsensitiveStringMap = CaseInsensitiveStringMap.empty(),
  override val isStreaming: Boolean = false) extends LeafNode {

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false

  def tableName: String = tableMultipartIdentifier.map(quoteIfNeeded).mkString(".")

  def viewChainAsString: String = viewMultipartIdentifierChain
    .map(_.map(quoteIfNeeded).mkString("."))
    .mkString(" → ")

  override def simpleString(maxFields: Int): String = {
    s"'UnresolvedRelationFromView [table=$tableName, viewChain=$viewChainAsString, " +
      s"${if (isStreaming) "streaming=true, " else ""}options=$options]"
  }

  override def toString: String = {
    val viewChainStr = viewMultipartIdentifierChain
      .map(v => s"[${v.mkString(", ")}]")
      .mkString(", ")
    s"UnresolvedRelationFromView([${tableMultipartIdentifier.mkString(", ")}], " +
      s"[$viewChainStr], $isStreaming)"
  }
}
