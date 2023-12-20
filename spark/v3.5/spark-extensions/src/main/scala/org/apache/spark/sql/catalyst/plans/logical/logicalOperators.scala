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

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Cast
import org.apache.spark.sql.types.StructType

/**
 * A trait for view description used by [[View]] container.
 */
trait ViewDescription {
  val identifier: String
  val schema: StructType
  val viewText: Option[String]
  val viewCatalogAndNamespace: Seq[String]
  val viewQueryColumnNames: Seq[String]
  val properties: Map[String, String]
}

/**
 * This is a 1:1 copy of a Spark's V1 View case class but using slightly different parameters
 */
case class IcebergView(
  desc: ViewDescription,
  child: LogicalPlan) extends UnaryNode {

  override def output: Seq[Attribute] = child.output

  override def metadataOutput: Seq[Attribute] = Nil

  override def simpleString(maxFields: Int): String = {
    s"View (${desc.identifier}, ${output.mkString("[", ",", "]")})"
  }

  override def doCanonicalize(): LogicalPlan = child match {
    case p: Project if p.resolved && canRemoveProject(p) => p.child.canonicalized
    case _ => child.canonicalized
  }

  // When resolving a SQL view, we use an extra Project to add cast and alias to make sure the view
  // output schema doesn't change even if the table referenced by the view is changed after view
  // creation. We should remove this extra Project during canonicalize if it does nothing.
  // See more details in `SessionCatalog.fromCatalogTable`.
  private def canRemoveProject(p: Project): Boolean = {
    p.output.length == p.child.output.length && p.projectList.zip(p.child.output).forall {
      case (Alias(cast: Cast, name), childAttr) =>
        cast.child match {
          case a: AttributeReference =>
            a.dataType == cast.dataType && a.name == name && childAttr.semanticEquals(a)
          case _ => false
        }
      case _ => false
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): IcebergView =
    copy(child = newChild)
}
