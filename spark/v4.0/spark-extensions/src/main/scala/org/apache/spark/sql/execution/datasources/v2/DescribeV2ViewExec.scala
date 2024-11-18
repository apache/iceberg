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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.escapeSingleQuotedString
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.LeafExecNode
import scala.collection.JavaConverters._

case class DescribeV2ViewExec(
  output: Seq[Attribute],
  view: View,
  isExtended: Boolean) extends V2CommandExec with LeafExecNode {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def run(): Seq[InternalRow] = {
    if (isExtended) {
      (describeSchema :+ emptyRow) ++ describeExtended
    } else {
      describeSchema
    }
  }

  private def describeSchema: Seq[InternalRow] =
    view.schema().map { column =>
      toCatalystRow(
        column.name,
        column.dataType.simpleString,
        column.getComment().getOrElse(""))
    }

  private def emptyRow: InternalRow = toCatalystRow("", "", "")

  private def describeExtended: Seq[InternalRow] = {
    val outputColumns = view.queryColumnNames.mkString("[", ", ", "]")
    val properties: Map[String, String] = view.properties.asScala.toMap -- ViewCatalog.RESERVED_PROPERTIES.asScala
    val viewCatalogAndNamespace: Seq[String] = view.currentCatalog +: view.currentNamespace.toSeq
    val viewProperties = properties.toSeq.sortBy(_._1).map {
      case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
    }.mkString("[", ", ", "]")


    toCatalystRow("# Detailed View Information", "", "") ::
      toCatalystRow("Comment", view.properties.getOrDefault(ViewCatalog.PROP_COMMENT, ""), "") ::
      toCatalystRow("View Catalog and Namespace", viewCatalogAndNamespace.quoted, "") ::
      toCatalystRow("View Query Output Columns", outputColumns, "") ::
      toCatalystRow("View Properties", viewProperties, "") ::
      toCatalystRow("Created By", view.properties.getOrDefault(ViewCatalog.PROP_CREATE_ENGINE_VERSION, ""), "") ::
      Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"DescribeV2ViewExec"
  }
}
