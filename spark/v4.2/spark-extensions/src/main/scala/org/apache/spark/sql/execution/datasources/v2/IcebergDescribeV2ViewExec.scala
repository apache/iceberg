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
import org.apache.spark.sql.catalyst.analysis.ViewUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.escapeSingleQuotedString
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.execution.LeafExecNode
import scala.jdk.CollectionConverters._

/**
 * Executes DESCRIBE VIEW for Spark V2 views.
 *
 * Uses a custom command instead of Spark's built-in implementation so Iceberg reserved metadata is
 * filtered consistently from the detailed view output.
 */
case class IcebergDescribeV2ViewExec(
    output: Seq[Attribute],
    catalogName: String,
    ident: Identifier,
    view: View,
    isExtended: Boolean)
    extends V2CommandExec
    with LeafExecNode {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def run(): Seq[InternalRow] = {
    if (isExtended) {
      (describeSchema :+ emptyRow) ++ describeExtended
    } else {
      describeSchema
    }
  }

  private def describeSchema: Seq[InternalRow] =
    view.schema.map { column =>
      toCatalystRow(column.name, column.dataType.simpleString, column.getComment().getOrElse(""))
    }

  private def emptyRow: InternalRow = toCatalystRow("", "", "")

  private def describeExtended: Seq[InternalRow] = {
    val outputColumns = view.queryColumnNames.mkString("[", ", ", "]")
    val properties: Map[String, String] =
      view.properties.asScala.toMap -- ViewUtil.RESERVED_PROPERTIES
    val viewCatalogAndNamespace: Seq[String] = catalogName +: ident.namespace().toIndexedSeq
    val viewProperties = properties.toSeq
      .sortBy(_._1)
      .map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }
      .mkString("[", ", ", "]")

    toCatalystRow("# Detailed View Information", "", "") ::
      toCatalystRow("Comment", view.properties.getOrDefault(TableCatalog.PROP_COMMENT, ""), "") ::
      toCatalystRow("View Catalog and Namespace", viewCatalogAndNamespace.quoted, "") ::
      toCatalystRow("View Query Output Columns", outputColumns, "") ::
      toCatalystRow("View Properties", viewProperties, "") ::
      toCatalystRow(
        "Created By",
        view.properties.getOrDefault(ViewUtil.PROP_CREATE_ENGINE_VERSION, ""),
        "") ::
      Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"IcebergDescribeV2ViewExec"
  }
}
