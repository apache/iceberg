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
import org.apache.spark.sql.catalyst.util.quoteIfNeeded
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.execution.LeafExecNode
import scala.jdk.CollectionConverters._

/**
 * Executes SHOW CREATE VIEW for Spark V2 views.
 *
 * Uses a custom command instead of Spark's built-in implementation so generated view DDL omits
 * Iceberg reserved metadata properties.
 */
case class IcebergShowCreateV2ViewExec(output: Seq[Attribute], quotedName: String, view: View)
    extends V2CommandExec
    with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    val builder = new StringBuilder
    builder ++= s"CREATE VIEW $quotedName "
    showColumns(view, builder)
    showComment(view, builder)
    showCollation(view, builder)
    showProperties(view, builder)
    showSchemaMode(view, builder)
    builder ++= s"AS\n${view.queryText}\n"

    Seq(toCatalystRow(builder.toString))
  }

  private def showColumns(view: View, builder: StringBuilder): Unit = {
    val columns = concatByMultiLines(
      view.schema.fields
        .map { column =>
          val comment = column.getComment().map { value =>
            s" COMMENT '${escapeSingleQuotedString(value)}'"
          }
          s"${quoteIfNeeded(column.name)}${comment.getOrElse("")}"
        })
    builder ++= columns
  }

  private def showComment(view: View, builder: StringBuilder): Unit = {
    Option(view.properties.get(TableCatalog.PROP_COMMENT))
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def showCollation(view: View, builder: StringBuilder): Unit = {
    Option(view.properties.get(TableCatalog.PROP_COLLATION))
      .map("DEFAULT COLLATION " + _ + "\n")
      .foreach(builder.append)
  }

  private def showProperties(view: View, builder: StringBuilder): Unit = {
    val reservedProperties = ViewUtil.RESERVED_PROPERTIES :+ TableCatalog.PROP_COLLATION
    val showProps = view.properties.asScala.toMap -- reservedProperties
    if (showProps.nonEmpty) {
      val props = conf.redactOptions(showProps).toSeq.sortBy(_._1).map { case (key, value) =>
        s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }

  private def showSchemaMode(view: View, builder: StringBuilder): Unit = {
    if (conf.viewSchemaBindingEnabled) {
      Option(view.schemaMode)
        .map("WITH SCHEMA " + _ + "\n")
        .foreach(builder.append)
    }
  }

  private def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }

  override def simpleString(maxFields: Int): String = {
    s"IcebergShowCreateV2ViewExec"
  }
}
