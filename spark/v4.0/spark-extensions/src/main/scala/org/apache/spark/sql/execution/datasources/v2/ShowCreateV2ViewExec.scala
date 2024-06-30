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

case class ShowCreateV2ViewExec(output: Seq[Attribute], view: View)
  extends V2CommandExec with LeafExecNode {

  override protected def run(): Seq[InternalRow] = {
    val builder = new StringBuilder
    builder ++= s"CREATE VIEW ${view.name} "
    showColumns(view, builder)
    showComment(view, builder)
    showProperties(view, builder)
    builder ++= s"AS\n${view.query}\n"

    Seq(toCatalystRow(builder.toString))
  }

  private def showColumns(view: View, builder: StringBuilder): Unit = {
    val columns = concatByMultiLines(
      view.schema().fields
        .map(x => s"${x.name}${x.getComment().map(c => s" COMMENT '$c'").getOrElse("")}"))
    builder ++= columns
  }

  private def showComment(view: View, builder: StringBuilder): Unit = {
    Option(view.properties.get(ViewCatalog.PROP_COMMENT))
      .map("COMMENT '" + escapeSingleQuotedString(_) + "'\n")
      .foreach(builder.append)
  }

  private def showProperties(
    view: View,
    builder: StringBuilder): Unit = {
    val showProps = view.properties.asScala.toMap -- ViewCatalog.RESERVED_PROPERTIES.asScala
    if (showProps.nonEmpty) {
      val props = conf.redactOptions(showProps).toSeq.sortBy(_._1).map {
        case (key, value) =>
          s"'${escapeSingleQuotedString(key)}' = '${escapeSingleQuotedString(value)}'"
      }

      builder ++= "TBLPROPERTIES "
      builder ++= concatByMultiLines(props)
    }
  }

  private def concatByMultiLines(iter: Iterable[String]): String = {
    iter.mkString("(\n  ", ",\n  ", ")\n")
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowCreateV2ViewExec"
  }
}
