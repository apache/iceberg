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
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.LeafExecNode
import scala.collection.mutable.ArrayBuffer

case class ShowV2ViewsExec(
  output: Seq[Attribute],
  catalog: ViewCatalog,
  namespace: Seq[String],
  pattern: Option[String]) extends V2CommandExec with LeafExecNode {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def run(): Seq[InternalRow] = {
    val rows = new ArrayBuffer[InternalRow]()

    // handle GLOBAL VIEWS
    // TODO: GlobalTempViewManager.database is not accessible any more.
    //       Comment out the global views handling for now until
    //       GlobalTempViewManager.database can be accessed again.
//    val globalTemp = session.sessionState.catalog.globalTempViewManager.database
//    if (namespace.nonEmpty && globalTemp == namespace.head) {
//      pattern.map(p => session.sessionState.catalog.globalTempViewManager.listViewNames(p))
//        .getOrElse(session.sessionState.catalog.globalTempViewManager.listViewNames("*"))
//        .map(name => rows += toCatalystRow(globalTemp, name, true))
//    } else {
      val views = catalog.listViews(namespace: _*)
      views.map { view =>
        if (pattern.map(StringUtils.filterPattern(Seq(view.name()), _).nonEmpty).getOrElse(true)) {
          rows += toCatalystRow(view.namespace().quoted, view.name(), false)
        }
      }
//    }

    // include TEMP VIEWS
    pattern.map(p => session.sessionState.catalog.listLocalTempViews(p))
      .getOrElse(session.sessionState.catalog.listLocalTempViews("*"))
      .map(v => rows += toCatalystRow(v.database.toArray.quoted, v.table, true))

    rows.toSeq
  }

  override def simpleString(maxFields: Int): String = {
    s"ShowV2ViewsExec"
  }
}
