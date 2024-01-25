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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.SchemaUtils

object CheckViews extends (LogicalPlan => Unit) {

  override def apply(plan: LogicalPlan): Unit = {
    plan foreach {
      case CreateIcebergView(ResolvedIdentifier(_: ViewCatalog, ident), _, query, columnAliases, _,
      queryColumnNames, _, _, _, _, _) =>
        verifyColumnCount(ident, columnAliases, query)
        SchemaUtils.checkColumnNameDuplication(queryColumnNames, SQLConf.get.resolver)

      case _ => // OK
    }
  }

  private def verifyColumnCount(ident: Identifier, columns: Seq[String], query: LogicalPlan): Unit = {
    if (columns.nonEmpty) {
      if (columns.length > query.output.length) {
        throw new AnalysisException(
          errorClass = "CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          messageParameters = Map(
            "viewName" -> ident.toString,
            "viewColumns" -> columns.mkString(", "),
            "dataColumns" -> query.output.map(c => c.name).mkString(", ")))
      } else if (columns.length < query.output.length) {
        throw new AnalysisException(
          errorClass = "CREATE_VIEW_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS",
          messageParameters = Map(
            "viewName" -> ident.toString,
            "viewColumns" -> columns.mkString(", "),
            "dataColumns" -> query.output.map(c => c.name).mkString(", ")))
      }
    }
  }
}
