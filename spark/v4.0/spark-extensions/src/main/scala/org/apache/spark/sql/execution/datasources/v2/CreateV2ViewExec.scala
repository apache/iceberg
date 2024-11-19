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

import org.apache.iceberg.spark.SupportsReplaceView
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.connector.catalog.ViewInfo
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._


case class CreateV2ViewExec(
  catalog: ViewCatalog,
  ident: Identifier,
  queryText: String,
  viewSchema: StructType,
  columnAliases: Seq[String],
  columnComments: Seq[Option[String]],
  queryColumnNames: Seq[String],
  comment: Option[String],
  properties: Map[String, String],
  allowExisting: Boolean,
  replace: Boolean) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog = if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val newProperties = properties ++
      comment.map(ViewCatalog.PROP_COMMENT -> _) +
      (ViewCatalog.PROP_CREATE_ENGINE_VERSION -> engineVersion,
        ViewCatalog.PROP_ENGINE_VERSION -> engineVersion)

    if (replace) {
      // CREATE OR REPLACE VIEW
      catalog match {
        case c: SupportsReplaceView =>
          try {
            replaceView(c, currentCatalog, currentNamespace, newProperties)
          } catch {
            // view might have been concurrently dropped during replace
            case _: NoSuchViewException =>
              replaceView(c, currentCatalog, currentNamespace, newProperties)
          }
        case _ =>
          if (catalog.viewExists(ident)) {
            catalog.dropView(ident)
          }

          createView(currentCatalog, currentNamespace, newProperties)
      }
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        createView(currentCatalog, currentNamespace, newProperties)
      } catch {
        case _: ViewAlreadyExistsException if allowExisting => // Ignore
      }
    }

    Nil
  }

  private def replaceView(
    supportsReplaceView: SupportsReplaceView,
    currentCatalog: String,
    currentNamespace: Array[String],
    newProperties: Map[String, String]) = {
    supportsReplaceView.replaceView(
      ident,
      queryText,
      currentCatalog,
      currentNamespace,
      viewSchema,
      queryColumnNames.toArray,
      columnAliases.toArray,
      columnComments.map(c => c.orNull).toArray,
      newProperties.asJava)
  }

  private def createView(
    currentCatalog: String,
    currentNamespace: Array[String],
    newProperties: Map[String, String]) = {
    val viewInfo: ViewInfo = new ViewInfo(
      ident,
      queryText,
      currentCatalog,
      currentNamespace,
      viewSchema,
      queryColumnNames.toArray,
      columnAliases.toArray,
      columnComments.map(c => c.orNull).toArray,
      newProperties.asJava)
    catalog.createView(viewInfo)
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateV2ViewExec: ${ident}"
  }
}
