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
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._


case class CreateV2ViewExec(
                             catalog: ViewCatalog,
                             ident: Identifier,
                             sql: String,
                             currentCatalog: String,
                             currentNamespace: Array[String],
                             comment: Option[String],
                             viewSchema: StructType,
                             queryColumnNames: Array[String],
                             columnAliases: Array[String],
                             columnComments: Array[String],
                             properties: Map[String, String],
                             allowExisting: Boolean,
                             replace: Boolean) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val createEngineVersion = Some(engineVersion)
    val newProperties = properties ++
      comment.map(ViewCatalog.PROP_COMMENT -> _) ++
      createEngineVersion.map(ViewCatalog.PROP_CREATE_ENGINE_VERSION -> _) +
      (ViewCatalog.PROP_ENGINE_VERSION -> engineVersion)

    if (replace) {
      // CREATE OR REPLACE VIEW
      if (catalog.viewExists(ident)) {
        catalog.dropView(ident)
      }
      // FIXME: replaceView API doesn't exist in Spark 3.5
      catalog.createView(
        ident,
        sql,
        currentCatalog,
        currentNamespace,
        viewSchema,
        queryColumnNames,
        columnAliases,
        columnComments,
        newProperties.asJava)
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        catalog.createView(
          ident,
          sql,
          currentCatalog,
          currentNamespace,
          viewSchema,
          queryColumnNames,
          columnAliases,
          columnComments,
          newProperties.asJava)
      } catch {
        case _: ViewAlreadyExistsException if allowExisting => // Ignore
      }
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateV2ViewExec: ${ident}"
  }
}
