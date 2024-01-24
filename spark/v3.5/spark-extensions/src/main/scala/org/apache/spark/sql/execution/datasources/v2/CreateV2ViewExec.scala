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

import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.CommandExecutionMode
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.SchemaUtils
import scala.collection.JavaConverters._


case class CreateV2ViewExec(
  catalog: ViewCatalog,
  ident: Identifier,
  queryText: String,
  viewSchema: StructType,
  columnAliases: Seq[String],
  columnComments: Seq[Option[String]],
  comment: Option[String],
  properties: Map[String, String],
  allowExisting: Boolean,
  replace: Boolean) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
//    val analyzedPlan = session.sessionState.executePlan(query, CommandExecutionMode.SKIP).analyzed
//
    val queryColumnNames = viewSchema.fieldNames
    SchemaUtils.checkColumnNameDuplication(queryColumnNames, SQLConf.get.resolver)

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
      if (catalog.viewExists(ident)) {
        catalog.dropView(ident)
      }
      // FIXME: replaceView API doesn't exist in Spark 3.5
      catalog.createView(
        ident,
        queryText,
        currentCatalog,
        currentNamespace,
        viewSchema,
        queryColumnNames,
        columnAliases.toArray,
        columnComments.map(c => c.orNull).toArray,
        newProperties.asJava)
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        catalog.createView(
          ident,
          queryText,
          currentCatalog,
          currentNamespace,
          viewSchema,
          queryColumnNames,
          columnAliases.toArray,
          columnComments.map(c => c.orNull).toArray,
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

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
   */
  private def aliasPlan(
    analyzedPlan: LogicalPlan,
    userSpecifiedColumns: Seq[(String, Option[String])]): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      val projectedPlan = Project(projectList, analyzedPlan)
      session.sessionState.executePlan(projectedPlan, CommandExecutionMode.SKIP).analyzed
    }
  }
}
