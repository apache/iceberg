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

import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions
import org.apache.iceberg.spark.MaterializedViewUtil
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.view.View
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog


case class DropV2ViewExec(
  catalog: ViewCatalog,
  ident: Identifier,
  ifExists: Boolean) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    val icebergCatalog = catalog.asInstanceOf[SparkCatalog].icebergCatalog()
    val icebergViewCatalog = icebergCatalog.asInstanceOf[org.apache.iceberg.catalog.ViewCatalog]
    var view: Option[View] = None
    try {
      view = Some(icebergViewCatalog.loadView(TableIdentifier.of(Namespace.of(ident.namespace(): _*), ident.name())))
    } catch {
      case e: exceptions.NoSuchViewException => {
        if (!ifExists) {
          throw new NoSuchViewException(ident)
        }
      }
    }
    // if view is not null read the properties and check if it is a materialized view
    view match {
      case Some(v) => {
        val viewProperties = v.properties();
        if (Option(
            viewProperties.get(MaterializedViewUtil.MATERIALIZED_VIEW_PROPERTY_KEY
            )).getOrElse("false").equals("true")) {
          // get the storage table location then drop the storage table
          val storageTableLocation = viewProperties.get(
            MaterializedViewUtil.MATERIALIZED_VIEW_STORAGE_TABLE_PROPERTY_KEY
          )
          val storageTableIdentifier = Spark3Util.catalogAndIdentifier(
            SparkSession.active, storageTableLocation).identifier()
          // get active spark session
          catalog.asInstanceOf[SparkCatalog].dropTable(storageTableIdentifier)
        }
      }
      case _ =>
    }

    val dropped = catalog.dropView(ident)
    if (!dropped && !ifExists) {
      throw new NoSuchViewException(ident)
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"DropV2View: ${ident}"
  }
}
