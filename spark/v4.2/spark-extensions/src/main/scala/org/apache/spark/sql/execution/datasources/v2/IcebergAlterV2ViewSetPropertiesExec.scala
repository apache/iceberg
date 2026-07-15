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
import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog

/**
 * Executes ALTER VIEW SET TBLPROPERTIES for Spark V2 views.
 *
 * Uses a custom command instead of Spark's built-in implementation so Iceberg catalogs commit
 * property-only metadata updates and reject changes to reserved view properties.
 */
case class IcebergAlterV2ViewSetPropertiesExec(
    catalog: ViewCatalog,
    ident: Identifier,
    properties: Map[String, String])
    extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    properties.keys.foreach(verifyNonReservedPropertyIsSet)

    val icebergViewCatalog =
      ViewUtil
        .icebergViewCatalog(catalog, ident)
        .getOrElse(
          throw new IllegalStateException(
            s"Cannot load underlying Iceberg view catalog for view: $ident"))
    val view = icebergViewCatalog.loadView(Spark3Util.identifierToTableIdentifier(ident))
    val update = view.updateProperties()
    properties.foreach { case (key, value) => update.set(key, value) }
    update.commit()

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"IcebergAlterV2ViewSetProperties: ${ident}"
  }

  private def verifyNonReservedPropertyIsSet(property: String): Unit = {
    if (SparkView.isReservedProperty(property)) {
      throw new UnsupportedOperationException(s"Cannot set reserved property: '$property'")
    }
  }
}
