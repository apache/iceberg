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

import org.apache.iceberg.spark.source.SparkView
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.IcebergAnalysisException
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier
import org.apache.spark.sql.catalyst.analysis.ViewUtil
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.execution.command.CommandUtils
import scala.jdk.CollectionConverters._

/**
 * Executes ALTER VIEW UNSET TBLPROPERTIES for Spark V2 views.
 *
 * Uses a custom command instead of Spark's built-in implementation so Iceberg catalogs commit
 * property-only metadata updates and reject changes to reserved view properties.
 */
case class IcebergAlterV2ViewUnsetPropertiesExec(
    catalog: ViewCatalog,
    ident: Identifier,
    propertyKeys: Seq[String],
    ifExists: Boolean)
    extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    propertyKeys.foreach(verifyNonReservedPropertyIsUnset)

    val viewInfo = catalog.loadView(ident)
    val properties = viewInfo.properties.asScala.toMap

    if (!ifExists) {
      propertyKeys.filterNot(properties.contains).foreach { property =>
        throw new IcebergAnalysisException(s"Cannot remove property that is not set: '$property'")
      }
    }

    val view =
      ViewUtil
        .loadIcebergView(catalog, ident)
        .getOrElse(
          throw new IllegalStateException(s"Cannot load underlying Iceberg view for view: $ident"))
    val update = view.updateProperties()
    propertyKeys.foreach(update.remove)
    CommandUtils.uncacheTableOrView(session, ResolvedIdentifier(catalog, ident))
    update.commit()

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"IcebergAlterV2ViewUnsetProperties: ${ident}"
  }

  private def verifyNonReservedPropertyIsUnset(property: String): Unit = {
    if (SparkView.isReservedProperty(property)) {
      throw new UnsupportedOperationException(s"Cannot unset reserved property: '$property'")
    }
  }
}
