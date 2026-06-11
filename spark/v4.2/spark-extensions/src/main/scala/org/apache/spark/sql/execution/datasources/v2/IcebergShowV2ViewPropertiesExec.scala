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
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.execution.LeafExecNode
import scala.jdk.CollectionConverters._

/**
 * Executes SHOW TBLPROPERTIES for Spark V2 views.
 *
 * Uses a custom command instead of Spark's built-in implementation so Iceberg reserved metadata is
 * filtered from user-visible view properties.
 */
case class IcebergShowV2ViewPropertiesExec(
    output: Seq[Attribute],
    catalogName: String,
    ident: Identifier,
    view: View,
    propertyKey: Option[String])
    extends V2CommandExec
    with LeafExecNode {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def run(): Seq[InternalRow] = {
    propertyKey match {
      case Some(p) =>
        val propValue =
          properties.getOrElse(p, s"View ${catalogName}.${ident.quoted} does not have property: $p")
        Seq(toCatalystRow(p, propValue))
      case None =>
        properties.map { case (k, v) =>
          toCatalystRow(k, v)
        }.toSeq
    }
  }

  private def properties = {
    view.properties.asScala.toMap -- ViewUtil.RESERVED_PROPERTIES
  }

  override def simpleString(maxFields: Int): String = {
    s"IcebergShowV2ViewPropertiesExec"
  }
}
