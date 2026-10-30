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

import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.View
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors

object ViewUtil {
  def loadView(catalog: CatalogPlugin, ident: Identifier): Option[View] = catalog match {
    case viewCatalog: ViewCatalog =>
      try {
        Option(viewCatalog.loadView(ident))
      } catch {
        case _: NoSuchViewException => None
      }
    case _ => None
  }

  def isViewCatalog(catalog: CatalogPlugin): Boolean = {
    catalog.isInstanceOf[ViewCatalog]
  }

  implicit class IcebergViewHelper(plugin: CatalogPlugin) {
    def asViewCatalog: ViewCatalog = plugin match {
      case viewCatalog: ViewCatalog =>
        viewCatalog
      case _ =>
        throw QueryCompilationErrors.missingCatalogAbilityError(plugin, "views")
    }
  }
}
