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

import org.apache.iceberg.catalog.LoadContext
import org.apache.iceberg.spark.SparkSupportsLoadContext
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.util.CaseInsensitiveStringMap

/**
 * Catalog for a table read through a view, so that reloads keep the view chain. Spark's table
 * version refresh before execution reloads tables by identifier only, using the relation's
 * catalog. Only read operations are supported.
 */
private[analysis] case class LoadContextTableCatalog(
    delegate: TableCatalog with SparkSupportsLoadContext,
    context: LoadContext)
    extends TableCatalog {

  override def name(): String = delegate.name()

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def loadTable(ident: Identifier): Table = delegate.loadTable(ident, context)

  override def tableExists(ident: Identifier): Boolean = delegate.tableExists(ident)

  override def invalidateTable(ident: Identifier): Unit = delegate.invalidateTable(ident)

  override def listTables(namespace: Array[String]): Array[Identifier] =
    delegate.listTables(namespace)

  override def alterTable(ident: Identifier, changes: TableChange*): Table =
    throw new UnsupportedOperationException(s"Cannot alter table $ident through a view relation")

  override def dropTable(ident: Identifier): Boolean =
    throw new UnsupportedOperationException(s"Cannot drop table $ident through a view relation")

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit =
    throw new UnsupportedOperationException(
      s"Cannot rename table $oldIdent through a view relation")
}
