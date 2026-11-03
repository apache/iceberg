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
package org.apache.iceberg.spark;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableCatalogCapability;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.TableInfo;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The catalog view Spark plans against for the duration of a {@link SparkTransaction}.
 *
 * <p>Table loads resolve to the transaction's staged Iceberg state. All other operations delegate
 * to the underlying {@link SparkCatalog}.
 */
class TransactionSparkCatalog implements TableCatalog {

  private final SparkCatalog delegate;
  private final SparkTransaction transaction;

  TransactionSparkCatalog(SparkCatalog delegate, SparkTransaction transaction) {
    this.delegate = delegate;
    this.transaction = transaction;
  }

  @Override
  public Table loadTable(Identifier ident) {
    return SparkTable.create(transaction.transactionFor(ident).table(), (TimeTravel) null);
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    delegate.initialize(name, options);
  }

  @Override
  public Set<TableCatalogCapability> capabilities() {
    return delegate.capabilities();
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    return delegate.listTables(namespace);
  }

  @Override
  public void invalidateTable(Identifier ident) {
    delegate.invalidateTable(ident);
  }

  @Override
  public boolean tableExists(Identifier ident) {
    return delegate.tableExists(ident);
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return delegate.createTable(ident, schema, partitions, properties);
  }

  @Override
  public Table createTable(
      Identifier ident, Column[] columns, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return delegate.createTable(ident, columns, partitions, properties);
  }

  @Override
  public Table createTable(Identifier ident, TableInfo tableInfo)
      throws TableAlreadyExistsException, NoSuchNamespaceException {
    return delegate.createTable(ident, tableInfo);
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    return delegate.alterTable(ident, changes);
  }

  @Override
  public boolean dropTable(Identifier ident) {
    return delegate.dropTable(ident);
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {
    delegate.renameTable(oldIdent, newIdent);
  }
}
