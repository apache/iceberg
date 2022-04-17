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

package org.apache.iceberg.rest;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;

abstract class DelegatingCatalog<C extends Catalog & SupportsNamespaces>
    implements Catalog, SupportsNamespaces, Closeable {
  private interface Task<T> {
    T run();
  }

  private final C delegate;

  DelegatingCatalog(C delegate) {
    this.delegate = delegate;
  }

  protected abstract void before();

  protected abstract void after();

  private <T> T wrap(Task<T> task) {
    before();
    try {
      return task.run();
    } finally {
      after();
    }
  }

  private void wrap(Runnable task) {
    before();
    try {
      task.run();
    } finally {
      after();
    }
  }

  @Override
  public void close() throws IOException {
    if (delegate instanceof Closeable) {
      ((Closeable) delegate).close();
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    wrap(() -> delegate.initialize(name, properties));
  }

  @Override
  public String name() {
    return delegate.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace ns) {
    return wrap(() -> delegate.listTables(ns));
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return wrap(() -> delegate.tableExists(identifier));
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    return wrap(() -> delegate.loadTable(ident));
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    wrap(() -> delegate.invalidateTable(ident));
  }

  @Override
  public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
    return wrap(() -> delegate.buildTable(ident, schema));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location,
                           Map<String, String> properties) {
    return wrap(() -> delegate.createTable(identifier, schema, spec, location, properties));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                           Map<String, String> properties) {
    return wrap(() -> delegate.createTable(identifier, schema, spec, properties));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return wrap(() -> delegate.createTable(identifier, schema, spec));
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return wrap(() -> delegate.createTable(identifier, schema));
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                               String location, Map<String, String> properties) {
    return wrap(() -> delegate.newCreateTableTransaction(identifier, schema, spec, location, properties));
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                               Map<String, String> properties) {
    return wrap(() -> delegate.newCreateTableTransaction(identifier, schema, spec, properties));
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return wrap(() -> delegate.newCreateTableTransaction(identifier, schema, spec));
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return wrap(() -> delegate.newCreateTableTransaction(identifier, schema));
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                String location, Map<String, String> properties, boolean orCreate) {
    return wrap(() -> delegate.newReplaceTableTransaction(identifier, schema, spec, location, properties, orCreate));
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                Map<String, String> properties, boolean orCreate) {
    return wrap(() -> delegate.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate));
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                boolean orCreate) {
    return wrap(() -> delegate.newReplaceTableTransaction(identifier, schema, spec, orCreate));
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, boolean orCreate) {
    return wrap(() -> delegate.newReplaceTableTransaction(identifier, schema, orCreate));
  }

  @Override
  public boolean dropTable(TableIdentifier ident) {
    return wrap(() -> delegate.dropTable(ident));
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    return wrap(() -> delegate.dropTable(ident, purge));
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    wrap(() -> delegate.renameTable(from, to));
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    return wrap(() -> delegate.registerTable(identifier, metadataFileLocation));
  }

  @Override
  public void createNamespace(Namespace ns, Map<String, String> props) {
    wrap(() -> delegate.createNamespace(ns, props));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace ns) throws NoSuchNamespaceException {
    return wrap(() -> delegate.listNamespaces(ns));
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
    return wrap(() -> delegate.loadNamespaceMetadata(ns));
  }

  @Override
  public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
    return wrap(() -> delegate.dropNamespace(ns));
  }

  @Override
  public boolean setProperties(Namespace ns, Map<String, String> props) throws NoSuchNamespaceException {
    return wrap(() -> delegate.setProperties(ns, props));
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    return wrap(() -> delegate.removeProperties(ns, props));
  }
}
