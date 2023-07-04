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
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public class RESTCatalog implements Catalog, SupportsNamespaces, Configurable<Object>, Closeable {
  private final RESTSessionCatalog sessionCatalog;
  private final Catalog delegate;
  private final SupportsNamespaces nsDelegate;
  private final SessionCatalog.SessionContext context;

  public RESTCatalog() {
    this(
        SessionCatalog.SessionContext.createEmpty(),
        config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build());
  }

  public RESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this(SessionCatalog.SessionContext.createEmpty(), clientBuilder);
  }

  public RESTCatalog(
      SessionCatalog.SessionContext context,
      Function<Map<String, String>, RESTClient> clientBuilder) {
    this.sessionCatalog = new RESTSessionCatalog(clientBuilder, null);
    this.delegate = sessionCatalog.asCatalog(context);
    this.nsDelegate = (SupportsNamespaces) delegate;
    this.context = context;
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    Preconditions.checkArgument(props != null, "Invalid configuration: null");
    sessionCatalog.initialize(name, props);
  }

  @Override
  public String name() {
    return sessionCatalog.name();
  }

  public Map<String, String> properties() {
    return sessionCatalog.properties();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace ns) {
    return delegate.listTables(ns);
  }

  @Override
  public boolean tableExists(TableIdentifier ident) {
    return delegate.tableExists(ident);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    return delegate.loadTable(ident);
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    delegate.invalidateTable(ident);
  }

  @Override
  public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
    return delegate.buildTable(ident, schema);
  }

  @Override
  public Table createTable(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return delegate.createTable(ident, schema, spec, location, props);
  }

  @Override
  public Table createTable(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return delegate.createTable(ident, schema, spec, props);
  }

  @Override
  public Table createTable(TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return delegate.createTable(ident, schema, spec);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return delegate.createTable(identifier, schema);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props) {
    return delegate.newCreateTableTransaction(ident, schema, spec, location, props);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, Map<String, String> props) {
    return delegate.newCreateTableTransaction(ident, schema, spec, props);
  }

  @Override
  public Transaction newCreateTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec) {
    return delegate.newCreateTableTransaction(ident, schema, spec);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return delegate.newCreateTableTransaction(identifier, schema);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> props,
      boolean orCreate) {
    return delegate.newReplaceTableTransaction(ident, schema, spec, location, props, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> props,
      boolean orCreate) {
    return delegate.newReplaceTableTransaction(ident, schema, spec, props, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, PartitionSpec spec, boolean orCreate) {
    return delegate.newReplaceTableTransaction(ident, schema, spec, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(
      TableIdentifier ident, Schema schema, boolean orCreate) {
    return delegate.newReplaceTableTransaction(ident, schema, orCreate);
  }

  @Override
  public boolean dropTable(TableIdentifier ident) {
    return delegate.dropTable(ident);
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    return delegate.dropTable(ident, purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    delegate.renameTable(from, to);
  }

  @Override
  public Table registerTable(TableIdentifier ident, String metadataFileLocation) {
    return delegate.registerTable(ident, metadataFileLocation);
  }

  @Override
  public void createNamespace(Namespace ns, Map<String, String> props) {
    nsDelegate.createNamespace(ns, props);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace ns) throws NoSuchNamespaceException {
    return nsDelegate.listNamespaces(ns);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
    return nsDelegate.loadNamespaceMetadata(ns);
  }

  @Override
  public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
    return nsDelegate.dropNamespace(ns);
  }

  @Override
  public boolean setProperties(Namespace ns, Map<String, String> props)
      throws NoSuchNamespaceException {
    return nsDelegate.setProperties(ns, props);
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    return nsDelegate.removeProperties(ns, props);
  }

  @Override
  public void setConf(Object conf) {
    sessionCatalog.setConf(conf);
  }

  @Override
  public void close() throws IOException {
    sessionCatalog.close();
  }

  public void commitTransaction(List<TableCommit> commits) {
    sessionCatalog.commitTransaction(context, commits);
  }

  public void commitTransaction(TableCommit... commits) {
    sessionCatalog.commitTransaction(
        context, ImmutableList.<TableCommit>builder().add(commits).build());
  }
}
