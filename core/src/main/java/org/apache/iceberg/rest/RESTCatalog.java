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
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

public class RESTCatalog implements Catalog, SupportsNamespaces, Configurable<Configuration>, Closeable {
  private final RESTSessionCatalog sessionCatalog;
  private final SessionCatalog.SessionContext context;
  private final Catalog delegate;
  private final SupportsNamespaces nsDelegate;
  private String name;
  private Map<String, String> properties;

  public RESTCatalog() {
    this(SessionCatalog.SessionContext.createEmpty(), new HTTPClientFactory());
  }

  public RESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this(SessionCatalog.SessionContext.createEmpty(), clientBuilder);
  }

  public RESTCatalog(SessionCatalog.SessionContext context, Function<Map<String, String>, RESTClient> clientBuilder) {
    this.sessionCatalog = new RESTSessionCatalog(clientBuilder);
    this.context = context;
    this.delegate = sessionCatalog.asCatalog(context);
    this.nsDelegate = (SupportsNamespaces) delegate;
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    Preconditions.checkArgument(props != null, "Invalid configuration: null");

    this.name = name;

    Map<String, String> contextHeaders = sessionCatalog.headers(context);
    ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
    builder.putAll(props);
    contextHeaders.forEach((header, value) -> builder.put("header." + header, value));
    this.properties = builder.build();

    sessionCatalog.initialize(name, properties);
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
  public boolean tableExists(TableIdentifier identifier) {
    return delegate.tableExists(identifier);
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
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location,
                           Map<String, String> properties) {
    return delegate.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                           Map<String, String> properties) {
    return delegate.createTable(identifier, schema, spec, properties);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return delegate.createTable(identifier, schema, spec);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema) {
    return delegate.createTable(identifier, schema);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                               String location, Map<String, String> properties) {
    return delegate.newCreateTableTransaction(identifier, schema, spec, location, properties);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                               Map<String, String> properties) {
    return delegate.newCreateTableTransaction(identifier, schema, spec, properties);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    return delegate.newCreateTableTransaction(identifier, schema, spec);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema) {
    return delegate.newCreateTableTransaction(identifier, schema);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                String location, Map<String, String> properties, boolean orCreate) {
    return delegate.newReplaceTableTransaction(identifier, schema, spec, location, properties, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                Map<String, String> properties, boolean orCreate) {
    return delegate.newReplaceTableTransaction(identifier, schema, spec, properties, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
                                                boolean orCreate) {
    return delegate.newReplaceTableTransaction(identifier, schema, spec, orCreate);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, boolean orCreate) {
    return delegate.newReplaceTableTransaction(identifier, schema, orCreate);
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
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    return delegate.registerTable(identifier, metadataFileLocation);
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
  public boolean setProperties(Namespace ns, Map<String, String> props) throws NoSuchNamespaceException {
    return nsDelegate.setProperties(ns, props);
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    return nsDelegate.removeProperties(ns, props);
  }

  @Override
  public void setConf(Configuration conf) {
    sessionCatalog.setConf(conf);
  }

  @Override
  public void close() throws IOException {
    sessionCatalog.close();
  }
}
