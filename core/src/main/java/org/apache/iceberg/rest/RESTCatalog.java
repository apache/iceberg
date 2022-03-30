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
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalog implements Catalog, SupportsNamespaces, Configurable<Configuration>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalog.class);
  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private RESTClient client = null;
  private String catalogName = null;
  private Map<String, String> properties = null;
  private ResourcePaths paths = null;
  private Object conf = null;
  private FileIO io = null;

  public RESTCatalog() {
    this(new HTTPClientFactory());
  }

  RESTCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    ConfigResponse config = fetchConfig(props);
    Map<String, String> mergedProps = config.merge(props);
    this.client = clientBuilder.apply(mergedProps);
    this.catalogName = name;
    this.properties = mergedProps;
    this.paths = ResourcePaths.forCatalogProperties(properties);
    String ioImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.io = CatalogUtil.loadFileIO(ioImpl != null ? ioImpl : ResolvingFileIO.class.getName(), properties, conf);
  }

  public Map<String, String> properties() {
    return properties;
  }

  @Override
  public void setConf(Configuration newConf) {
    this.conf = newConf;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace ns) {
    ListTablesResponse response = client.get(
        paths.tables(ns), ListTablesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    // TODO: support purge flagN
    try {
      client.delete(paths.table(identifier), null, ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    RenameTableRequest request = RenameTableRequest.builder()
        .withSource(from)
        .withDestination(to)
        .build();

    // for now, ignore the response because there is no way to return it.
    client.post("v1/tables/rename", request, null, ErrorHandlers.tableErrorHandler());
  }

  private LoadTableResponse loadInternal(TableIdentifier identifier) {
    return client.get(paths.table(identifier), LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    LoadTableResponse response = loadInternal(identifier);
    Pair<RESTClient, FileIO> clients = tableClients(response.config());

    return new BaseTable(
        new RESTTableOperations(clients.first(), paths.table(identifier), clients.second(), response.tableMetadata()),
        fullTableName(identifier));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .setProperties(metadata)
        .build();

    // for now, ignore the response because there is no way to return it
    client.post(paths.namespaces(), request, CreateNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.isEmpty(), "Cannot list namespaces under parent: %s", namespace);
    // String joined = NULL.join(namespace.levels());
    ListNamespacesResponse response = client
        .get(paths.namespaces(), ListNamespacesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace ns) throws NoSuchNamespaceException {
    // TODO: rename to LoadNamespaceResponse?
    GetNamespaceResponse response = client
        .get(paths.namespace(ns), GetNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(Namespace ns) throws NamespaceNotEmptyException {
    try {
      client.delete(paths.namespace(ns), null, ErrorHandlers.namespaceErrorHandler());
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace ns, Map<String, String> props) throws NoSuchNamespaceException {
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .updateAll(props)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        paths.namespaceProperties(ns), request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public boolean removeProperties(Namespace ns, Set<String> props) throws NoSuchNamespaceException {
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .removeAll(props)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        paths.namespaceProperties(ns), request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.removed().isEmpty();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema);
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  private class Builder implements TableBuilder {
    private final TableIdentifier ident;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = null;
    private SortOrder writeOrder = null;
    private String location = null;

    private Builder(TableIdentifier ident, Schema schema) {
      this.ident = ident;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder tableWriteOrder) {
      this.writeOrder = tableWriteOrder;
      return this;
    }

    @Override
    public TableBuilder withLocation(String tableLocation) {
      this.location = tableLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> props) {
      this.propertiesBuilder.putAll(props);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      String ns = RESTUtil.encodeNamespace(ident.namespace());
      CreateTableRequest request = CreateTableRequest.builder()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(propertiesBuilder.build())
          .build();

      LoadTableResponse response = client.post(
          "v1/namespaces/" + ns + "/tables", request, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());

      Pair<RESTClient, FileIO> clients = tableClients(response.config());

      return new BaseTable(
          new RESTTableOperations(clients.first(), paths.table(ident), clients.second(), response.tableMetadata()),
          fullTableName(ident));
    }

    @Override
    public Transaction createTransaction() {
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      Pair<RESTClient, FileIO> clients = tableClients(response.config());
      TableMetadata meta = response.tableMetadata();

      RESTTableOperations ops = new RESTTableOperations(
          clients.first(), paths.table(ident), clients.second(),
          RESTTableOperations.UpdateType.CREATE, createChanges(meta), meta);

      return Transactions.createTableTransaction(fullName, ops, meta);
    }

    @Override
    public Transaction replaceTransaction() {
      LoadTableResponse response = loadInternal(ident);
      String fullName = fullTableName(ident);

      Pair<RESTClient, FileIO> clients = tableClients(response.config());
      TableMetadata base = response.tableMetadata();

      Map<String, String> tableProperties = propertiesBuilder.build();
      TableMetadata replacement = base.buildReplacement(
          schema,
          spec != null ? spec : PartitionSpec.unpartitioned(),
          writeOrder != null ? writeOrder : SortOrder.unsorted(),
          location != null ? location : base.location(),
          tableProperties);

      ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetCurrentSchema.class::isInstance)) {
        // ensure there is a change to set the current schema
        changes.add(new MetadataUpdate.SetCurrentSchema(replacement.currentSchemaId()));
      }

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetDefaultPartitionSpec.class::isInstance)) {
        // ensure there is a change to set the default spec
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(replacement.defaultSpecId()));
      }

      if (replacement.changes().stream().noneMatch(MetadataUpdate.SetDefaultSortOrder.class::isInstance)) {
        // ensure there is a change to set the default sort order
        changes.add(new MetadataUpdate.SetDefaultSortOrder(replacement.defaultSortOrderId()));
      }

      RESTTableOperations ops = new RESTTableOperations(
          clients.first(), paths.table(ident), clients.second(),
          RESTTableOperations.UpdateType.REPLACE, changes.build(), base);

      return Transactions.replaceTableTransaction(fullName, ops, replacement);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // return a create or a replace transaction, depending on whether the table exists
      // deciding whether to create or replace can't be determined on the service because schema field IDs are assigned
      // at this point and then used in data and metadata files. because create and replace will assign different
      // field IDs, they must be determined before any writes occur
      try {
        return replaceTransaction();
      } catch (NoSuchTableException e) {
        return createTransaction();
      }
    }

    private LoadTableResponse stageCreate() {
      Map<String, String> tableProperties = propertiesBuilder.build();

      CreateTableRequest request = CreateTableRequest.builder()
          .stageCreate()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(tableProperties)
          .build();

      return client.post(
          paths.tables(ident.namespace()), request, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
    }
  }

  private static List<MetadataUpdate> createChanges(TableMetadata meta) {
    ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

    Schema schema = meta.schema();
    changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
    changes.add(new MetadataUpdate.SetCurrentSchema(-1));

    PartitionSpec spec = meta.spec();
    if (spec != null && spec.isPartitioned()) {
      changes.add(new MetadataUpdate.AddPartitionSpec(spec));
      changes.add(new MetadataUpdate.SetDefaultPartitionSpec(-1));
    }

    SortOrder order = meta.sortOrder();
    if (order != null && order.isSorted()) {
      changes.add(new MetadataUpdate.AddSortOrder(order));
      changes.add(new MetadataUpdate.SetDefaultSortOrder(-1));
    }

    String location = meta.location();
    if (location != null) {
      changes.add(new MetadataUpdate.SetLocation(location));
    }

    Map<String, String> properties = meta.properties();
    if (properties != null && !properties.isEmpty()) {
      changes.add(new MetadataUpdate.SetProperties(properties));
    }

    return changes.build();
  }

  private String fullTableName(TableIdentifier ident) {
    return String.format("%s.%s", catalogName, ident);
  }

  private Map<String, String> fullConf(Map<String, String> config) {
    Map<String, String> fullConf = Maps.newHashMap(properties);
    fullConf.putAll(config);
    return fullConf;
  }

  private Pair<RESTClient, FileIO> tableClients(Map<String, String> config) {
    if (config.isEmpty()) {
      return Pair.of(client, io); // reuse client and io since config is the same
    }

    Map<String, String> fullConf = fullConf(config);
    String ioImpl = fullConf.get(CatalogProperties.FILE_IO_IMPL);
    FileIO tableIO = CatalogUtil.loadFileIO(
        ioImpl != null ? ioImpl : ResolvingFileIO.class.getName(), fullConf, this.conf);
    RESTClient tableClient = clientBuilder.apply(fullConf);

    return Pair.of(tableClient, tableIO);
  }

  private ConfigResponse fetchConfig(Map<String, String> props) {
    // Create a client for one time use, as we will reconfigure the client using the merged server and application
    // defined configuration.
    RESTClient singleUseClient = clientBuilder.apply(props);

    try {
      ConfigResponse configResponse = singleUseClient
          .get(ResourcePaths.config(), ConfigResponse.class, ErrorHandlers.defaultErrorHandler());
      configResponse.validate();
      return configResponse;
    } finally {
      try {
        singleUseClient.close();
      } catch (IOException e) {
        LOG.error("Failed to close HTTP client used for getting catalog configuration. Possible resource leak.", e);
      }
    }
  }
}
