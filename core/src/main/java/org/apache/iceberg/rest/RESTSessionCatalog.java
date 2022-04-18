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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import org.apache.iceberg.catalog.BaseSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
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

public class RESTSessionCatalog extends BaseSessionCatalog implements Configurable<Configuration>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTSessionCatalog.class);
  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private final Cache<String, Map<String, String>> headers = Caffeine.newBuilder().build();
  private Map<String, String> baseHeaders = ImmutableMap.of();
  private RESTClient client = null;
  private ResourcePaths paths = null;
  private Object conf = null;
  private FileIO io = null;

  public RESTSessionCatalog() {
    this(new HTTPClientFactory());
  }

  RESTSessionCatalog(Function<Map<String, String>, RESTClient> clientBuilder) {
    this.clientBuilder = clientBuilder;
  }

  @Override
  public void initialize(String name, Map<String, String> props) {
    Preconditions.checkArgument(props != null, "Invalid configuration: null");
    this.baseHeaders = RESTUtil.extractPrefixMap(props, "header.");
    ConfigResponse config = fetchConfig(props);
    Map<String, String> mergedProps = config.merge(props);
    this.client = clientBuilder.apply(mergedProps);
    super.initialize(name, mergedProps);
    this.paths = ResourcePaths.forCatalogProperties(mergedProps);
    String ioImpl = mergedProps.get(CatalogProperties.FILE_IO_IMPL);
    this.io = CatalogUtil.loadFileIO(ioImpl != null ? ioImpl : ResolvingFileIO.class.getName(), mergedProps, conf);
  }

  Map<String, String> headers(SessionContext context) {
    return headers.get(context.sessionId(), id -> {
      ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
      builder.putAll(baseHeaders);

      if (context.identity() != null) {
        builder.put("x-iceberg-identity", context.identity());
      }

      if (context.credential() != null) {
        builder.put("x-iceberg-credential", context.credential());
      }

      return builder.build();
    });
  }

  @Override
  public void setConf(Configuration newConf) {
    this.conf = newConf;
  }

  @Override
  public List<TableIdentifier> listTables(SessionContext context, Namespace ns) {
    ListTablesResponse response = client.get(
        paths.tables(ns), ListTablesResponse.class, headers(context), ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(SessionContext context, TableIdentifier identifier) {
    try {
      client.delete(paths.table(identifier), null, headers(context), ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public boolean purgeTable(SessionContext context, TableIdentifier ident) {
    throw new UnsupportedOperationException("Purge is not supported");
  }

  @Override
  public void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Rename is not supported");
  }

  private LoadTableResponse loadInternal(SessionContext context, TableIdentifier identifier) {
    return client.get(
        paths.table(identifier), LoadTableResponse.class, headers(context), ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(SessionContext context, TableIdentifier identifier) {
    LoadTableResponse response = loadInternal(context, identifier);
    Pair<RESTClient, FileIO> clients = tableClients(response.config());
    RESTTableOperations ops = new RESTTableOperations(
        clients.first(), paths.table(identifier), headers(context), clients.second(), response.tableMetadata());
    return new BaseTable(ops, fullTableName(identifier));
  }

  @Override
  public Catalog.TableBuilder buildTable(SessionContext context, TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema, context);
  }

  @Override
  public void invalidateTable(SessionContext context, TableIdentifier ident) {
  }

  @Override
  public Table registerTable(SessionContext context, TableIdentifier ident, String metadataFileLocation) {
    throw new UnsupportedOperationException("Register table is not supported");
  }

  @Override
  public void createNamespace(SessionContext context, Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .setProperties(metadata)
        .build();

    // for now, ignore the response because there is no way to return it
    client.post(
        paths.namespaces(), request, CreateNamespaceResponse.class, headers(context),
        ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(SessionContext context, Namespace namespace) {
    Preconditions.checkArgument(namespace.isEmpty(), "Cannot list namespaces under parent: %s", namespace);
    // String joined = NULL.join(namespace.levels());
    ListNamespacesResponse response = client
        .get(paths.namespaces(), ListNamespacesResponse.class, headers(context), ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace ns) {
    // TODO: rename to LoadNamespaceResponse?
    GetNamespaceResponse response = client
        .get(paths.namespace(ns), GetNamespaceResponse.class, headers(context), ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(SessionContext context, Namespace ns) {
    try {
      client.delete(paths.namespace(ns), null, headers(context), ErrorHandlers.namespaceErrorHandler());
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean updateNamespaceMetadata(SessionContext context, Namespace ns,
                                         Map<String, String> updates, Set<String> removals) {
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .updateAll(updates)
        .removeAll(removals)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        paths.namespaceProperties(ns), request, UpdateNamespacePropertiesResponse.class, headers(context),
        ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  private class Builder implements Catalog.TableBuilder {
    private final TableIdentifier ident;
    private final Schema schema;
    private final SessionContext context;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = null;
    private SortOrder writeOrder = null;
    private String location = null;

    private Builder(TableIdentifier ident, Schema schema, SessionContext context) {
      this.ident = ident;
      this.schema = schema;
      this.context = context;
    }

    @Override
    public Builder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      return this;
    }

    @Override
    public Builder withSortOrder(SortOrder tableWriteOrder) {
      this.writeOrder = tableWriteOrder;
      return this;
    }

    @Override
    public Builder withLocation(String tableLocation) {
      this.location = tableLocation;
      return this;
    }

    @Override
    public Builder withProperties(Map<String, String> props) {
      this.propertiesBuilder.putAll(props);
      return this;
    }

    @Override
    public Builder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      CreateTableRequest request = CreateTableRequest.builder()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(propertiesBuilder.build())
          .build();

      LoadTableResponse response = client.post(
          paths.tables(ident.namespace()), request, LoadTableResponse.class, headers(context),
          ErrorHandlers.tableErrorHandler());

      Pair<RESTClient, FileIO> clients = tableClients(response.config());
      RESTTableOperations ops = new RESTTableOperations(
          clients.first(), paths.table(ident), headers(context), clients.second(), response.tableMetadata());

      return new BaseTable(ops, fullTableName(ident));
    }

    @Override
    public Transaction createTransaction() {
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      Pair<RESTClient, FileIO> clients = tableClients(response.config());
      TableMetadata meta = response.tableMetadata();

      RESTTableOperations ops = new RESTTableOperations(
          clients.first(), paths.table(ident), headers(context), clients.second(),
          RESTTableOperations.UpdateType.CREATE, createChanges(meta), meta);

      return Transactions.createTableTransaction(fullName, ops, meta);
    }

    @Override
    public Transaction replaceTransaction() {
      LoadTableResponse response = loadInternal(context, ident);
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
          clients.first(), paths.table(ident), headers(context), clients.second(),
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
          paths.tables(ident.namespace()), request, LoadTableResponse.class, headers(context),
          ErrorHandlers.tableErrorHandler());
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
    return String.format("%s.%s", name(), ident);
  }

  private Map<String, String> fullConf(Map<String, String> config) {
    Map<String, String> fullConf = Maps.newHashMap(properties());
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
          .get(ResourcePaths.config(), ConfigResponse.class, baseHeaders, ErrorHandlers.defaultErrorHandler());
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
