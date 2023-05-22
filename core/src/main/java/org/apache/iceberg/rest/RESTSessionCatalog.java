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
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.BaseSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.auth.OAuth2Util.AuthSession;
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
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTSessionCatalog extends BaseSessionCatalog
    implements Configurable<Object>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTSessionCatalog.class);
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";
  private static final String REST_METRICS_REPORTING_ENABLED = "rest-metrics-reporting-enabled";
  private static final String REST_SNAPSHOT_LOADING_MODE = "snapshot-loading-mode";
  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private final BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder;
  private Cache<String, AuthSession> sessions = null;
  private Cache<TableOperations, FileIO> fileIOCloser;
  private AuthSession catalogAuth = null;
  private boolean keepTokenRefreshed = true;
  private RESTClient client = null;
  private ResourcePaths paths = null;
  private SnapshotMode snapshotMode = null;
  private Object conf = null;
  private FileIO io = null;
  private MetricsReporter reporter = null;
  private boolean reportingViaRestEnabled;
  private CloseableGroup closeables = null;

  // a lazy thread pool for token refresh
  private volatile ScheduledExecutorService refreshExecutor = null;

  enum SnapshotMode {
    ALL,
    REFS;

    Map<String, String> params() {
      return ImmutableMap.of("snapshots", this.name().toLowerCase(Locale.US));
    }
  }

  public RESTSessionCatalog() {
    this(config -> HTTPClient.builder(config).uri(config.get(CatalogProperties.URI)).build(), null);
  }

  public RESTSessionCatalog(
      Function<Map<String, String>, RESTClient> clientBuilder,
      BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder) {
    Preconditions.checkNotNull(clientBuilder, "Invalid client builder: null");
    this.clientBuilder = clientBuilder;
    this.ioBuilder = ioBuilder;
  }

  @Override
  public void initialize(String name, Map<String, String> unresolved) {
    Preconditions.checkArgument(unresolved != null, "Invalid configuration: null");
    // resolve any configuration that is supplied by environment variables
    // note that this is only done for local config properties and not for properties from the
    // catalog service
    Map<String, String> props = EnvironmentUtil.resolveAll(unresolved);

    long startTimeMillis =
        System.currentTimeMillis(); // keep track of the init start time for token refresh
    String initToken = props.get(OAuth2Properties.TOKEN);

    // fetch auth and config to complete initialization
    ConfigResponse config;
    OAuthTokenResponse authResponse;
    String credential = props.get(OAuth2Properties.CREDENTIAL);
    String scope = props.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    try (RESTClient initClient = clientBuilder.apply(props)) {
      Map<String, String> initHeaders =
          RESTUtil.merge(configHeaders(props), OAuth2Util.authHeaders(initToken));
      if (credential != null && !credential.isEmpty()) {
        authResponse = OAuth2Util.fetchToken(initClient, initHeaders, credential, scope);
        Map<String, String> authHeaders =
            RESTUtil.merge(initHeaders, OAuth2Util.authHeaders(authResponse.token()));
        config = fetchConfig(initClient, authHeaders, props);
      } else {
        authResponse = null;
        config = fetchConfig(initClient, initHeaders, props);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close HTTP client", e);
    }

    // build the final configuration and set up the catalog's auth
    Map<String, String> mergedProps = config.merge(props);
    Map<String, String> baseHeaders = configHeaders(mergedProps);

    this.sessions = newSessionCache(mergedProps);
    this.keepTokenRefreshed =
        PropertyUtil.propertyAsBoolean(
            mergedProps,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);
    this.client = clientBuilder.apply(mergedProps);
    this.paths = ResourcePaths.forCatalogProperties(mergedProps);

    String token = mergedProps.get(OAuth2Properties.TOKEN);
    this.catalogAuth = new AuthSession(baseHeaders, null, null, credential, scope);
    if (authResponse != null) {
      this.catalogAuth =
          AuthSession.fromTokenResponse(
              client, tokenRefreshExecutor(), authResponse, startTimeMillis, catalogAuth);
    } else if (token != null) {
      this.catalogAuth =
          AuthSession.fromAccessToken(
              client, tokenRefreshExecutor(), token, expiresAtMillis(mergedProps), catalogAuth);
    }

    this.io = newFileIO(SessionContext.createEmpty(), mergedProps);

    this.fileIOCloser = newFileIOCloser();
    this.closeables = new CloseableGroup();
    this.closeables.addCloseable(this.io);
    this.closeables.addCloseable(this.client);
    this.closeables.setSuppressCloseFailure(true);

    this.snapshotMode =
        SnapshotMode.valueOf(
            PropertyUtil.propertyAsString(
                    mergedProps, REST_SNAPSHOT_LOADING_MODE, SnapshotMode.ALL.name())
                .toUpperCase(Locale.US));

    this.reporter = CatalogUtil.loadMetricsReporter(mergedProps);

    this.reportingViaRestEnabled =
        PropertyUtil.propertyAsBoolean(mergedProps, REST_METRICS_REPORTING_ENABLED, true);
    super.initialize(name, mergedProps);
  }

  private AuthSession session(SessionContext context) {
    AuthSession session =
        sessions.get(
            context.sessionId(),
            id -> newSession(context.credentials(), context.properties(), catalogAuth));

    return session != null ? session : catalogAuth;
  }

  private Supplier<Map<String, String>> headers(SessionContext context) {
    return session(context)::headers;
  }

  @Override
  public void setConf(Object newConf) {
    this.conf = newConf;
  }

  @Override
  public List<TableIdentifier> listTables(SessionContext context, Namespace ns) {
    checkNamespaceIsValid(ns);

    ListTablesResponse response =
        client.get(
            paths.tables(ns),
            ListTablesResponse.class,
            headers(context),
            ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);

    try {
      client.delete(
          paths.table(identifier), null, headers(context), ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public boolean purgeTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);

    try {
      client.delete(
          paths.table(identifier),
          ImmutableMap.of("purgeRequested", "true"),
          null,
          headers(context),
          ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to) {
    checkIdentifierIsValid(from);
    checkIdentifierIsValid(to);

    RenameTableRequest request =
        RenameTableRequest.builder().withSource(from).withDestination(to).build();

    // for now, ignore the response because there is no way to return it
    client.post(paths.rename(), request, null, headers(context), ErrorHandlers.tableErrorHandler());
  }

  private LoadTableResponse loadInternal(
      SessionContext context, TableIdentifier identifier, SnapshotMode mode) {
    return client.get(
        paths.table(identifier),
        mode.params(),
        LoadTableResponse.class,
        headers(context),
        ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(SessionContext context, TableIdentifier identifier) {
    checkIdentifierIsValid(identifier);

    MetadataTableType metadataType;
    LoadTableResponse response;
    TableIdentifier loadedIdent;
    try {
      response = loadInternal(context, identifier, snapshotMode);
      loadedIdent = identifier;
      metadataType = null;

    } catch (NoSuchTableException original) {
      metadataType = MetadataTableType.from(identifier.name());
      if (metadataType != null) {
        // attempt to load a metadata table using the identifier's namespace as the base table
        TableIdentifier baseIdent = TableIdentifier.of(identifier.namespace().levels());
        try {
          response = loadInternal(context, baseIdent, snapshotMode);
          loadedIdent = baseIdent;
        } catch (NoSuchTableException ignored) {
          // the base table does not exist
          throw original;
        }
      } else {
        // name is not a metadata table
        throw original;
      }
    }

    TableIdentifier finalIdentifier = loadedIdent;
    AuthSession session = tableSession(response.config(), session(context));
    TableMetadata tableMetadata;

    if (snapshotMode == SnapshotMode.REFS) {
      tableMetadata =
          TableMetadata.buildFrom(response.tableMetadata())
              .withMetadataLocation(response.metadataLocation())
              .setPreviousFileLocation(null)
              .setSnapshotsSupplier(
                  () ->
                      loadInternal(context, finalIdentifier, SnapshotMode.ALL)
                          .tableMetadata()
                          .snapshots())
              .discardChanges()
              .build();
    } else {
      tableMetadata = response.tableMetadata();
    }

    RESTTableOperations ops =
        new RESTTableOperations(
            client,
            paths.table(finalIdentifier),
            session::headers,
            tableFileIO(context, response.config()),
            tableMetadata);

    trackFileIO(ops);

    BaseTable table =
        new BaseTable(
            ops,
            fullTableName(finalIdentifier),
            metricsReporter(paths.metrics(finalIdentifier), session::headers));
    if (metadataType != null) {
      return MetadataTableUtils.createMetadataTableInstance(table, metadataType);
    }

    return table;
  }

  private void trackFileIO(RESTTableOperations ops) {
    if (io != ops.io()) {
      fileIOCloser.put(ops, ops.io());
    }
  }

  private MetricsReporter metricsReporter(
      String metricsEndpoint, Supplier<Map<String, String>> headers) {
    if (reportingViaRestEnabled) {
      RESTMetricsReporter restMetricsReporter =
          new RESTMetricsReporter(client, metricsEndpoint, headers);
      return MetricsReporters.combine(reporter, restMetricsReporter);
    } else {
      return this.reporter;
    }
  }

  @Override
  public Catalog.TableBuilder buildTable(
      SessionContext context, TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema, context);
  }

  @Override
  public void invalidateTable(SessionContext context, TableIdentifier ident) {}

  @Override
  public Table registerTable(
      SessionContext context, TableIdentifier ident, String metadataFileLocation) {
    throw new UnsupportedOperationException("Register table is not supported");
  }

  @Override
  public void createNamespace(
      SessionContext context, Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).setProperties(metadata).build();

    // for now, ignore the response because there is no way to return it
    client.post(
        paths.namespaces(),
        request,
        CreateNamespaceResponse.class,
        headers(context),
        ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(SessionContext context, Namespace namespace) {
    Map<String, String> queryParams;
    if (namespace.isEmpty()) {
      queryParams = ImmutableMap.of();
    } else {
      // query params should be unescaped
      queryParams = ImmutableMap.of("parent", RESTUtil.NAMESPACE_JOINER.join(namespace.levels()));
    }

    ListNamespacesResponse response =
        client.get(
            paths.namespaces(),
            queryParams,
            ListNamespacesResponse.class,
            headers(context),
            ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace ns) {
    checkNamespaceIsValid(ns);

    // TODO: rename to LoadNamespaceResponse?
    GetNamespaceResponse response =
        client.get(
            paths.namespace(ns),
            GetNamespaceResponse.class,
            headers(context),
            ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(SessionContext context, Namespace ns) {
    checkNamespaceIsValid(ns);

    try {
      client.delete(
          paths.namespace(ns), null, headers(context), ErrorHandlers.namespaceErrorHandler());
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean updateNamespaceMetadata(
      SessionContext context, Namespace ns, Map<String, String> updates, Set<String> removals) {
    checkNamespaceIsValid(ns);

    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();

    UpdateNamespacePropertiesResponse response =
        client.post(
            paths.namespaceProperties(ns),
            request,
            UpdateNamespacePropertiesResponse.class,
            headers(context),
            ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  private ScheduledExecutorService tokenRefreshExecutor() {
    if (!keepTokenRefreshed) {
      return null;
    }

    if (refreshExecutor == null) {
      synchronized (this) {
        if (refreshExecutor == null) {
          this.refreshExecutor = ThreadPools.newScheduledPool(name() + "-token-refresh", 1);
        }
      }
    }

    return refreshExecutor;
  }

  @Override
  public void close() throws IOException {
    shutdownRefreshExecutor();

    if (closeables != null) {
      closeables.close();
    }

    if (fileIOCloser != null) {
      fileIOCloser.invalidateAll();
      fileIOCloser.cleanUp();
    }
  }

  private void shutdownRefreshExecutor() {
    if (refreshExecutor != null) {
      ScheduledExecutorService service = refreshExecutor;
      this.refreshExecutor = null;

      List<Runnable> tasks = service.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });

      try {
        if (service.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Timed out waiting for refresh executor to terminate");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for refresh executor to terminate", e);
        Thread.currentThread().interrupt();
      }
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
      checkIdentifierIsValid(ident);

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
      if (props != null) {
        this.propertiesBuilder.putAll(props);
      }
      return this;
    }

    @Override
    public Builder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(ident.name())
              .withSchema(schema)
              .withPartitionSpec(spec)
              .withWriteOrder(writeOrder)
              .withLocation(location)
              .setProperties(propertiesBuilder.build())
              .build();

      LoadTableResponse response =
          client.post(
              paths.tables(ident.namespace()),
              request,
              LoadTableResponse.class,
              headers(context),
              ErrorHandlers.tableErrorHandler());

      AuthSession session = tableSession(response.config(), session(context));
      RESTTableOperations ops =
          new RESTTableOperations(
              client,
              paths.table(ident),
              session::headers,
              tableFileIO(context, response.config()),
              response.tableMetadata());

      trackFileIO(ops);

      return new BaseTable(
          ops, fullTableName(ident), metricsReporter(paths.metrics(ident), session::headers));
    }

    @Override
    public Transaction createTransaction() {
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      AuthSession session = tableSession(response.config(), session(context));
      TableMetadata meta = response.tableMetadata();

      RESTTableOperations ops =
          new RESTTableOperations(
              client,
              paths.table(ident),
              session::headers,
              tableFileIO(context, response.config()),
              RESTTableOperations.UpdateType.CREATE,
              createChanges(meta),
              meta);

      trackFileIO(ops);

      return Transactions.createTableTransaction(
          fullName, ops, meta, metricsReporter(paths.metrics(ident), session::headers));
    }

    @Override
    public Transaction replaceTransaction() {
      LoadTableResponse response = loadInternal(context, ident, snapshotMode);
      String fullName = fullTableName(ident);

      AuthSession session = tableSession(response.config(), session(context));
      TableMetadata base = response.tableMetadata();

      Map<String, String> tableProperties = propertiesBuilder.build();
      TableMetadata replacement =
          base.buildReplacement(
              schema,
              spec != null ? spec : PartitionSpec.unpartitioned(),
              writeOrder != null ? writeOrder : SortOrder.unsorted(),
              location != null ? location : base.location(),
              tableProperties);

      ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

      if (replacement.changes().stream()
          .noneMatch(MetadataUpdate.SetCurrentSchema.class::isInstance)) {
        // ensure there is a change to set the current schema
        changes.add(new MetadataUpdate.SetCurrentSchema(replacement.currentSchemaId()));
      }

      if (replacement.changes().stream()
          .noneMatch(MetadataUpdate.SetDefaultPartitionSpec.class::isInstance)) {
        // ensure there is a change to set the default spec
        changes.add(new MetadataUpdate.SetDefaultPartitionSpec(replacement.defaultSpecId()));
      }

      if (replacement.changes().stream()
          .noneMatch(MetadataUpdate.SetDefaultSortOrder.class::isInstance)) {
        // ensure there is a change to set the default sort order
        changes.add(new MetadataUpdate.SetDefaultSortOrder(replacement.defaultSortOrderId()));
      }

      RESTTableOperations ops =
          new RESTTableOperations(
              client,
              paths.table(ident),
              session::headers,
              tableFileIO(context, response.config()),
              RESTTableOperations.UpdateType.REPLACE,
              changes.build(),
              base);

      trackFileIO(ops);

      return Transactions.replaceTableTransaction(
          fullName, ops, replacement, metricsReporter(paths.metrics(ident), session::headers));
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // return a create or a replace transaction, depending on whether the table exists
      // deciding whether to create or replace can't be determined on the service because schema
      // field IDs are assigned
      // at this point and then used in data and metadata files. because create and replace will
      // assign different
      // field IDs, they must be determined before any writes occur
      try {
        return replaceTransaction();
      } catch (NoSuchTableException e) {
        return createTransaction();
      }
    }

    private LoadTableResponse stageCreate() {
      Map<String, String> tableProperties = propertiesBuilder.build();

      CreateTableRequest request =
          CreateTableRequest.builder()
              .stageCreate()
              .withName(ident.name())
              .withSchema(schema)
              .withPartitionSpec(spec)
              .withWriteOrder(writeOrder)
              .withLocation(location)
              .setProperties(tableProperties)
              .build();

      return client.post(
          paths.tables(ident.namespace()),
          request,
          LoadTableResponse.class,
          headers(context),
          ErrorHandlers.tableErrorHandler());
    }
  }

  private static List<MetadataUpdate> createChanges(TableMetadata meta) {
    ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

    changes.add(new MetadataUpdate.AssignUUID(meta.uuid()));
    changes.add(new MetadataUpdate.UpgradeFormatVersion(meta.formatVersion()));

    Schema schema = meta.schema();
    changes.add(new MetadataUpdate.AddSchema(schema, schema.highestFieldId()));
    changes.add(new MetadataUpdate.SetCurrentSchema(-1));

    PartitionSpec spec = meta.spec();
    if (spec != null && spec.isPartitioned()) {
      changes.add(new MetadataUpdate.AddPartitionSpec(spec));
    } else {
      changes.add(new MetadataUpdate.AddPartitionSpec(PartitionSpec.unpartitioned()));
    }
    changes.add(new MetadataUpdate.SetDefaultPartitionSpec(-1));

    SortOrder order = meta.sortOrder();
    if (order != null && order.isSorted()) {
      changes.add(new MetadataUpdate.AddSortOrder(order));
    } else {
      changes.add(new MetadataUpdate.AddSortOrder(SortOrder.unsorted()));
    }
    changes.add(new MetadataUpdate.SetDefaultSortOrder(-1));

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

  private FileIO newFileIO(SessionContext context, Map<String, String> properties) {
    if (null != ioBuilder) {
      return ioBuilder.apply(context, properties);
    } else {
      String ioImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL);
      return CatalogUtil.loadFileIO(ioImpl, properties, conf);
    }
  }

  private FileIO tableFileIO(SessionContext context, Map<String, String> config) {
    if (config.isEmpty() && ioBuilder == null) {
      return io; // reuse client and io since config is the same
    }

    Map<String, String> fullConf = RESTUtil.merge(properties(), config);

    return newFileIO(context, fullConf);
  }

  private AuthSession tableSession(Map<String, String> tableConf, AuthSession parent) {
    AuthSession session = newSession(tableConf, tableConf, parent);

    return session != null ? session : parent;
  }

  private static ConfigResponse fetchConfig(
      RESTClient client, Map<String, String> headers, Map<String, String> properties) {
    // send the client's warehouse location to the service to keep in sync
    // this is needed for cases where the warehouse is configured client side, but may be used on
    // the server side,
    // like the Hive Metastore, where both client and service hive-site.xml may have a warehouse
    // location.
    ImmutableMap.Builder<String, String> queryParams = ImmutableMap.builder();
    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      queryParams.put(
          CatalogProperties.WAREHOUSE_LOCATION,
          properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    ConfigResponse configResponse =
        client.get(
            ResourcePaths.config(),
            queryParams.build(),
            ConfigResponse.class,
            headers,
            ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
  }

  private AuthSession newSession(
      Map<String, String> credentials, Map<String, String> properties, AuthSession parent) {
    if (credentials != null) {
      // use the bearer token without exchanging
      if (credentials.containsKey(OAuth2Properties.TOKEN)) {
        return AuthSession.fromAccessToken(
            client,
            tokenRefreshExecutor(),
            credentials.get(OAuth2Properties.TOKEN),
            expiresAtMillis(properties),
            parent);
      }

      if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
        // fetch a token using the client credentials flow
        return AuthSession.fromCredential(
            client, tokenRefreshExecutor(), credentials.get(OAuth2Properties.CREDENTIAL), parent);
      }

      for (String tokenType : TOKEN_PREFERENCE_ORDER) {
        if (credentials.containsKey(tokenType)) {
          // exchange the token for an access token using the token exchange flow
          return AuthSession.fromTokenExchange(
              client, tokenRefreshExecutor(), credentials.get(tokenType), tokenType, parent);
        }
      }
    }

    return null;
  }

  private Long expiresAtMillis(Map<String, String> properties) {
    if (properties.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
      long expiresInMillis =
          PropertyUtil.propertyAsLong(
              properties,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
      return System.currentTimeMillis() + expiresInMillis;
    } else {
      return null;
    }
  }

  private void checkIdentifierIsValid(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      throw new NoSuchTableException("Invalid table identifier: %s", tableIdentifier);
    }
  }

  private void checkNamespaceIsValid(Namespace namespace) {
    if (namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
    }
  }

  private static Map<String, String> configHeaders(Map<String, String> properties) {
    return RESTUtil.extractPrefixMap(properties, "header.");
  }

  private static Cache<String, AuthSession> newSessionCache(Map<String, String> properties) {
    long expirationIntervalMs =
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT);

    return Caffeine.newBuilder()
        .expireAfterAccess(Duration.ofMillis(expirationIntervalMs))
        .removalListener(
            (RemovalListener<String, AuthSession>) (id, auth, cause) -> auth.stopRefreshing())
        .build();
  }

  private Cache<TableOperations, FileIO> newFileIOCloser() {
    return Caffeine.newBuilder()
        .weakKeys()
        .removalListener(
            (RemovalListener<TableOperations, FileIO>)
                (ops, fileIO, cause) -> {
                  if (null != fileIO) {
                    fileIO.close();
                  }
                })
        .build();
  }
}
