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
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.catalog.BaseViewSessionCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableCommit;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOTracker;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.MetricsReporters;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTCatalogProperties.SnapshotMode;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableCreateViewRequest;
import org.apache.iceberg.rest.requests.ImmutableRegisterTableRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.EnvironmentUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.ImmutableSQLViewRepresentation;
import org.apache.iceberg.view.ImmutableViewVersion;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewRepresentation;
import org.apache.iceberg.view.ViewUtil;
import org.apache.iceberg.view.ViewVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTSessionCatalog extends BaseViewSessionCatalog
    implements Configurable<Object>, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(RESTSessionCatalog.class);
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";

  /**
   * @deprecated will be removed in 2.0.0. Use {@link
   *     org.apache.iceberg.rest.RESTCatalogProperties#PAGE_SIZE} instead.
   */
  @Deprecated public static final String REST_PAGE_SIZE = "rest-page-size";

  // these default endpoints must not be updated in order to maintain backwards compatibility with
  // legacy servers
  private static final Set<Endpoint> DEFAULT_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_NAMESPACES)
          .add(Endpoint.V1_LOAD_NAMESPACE)
          .add(Endpoint.V1_CREATE_NAMESPACE)
          .add(Endpoint.V1_UPDATE_NAMESPACE)
          .add(Endpoint.V1_DELETE_NAMESPACE)
          .add(Endpoint.V1_LIST_TABLES)
          .add(Endpoint.V1_LOAD_TABLE)
          .add(Endpoint.V1_CREATE_TABLE)
          .add(Endpoint.V1_UPDATE_TABLE)
          .add(Endpoint.V1_DELETE_TABLE)
          .add(Endpoint.V1_RENAME_TABLE)
          .add(Endpoint.V1_REGISTER_TABLE)
          .add(Endpoint.V1_REPORT_METRICS)
          .add(Endpoint.V1_COMMIT_TRANSACTION)
          .build();

  // these view endpoints must not be updated in order to maintain backwards compatibility with
  // legacy servers
  private static final Set<Endpoint> VIEW_ENDPOINTS =
      ImmutableSet.<Endpoint>builder()
          .add(Endpoint.V1_LIST_VIEWS)
          .add(Endpoint.V1_LOAD_VIEW)
          .add(Endpoint.V1_CREATE_VIEW)
          .add(Endpoint.V1_UPDATE_VIEW)
          .add(Endpoint.V1_DELETE_VIEW)
          .add(Endpoint.V1_RENAME_VIEW)
          .build();

  private final Function<Map<String, String>, RESTClient> clientBuilder;
  private final BiFunction<SessionContext, Map<String, String>, FileIO> ioBuilder;
  private FileIOTracker fileIOTracker = null;
  private AuthSession catalogAuth = null;
  private AuthManager authManager;
  private RESTClient client = null;
  private ResourcePaths paths = null;
  private SnapshotMode snapshotMode = null;
  private Object conf = null;
  private FileIO io = null;
  private MetricsReporter reporter = null;
  private boolean reportingViaRestEnabled;
  private Integer pageSize = null;
  private CloseableGroup closeables = null;
  private Set<Endpoint> endpoints;
  private Supplier<Map<String, String>> mutationHeaders = Map::of;

  public RESTSessionCatalog() {
    this(
        config ->
            HTTPClient.builder(config)
                .uri(config.get(CatalogProperties.URI))
                .withHeaders(RESTUtil.configHeaders(config))
                .build(),
        null);
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

    this.closeables = new CloseableGroup();

    this.authManager = AuthManagers.loadAuthManager(name, props);
    this.closeables.addCloseable(this.authManager);

    ConfigResponse config;
    try (RESTClient initClient = clientBuilder.apply(props);
        AuthSession initSession = authManager.initSession(initClient, props)) {
      config = fetchConfig(initClient.withAuthSession(initSession), initSession, props);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close HTTP client", e);
    }

    // build the final configuration and set up the catalog's auth
    Map<String, String> mergedProps = config.merge(props);

    // Enable Idempotency-Key header for mutation endpoints if the server advertises support
    if (config.idempotencyKeyLifetime() != null) {
      this.mutationHeaders = RESTUtil::idempotencyHeaders;
    }

    if (config.endpoints().isEmpty()) {
      this.endpoints =
          PropertyUtil.propertyAsBoolean(
                  mergedProps,
                  RESTCatalogProperties.VIEW_ENDPOINTS_SUPPORTED,
                  RESTCatalogProperties.VIEW_ENDPOINTS_SUPPORTED_DEFAULT)
              ? ImmutableSet.<Endpoint>builder()
                  .addAll(DEFAULT_ENDPOINTS)
                  .addAll(VIEW_ENDPOINTS)
                  .build()
              : DEFAULT_ENDPOINTS;
    } else {
      this.endpoints = ImmutableSet.copyOf(config.endpoints());
    }

    this.client = clientBuilder.apply(mergedProps);
    this.closeables.addCloseable(this.client);

    this.paths = ResourcePaths.forCatalogProperties(mergedProps);

    this.catalogAuth = authManager.catalogSession(client, mergedProps);
    this.closeables.addCloseable(this.catalogAuth);

    this.pageSize =
        PropertyUtil.propertyAsNullableInt(mergedProps, RESTCatalogProperties.PAGE_SIZE);
    if (pageSize != null) {
      Preconditions.checkArgument(
          pageSize > 0,
          "Invalid value for %s, must be a positive integer",
          RESTCatalogProperties.PAGE_SIZE);
    }

    this.io = newFileIO(SessionContext.createEmpty(), mergedProps);

    this.fileIOTracker = new FileIOTracker();
    this.closeables.addCloseable(this.io);
    this.closeables.addCloseable(fileIOTracker);
    this.closeables.setSuppressCloseFailure(true);

    this.snapshotMode =
        SnapshotMode.valueOf(
            PropertyUtil.propertyAsString(
                    mergedProps,
                    RESTCatalogProperties.SNAPSHOT_LOADING_MODE,
                    RESTCatalogProperties.SNAPSHOT_LOADING_MODE_DEFAULT)
                .toUpperCase(Locale.US));

    this.reporter = CatalogUtil.loadMetricsReporter(mergedProps);

    this.reportingViaRestEnabled =
        PropertyUtil.propertyAsBoolean(
            mergedProps,
            RESTCatalogProperties.METRICS_REPORTING_ENABLED,
            RESTCatalogProperties.METRICS_REPORTING_ENABLED_DEFAULT);
    super.initialize(name, mergedProps);
  }

  @Override
  public void setConf(Object newConf) {
    this.conf = newConf;
  }

  @Override
  public List<TableIdentifier> listTables(SessionContext context, Namespace ns) {
    if (!endpoints.contains(Endpoint.V1_LIST_TABLES)) {
      return ImmutableList.of();
    }

    checkNamespaceIsValid(ns);
    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> tables = ImmutableList.builder();
    String pageToken = "";
    if (pageSize != null) {
      queryParams.put("pageSize", String.valueOf(pageSize));
    }

    do {
      queryParams.put("pageToken", pageToken);
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      ListTablesResponse response =
          client
              .withAuthSession(contextualSession)
              .get(
                  paths.tables(ns),
                  queryParams,
                  ListTablesResponse.class,
                  Map.of(),
                  ErrorHandlers.namespaceErrorHandler());
      pageToken = response.nextPageToken();
      tables.addAll(response.identifiers());
    } while (pageToken != null);

    return tables.build();
  }

  @Override
  public boolean dropTable(SessionContext context, TableIdentifier identifier) {
    Endpoint.check(endpoints, Endpoint.V1_DELETE_TABLE);
    checkIdentifierIsValid(identifier);

    try {
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      client
          .withAuthSession(contextualSession)
          .delete(
              paths.table(identifier), null, mutationHeaders, ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public boolean purgeTable(SessionContext context, TableIdentifier identifier) {
    Endpoint.check(endpoints, Endpoint.V1_DELETE_TABLE);
    checkIdentifierIsValid(identifier);

    try {
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      client
          .withAuthSession(contextualSession)
          .delete(
              paths.table(identifier),
              ImmutableMap.of("purgeRequested", "true"),
              null,
              mutationHeaders,
              ErrorHandlers.tableErrorHandler());
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  @Override
  public void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to) {
    Endpoint.check(endpoints, Endpoint.V1_RENAME_TABLE);
    checkIdentifierIsValid(from);
    checkIdentifierIsValid(to);

    RenameTableRequest request =
        RenameTableRequest.builder().withSource(from).withDestination(to).build();

    // for now, ignore the response because there is no way to return it
    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    client
        .withAuthSession(contextualSession)
        .post(paths.rename(), request, null, mutationHeaders, ErrorHandlers.tableErrorHandler());
  }

  @Override
  public boolean tableExists(SessionContext context, TableIdentifier identifier) {
    try {
      checkIdentifierIsValid(identifier);
      if (endpoints.contains(Endpoint.V1_TABLE_EXISTS)) {
        AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
        client
            .withAuthSession(contextualSession)
            .head(paths.table(identifier), Map.of(), ErrorHandlers.tableErrorHandler());
        return true;
      } else {
        // fallback in order to work with 1.7.x and older servers
        return super.tableExists(context, identifier);
      }
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  private static Map<String, String> snapshotModeToParam(SnapshotMode mode) {
    return ImmutableMap.of("snapshots", mode.name().toLowerCase(Locale.US));
  }

  private LoadTableResponse loadInternal(
      SessionContext context, TableIdentifier identifier, SnapshotMode mode) {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_TABLE);
    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    return client
        .withAuthSession(contextualSession)
        .get(
            paths.table(identifier),
            snapshotModeToParam(mode),
            LoadTableResponse.class,
            Map.of(),
            ErrorHandlers.tableErrorHandler());
  }

  @Override
  public Table loadTable(SessionContext context, TableIdentifier identifier) {
    Endpoint.check(
        endpoints,
        Endpoint.V1_LOAD_TABLE,
        () ->
            new NoSuchTableException(
                "Unable to load table %s.%s: Server does not support endpoint %s",
                name(), identifier, Endpoint.V1_LOAD_TABLE));

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
    Map<String, String> tableConf = response.config();
    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    AuthSession tableSession =
        authManager.tableSession(finalIdentifier, tableConf, contextualSession);
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

    RESTClient tableClient = client.withAuthSession(tableSession);
    RESTTableOperations ops =
        newTableOps(
            tableClient,
            paths.table(finalIdentifier),
            Map::of,
            mutationHeaders,
            tableFileIO(context, tableConf, response.credentials()),
            tableMetadata,
            endpoints);

    trackFileIO(ops);

    BaseTable table =
        new BaseTable(
            ops,
            fullTableName(finalIdentifier),
            metricsReporter(paths.metrics(finalIdentifier), tableClient));
    if (metadataType != null) {
      return MetadataTableUtils.createMetadataTableInstance(table, metadataType);
    }

    return table;
  }

  private void trackFileIO(RESTTableOperations ops) {
    if (io != ops.io()) {
      fileIOTracker.track(ops);
    }
  }

  private MetricsReporter metricsReporter(String metricsEndpoint, RESTClient restClient) {
    if (reportingViaRestEnabled && endpoints.contains(Endpoint.V1_REPORT_METRICS)) {
      RESTMetricsReporter restMetricsReporter =
          new RESTMetricsReporter(restClient, metricsEndpoint, Map::of);
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
    Endpoint.check(endpoints, Endpoint.V1_REGISTER_TABLE);
    checkIdentifierIsValid(ident);

    Preconditions.checkArgument(
        metadataFileLocation != null && !metadataFileLocation.isEmpty(),
        "Invalid metadata file location: %s",
        metadataFileLocation);

    RegisterTableRequest request =
        ImmutableRegisterTableRequest.builder()
            .name(ident.name())
            .metadataLocation(metadataFileLocation)
            .build();

    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    LoadTableResponse response =
        client
            .withAuthSession(contextualSession)
            .post(
                paths.register(ident.namespace()),
                request,
                LoadTableResponse.class,
                mutationHeaders,
                ErrorHandlers.tableErrorHandler());

    Map<String, String> tableConf = response.config();
    AuthSession tableSession = authManager.tableSession(ident, tableConf, contextualSession);
    RESTClient tableClient = client.withAuthSession(tableSession);
    RESTTableOperations ops =
        newTableOps(
            tableClient,
            paths.table(ident),
            Map::of,
            mutationHeaders,
            tableFileIO(context, tableConf, response.credentials()),
            response.tableMetadata(),
            endpoints);

    trackFileIO(ops);

    return new BaseTable(
        ops, fullTableName(ident), metricsReporter(paths.metrics(ident), tableClient));
  }

  @Override
  public void createNamespace(
      SessionContext context, Namespace namespace, Map<String, String> metadata) {
    Endpoint.check(endpoints, Endpoint.V1_CREATE_NAMESPACE);
    CreateNamespaceRequest request =
        CreateNamespaceRequest.builder().withNamespace(namespace).setProperties(metadata).build();

    // for now, ignore the response because there is no way to return it
    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    client
        .withAuthSession(contextualSession)
        .post(
            paths.namespaces(),
            request,
            CreateNamespaceResponse.class,
            mutationHeaders,
            ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(SessionContext context, Namespace namespace) {
    if (!endpoints.contains(Endpoint.V1_LIST_NAMESPACES)) {
      return ImmutableList.of();
    }

    Map<String, String> queryParams = Maps.newHashMap();
    if (!namespace.isEmpty()) {
      queryParams.put("parent", RESTUtil.namespaceToQueryParam(namespace));
    }

    ImmutableList.Builder<Namespace> namespaces = ImmutableList.builder();
    String pageToken = "";
    if (pageSize != null) {
      queryParams.put("pageSize", String.valueOf(pageSize));
    }

    do {
      queryParams.put("pageToken", pageToken);
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      ListNamespacesResponse response =
          client
              .withAuthSession(contextualSession)
              .get(
                  paths.namespaces(),
                  queryParams,
                  ListNamespacesResponse.class,
                  Map.of(),
                  ErrorHandlers.namespaceErrorHandler());
      pageToken = response.nextPageToken();
      namespaces.addAll(response.namespaces());
    } while (pageToken != null);

    return namespaces.build();
  }

  @Override
  public boolean namespaceExists(SessionContext context, Namespace namespace) {
    try {
      checkNamespaceIsValid(namespace);
      if (endpoints.contains(Endpoint.V1_NAMESPACE_EXISTS)) {
        AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
        client
            .withAuthSession(contextualSession)
            .head(paths.namespace(namespace), Map.of(), ErrorHandlers.namespaceErrorHandler());
        return true;
      } else {
        // fallback in order to work with 1.7.x and older servers
        return super.namespaceExists(context, namespace);
      }
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace ns) {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_NAMESPACE);
    checkNamespaceIsValid(ns);

    // TODO: rename to LoadNamespaceResponse?
    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    GetNamespaceResponse response =
        client
            .withAuthSession(contextualSession)
            .get(
                paths.namespace(ns),
                GetNamespaceResponse.class,
                Map.of(),
                ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(SessionContext context, Namespace ns) {
    Endpoint.check(endpoints, Endpoint.V1_DELETE_NAMESPACE);
    checkNamespaceIsValid(ns);

    try {
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      client
          .withAuthSession(contextualSession)
          .delete(
              paths.namespace(ns),
              null,
              mutationHeaders,
              ErrorHandlers.dropNamespaceErrorHandler());
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean updateNamespaceMetadata(
      SessionContext context, Namespace ns, Map<String, String> updates, Set<String> removals) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_NAMESPACE);
    checkNamespaceIsValid(ns);

    UpdateNamespacePropertiesRequest request =
        UpdateNamespacePropertiesRequest.builder().updateAll(updates).removeAll(removals).build();

    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    UpdateNamespacePropertiesResponse response =
        client
            .withAuthSession(contextualSession)
            .post(
                paths.namespaceProperties(ns),
                request,
                UpdateNamespacePropertiesResponse.class,
                mutationHeaders,
                ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public void close() throws IOException {
    if (closeables != null) {
      closeables.close();
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
      propertiesBuilder.putAll(tableDefaultProperties());
    }

    /**
     * Get default table properties set at Catalog level through catalog properties.
     *
     * @return default table properties specified in catalog properties
     */
    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    /**
     * Get table properties that are enforced at Catalog level through catalog properties.
     *
     * @return overriding table properties enforced through catalog properties
     */
    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
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
      Endpoint.check(endpoints, Endpoint.V1_CREATE_TABLE);
      propertiesBuilder.putAll(tableOverrideProperties());
      CreateTableRequest request =
          CreateTableRequest.builder()
              .withName(ident.name())
              .withSchema(schema)
              .withPartitionSpec(spec)
              .withWriteOrder(writeOrder)
              .withLocation(location)
              .setProperties(propertiesBuilder.buildKeepingLast())
              .build();

      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      LoadTableResponse response =
          client
              .withAuthSession(contextualSession)
              .post(
                  paths.tables(ident.namespace()),
                  request,
                  LoadTableResponse.class,
                  mutationHeaders,
                  ErrorHandlers.tableErrorHandler());

      Map<String, String> tableConf = response.config();
      AuthSession tableSession = authManager.tableSession(ident, tableConf, contextualSession);
      RESTClient tableClient = client.withAuthSession(tableSession);
      RESTTableOperations ops =
          newTableOps(
              tableClient,
              paths.table(ident),
              Map::of,
              mutationHeaders,
              tableFileIO(context, tableConf, response.credentials()),
              response.tableMetadata(),
              endpoints);

      trackFileIO(ops);

      return new BaseTable(
          ops, fullTableName(ident), metricsReporter(paths.metrics(ident), tableClient));
    }

    @Override
    public Transaction createTransaction() {
      Endpoint.check(endpoints, Endpoint.V1_CREATE_TABLE);
      LoadTableResponse response = stageCreate();
      String fullName = fullTableName(ident);

      Map<String, String> tableConf = response.config();
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      AuthSession tableSession = authManager.tableSession(ident, tableConf, contextualSession);
      TableMetadata meta = response.tableMetadata();

      RESTClient tableClient = client.withAuthSession(tableSession);
      RESTTableOperations ops =
          newTableOps(
              tableClient,
              paths.table(ident),
              Map::of,
              mutationHeaders,
              tableFileIO(context, tableConf, response.credentials()),
              RESTTableOperations.UpdateType.CREATE,
              createChanges(meta),
              meta,
              endpoints);

      trackFileIO(ops);

      return Transactions.createTableTransaction(
          fullName, ops, meta, metricsReporter(paths.metrics(ident), tableClient));
    }

    @Override
    public Transaction replaceTransaction() {
      Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);
      if (viewExists(context, ident)) {
        throw new AlreadyExistsException("View with same name already exists: %s", ident);
      }

      LoadTableResponse response = loadInternal(context, ident, snapshotMode);
      String fullName = fullTableName(ident);

      Map<String, String> tableConf = response.config();
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      AuthSession tableSession = authManager.tableSession(ident, tableConf, contextualSession);
      TableMetadata base = response.tableMetadata();

      propertiesBuilder.putAll(tableOverrideProperties());
      Map<String, String> tableProperties = propertiesBuilder.buildKeepingLast();
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

      RESTClient tableClient = client.withAuthSession(tableSession);
      RESTTableOperations ops =
          newTableOps(
              tableClient,
              paths.table(ident),
              Map::of,
              mutationHeaders,
              tableFileIO(context, tableConf, response.credentials()),
              RESTTableOperations.UpdateType.REPLACE,
              changes.build(),
              base,
              endpoints);

      trackFileIO(ops);

      return Transactions.replaceTableTransaction(
          fullName, ops, replacement, metricsReporter(paths.metrics(ident), tableClient));
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
      propertiesBuilder.putAll(tableOverrideProperties());
      Map<String, String> tableProperties = propertiesBuilder.buildKeepingLast();

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

      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      return client
          .withAuthSession(contextualSession)
          .post(
              paths.tables(ident.namespace()),
              request,
              LoadTableResponse.class,
              mutationHeaders,
              ErrorHandlers.tableErrorHandler());
    }
  }

  private static List<MetadataUpdate> createChanges(TableMetadata meta) {
    ImmutableList.Builder<MetadataUpdate> changes = ImmutableList.builder();

    changes.add(new MetadataUpdate.AssignUUID(meta.uuid()));
    changes.add(new MetadataUpdate.UpgradeFormatVersion(meta.formatVersion()));

    Schema schema = meta.schema();
    changes.add(new MetadataUpdate.AddSchema(schema));
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
    return newFileIO(context, properties, ImmutableList.of());
  }

  private FileIO newFileIO(
      SessionContext context, Map<String, String> properties, List<Credential> storageCredentials) {
    if (null != ioBuilder) {
      return ioBuilder.apply(context, properties);
    } else {
      String ioImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, DEFAULT_FILE_IO_IMPL);
      return CatalogUtil.loadFileIO(
          ioImpl,
          properties,
          conf,
          storageCredentials.stream()
              .map(c -> StorageCredential.create(c.prefix(), c.config()))
              .collect(Collectors.toList()));
    }
  }

  private FileIO tableFileIO(
      SessionContext context, Map<String, String> config, List<Credential> storageCredentials) {
    if (config.isEmpty() && ioBuilder == null && storageCredentials.isEmpty()) {
      return io; // reuse client and io since config/credentials are the same
    }

    Map<String, String> fullConf = RESTUtil.merge(properties(), config);

    return newFileIO(context, fullConf, storageCredentials);
  }

  /**
   * Create a new {@link RESTTableOperations} instance for simple table operations.
   *
   * <p>This method can be overridden in subclasses to provide custom table operations
   * implementations.
   *
   * @param restClient the REST client to use for communicating with the catalog server
   * @param path the REST path for the table
   * @param readHeaders a supplier for additional HTTP headers to include in read requests
   *     (GET/HEAD)
   * @param mutationHeaderSupplier a supplier for additional HTTP headers to include in mutation
   *     requests (POST/DELETE)
   * @param fileIO the FileIO implementation for reading and writing table metadata and data files
   * @param current the current table metadata
   * @param supportedEndpoints the set of supported REST endpoints
   * @return a new RESTTableOperations instance
   */
  protected RESTTableOperations newTableOps(
      RESTClient restClient,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaderSupplier,
      FileIO fileIO,
      TableMetadata current,
      Set<Endpoint> supportedEndpoints) {
    return new RESTTableOperations(
        restClient, path, readHeaders, mutationHeaderSupplier, fileIO, current, supportedEndpoints);
  }

  /**
   * Create a new {@link RESTTableOperations} instance for transaction-based operations (create or
   * replace).
   *
   * <p>This method can be overridden in subclasses to provide custom table operations
   * implementations for transaction-based operations.
   *
   * @param restClient the REST client to use for communicating with the catalog server
   * @param path the REST path for the table
   * @param readHeaders a supplier for additional HTTP headers to include in read requests
   *     (GET/HEAD)
   * @param mutationHeaderSupplier a supplier for additional HTTP headers to include in mutation
   *     requests (POST/DELETE)
   * @param fileIO the FileIO implementation for reading and writing table metadata and data files
   * @param updateType the {@link RESTTableOperations.UpdateType} being performed
   * @param createChanges the list of metadata updates to apply during table creation or replacement
   * @param current the current table metadata (may be null for CREATE operations)
   * @param supportedEndpoints the set of supported REST endpoints
   * @return a new RESTTableOperations instance
   */
  protected RESTTableOperations newTableOps(
      RESTClient restClient,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaderSupplier,
      FileIO fileIO,
      RESTTableOperations.UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> supportedEndpoints) {
    return new RESTTableOperations(
        restClient,
        path,
        readHeaders,
        mutationHeaderSupplier,
        fileIO,
        updateType,
        createChanges,
        current,
        supportedEndpoints);
  }

  /**
   * Create a new {@link RESTViewOperations} instance.
   *
   * <p>This method can be overridden in subclasses to provide custom view operations
   * implementations.
   *
   * @param restClient the REST client to use for communicating with the catalog server
   * @param path the REST path for the view
   * @param readHeaders a supplier for additional HTTP headers to include in read requests
   *     (GET/HEAD)
   * @param mutationHeaderSupplier a supplier for additional HTTP headers to include in mutation
   *     requests (POST/DELETE)
   * @param current the current view metadata
   * @param supportedEndpoints the set of supported REST endpoints
   * @return a new RESTViewOperations instance
   */
  protected RESTViewOperations newViewOps(
      RESTClient restClient,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaderSupplier,
      ViewMetadata current,
      Set<Endpoint> supportedEndpoints) {
    return new RESTViewOperations(
        restClient, path, readHeaders, mutationHeaderSupplier, current, supportedEndpoints);
  }

  private static ConfigResponse fetchConfig(
      RESTClient client, AuthSession initialAuth, Map<String, String> properties) {
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
        client
            .withAuthSession(initialAuth)
            .get(
                ResourcePaths.config(),
                queryParams.build(),
                ConfigResponse.class,
                RESTUtil.configHeaders(properties),
                ErrorHandlers.defaultErrorHandler());
    configResponse.validate();
    return configResponse;
  }

  private void checkIdentifierIsValid(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace().isEmpty()) {
      throw new NoSuchTableException("Invalid table identifier: %s", tableIdentifier);
    }
  }

  private void checkViewIdentifierIsValid(TableIdentifier identifier) {
    if (identifier.namespace().isEmpty()) {
      throw new NoSuchViewException("Invalid view identifier: %s", identifier);
    }
  }

  private void checkNamespaceIsValid(Namespace namespace) {
    if (namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
    }
  }

  public void commitTransaction(SessionContext context, List<TableCommit> commits) {
    Endpoint.check(endpoints, Endpoint.V1_COMMIT_TRANSACTION);
    List<UpdateTableRequest> tableChanges = Lists.newArrayListWithCapacity(commits.size());

    for (TableCommit commit : commits) {
      tableChanges.add(
          UpdateTableRequest.create(commit.identifier(), commit.requirements(), commit.updates()));
    }

    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    client
        .withAuthSession(contextualSession)
        .post(
            paths.commitTransaction(),
            new CommitTransactionRequest(tableChanges),
            null,
            mutationHeaders,
            ErrorHandlers.tableCommitHandler());
  }

  @Override
  public List<TableIdentifier> listViews(SessionContext context, Namespace namespace) {
    if (!endpoints.contains(Endpoint.V1_LIST_VIEWS)) {
      return ImmutableList.of();
    }

    checkNamespaceIsValid(namespace);
    Map<String, String> queryParams = Maps.newHashMap();
    ImmutableList.Builder<TableIdentifier> views = ImmutableList.builder();
    String pageToken = "";
    if (pageSize != null) {
      queryParams.put("pageSize", String.valueOf(pageSize));
    }

    do {
      queryParams.put("pageToken", pageToken);
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      ListTablesResponse response =
          client
              .withAuthSession(contextualSession)
              .get(
                  paths.views(namespace),
                  queryParams,
                  ListTablesResponse.class,
                  Map.of(),
                  ErrorHandlers.namespaceErrorHandler());
      pageToken = response.nextPageToken();
      views.addAll(response.identifiers());
    } while (pageToken != null);

    return views.build();
  }

  @Override
  public boolean viewExists(SessionContext context, TableIdentifier identifier) {
    try {
      checkViewIdentifierIsValid(identifier);
      if (endpoints.contains(Endpoint.V1_VIEW_EXISTS)) {
        AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
        client
            .withAuthSession(contextualSession)
            .head(paths.view(identifier), Map.of(), ErrorHandlers.viewErrorHandler());
        return true;
      } else {
        // fallback in order to work with 1.7.x and older servers
        return super.viewExists(context, identifier);
      }
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  @Override
  public View loadView(SessionContext context, TableIdentifier identifier) {
    Endpoint.check(
        endpoints,
        Endpoint.V1_LOAD_VIEW,
        () ->
            new NoSuchViewException(
                "Unable to load view %s.%s: Server does not support endpoint %s",
                name(), identifier, Endpoint.V1_LOAD_VIEW));

    checkViewIdentifierIsValid(identifier);

    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    LoadViewResponse response =
        client
            .withAuthSession(contextualSession)
            .get(
                paths.view(identifier),
                LoadViewResponse.class,
                Map.of(),
                ErrorHandlers.viewErrorHandler());

    Map<String, String> tableConf = response.config();
    AuthSession tableSession = authManager.tableSession(identifier, tableConf, contextualSession);
    ViewMetadata metadata = response.metadata();

    RESTViewOperations ops =
        newViewOps(
            client.withAuthSession(tableSession),
            paths.view(identifier),
            Map::of,
            mutationHeaders,
            metadata,
            endpoints);

    return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
  }

  @Override
  public ViewBuilder buildView(SessionContext context, TableIdentifier identifier) {
    return new RESTViewBuilder(context, identifier);
  }

  @Override
  public boolean dropView(SessionContext context, TableIdentifier identifier) {
    Endpoint.check(endpoints, Endpoint.V1_DELETE_VIEW);
    checkViewIdentifierIsValid(identifier);

    try {
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      client
          .withAuthSession(contextualSession)
          .delete(paths.view(identifier), null, mutationHeaders, ErrorHandlers.viewErrorHandler());
      return true;
    } catch (NoSuchViewException e) {
      return false;
    }
  }

  @Override
  public void renameView(SessionContext context, TableIdentifier from, TableIdentifier to) {
    Endpoint.check(endpoints, Endpoint.V1_RENAME_VIEW);
    checkViewIdentifierIsValid(from);
    checkViewIdentifierIsValid(to);

    RenameTableRequest request =
        RenameTableRequest.builder().withSource(from).withDestination(to).build();

    AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
    client
        .withAuthSession(contextualSession)
        .post(paths.renameView(), request, null, mutationHeaders, ErrorHandlers.viewErrorHandler());
  }

  private class RESTViewBuilder implements ViewBuilder {
    private final SessionContext context;
    private final TableIdentifier identifier;
    private final Map<String, String> properties = Maps.newHashMap();
    private final List<ViewRepresentation> representations = Lists.newArrayList();
    private Namespace defaultNamespace = null;
    private String defaultCatalog = null;
    private Schema schema = null;
    private String location = null;

    private RESTViewBuilder(SessionContext context, TableIdentifier identifier) {
      checkViewIdentifierIsValid(identifier);
      this.identifier = identifier;
      this.context = context;
      this.properties.putAll(viewDefaultProperties());
    }

    /**
     * Get default view properties set at Catalog level through catalog properties.
     *
     * @return default view properties specified in catalog properties
     */
    private Map<String, String> viewDefaultProperties() {
      Map<String, String> viewDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.VIEW_DEFAULT_PREFIX);
      LOG.info(
          "View properties set at catalog level through catalog properties: {}",
          viewDefaultProperties);
      return viewDefaultProperties;
    }

    /**
     * Get view properties that are enforced at Catalog level through catalog properties.
     *
     * @return overriding view properties enforced through catalog properties
     */
    private Map<String, String> viewOverrideProperties() {
      Map<String, String> viewOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.VIEW_OVERRIDE_PREFIX);
      LOG.info(
          "View properties enforced at catalog level through catalog properties: {}",
          viewOverrideProperties);
      return viewOverrideProperties;
    }

    @Override
    public ViewBuilder withSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ViewBuilder withQuery(String dialect, String sql) {
      representations.add(
          ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
      return this;
    }

    @Override
    public ViewBuilder withDefaultCatalog(String catalog) {
      this.defaultCatalog = catalog;
      return this;
    }

    @Override
    public ViewBuilder withDefaultNamespace(Namespace namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    @Override
    public ViewBuilder withProperties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      return this;
    }

    @Override
    public ViewBuilder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    @Override
    public ViewBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public View create() {
      Endpoint.check(endpoints, Endpoint.V1_CREATE_VIEW);
      Preconditions.checkState(
          !representations.isEmpty(), "Cannot create view without specifying a query");
      Preconditions.checkState(null != schema, "Cannot create view without specifying schema");
      Preconditions.checkState(
          null != defaultNamespace, "Cannot create view without specifying a default namespace");

      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(1)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      properties.putAll(viewOverrideProperties());

      CreateViewRequest request =
          ImmutableCreateViewRequest.builder()
              .name(identifier.name())
              .location(location)
              .schema(schema)
              .viewVersion(viewVersion)
              .properties(properties)
              .build();

      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      LoadViewResponse response =
          client
              .withAuthSession(contextualSession)
              .post(
                  paths.views(identifier.namespace()),
                  request,
                  LoadViewResponse.class,
                  mutationHeaders,
                  ErrorHandlers.viewErrorHandler());

      Map<String, String> tableConf = response.config();
      AuthSession tableSession = authManager.tableSession(identifier, tableConf, contextualSession);
      RESTViewOperations ops =
          newViewOps(
              client.withAuthSession(tableSession),
              paths.view(identifier),
              Map::of,
              mutationHeaders,
              response.metadata(),
              endpoints);

      return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
    }

    @Override
    public View createOrReplace() {
      try {
        return replace(loadView());
      } catch (NoSuchViewException e) {
        return create();
      }
    }

    @Override
    public View replace() {
      if (tableExists(context, identifier)) {
        throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
      }

      return replace(loadView());
    }

    private LoadViewResponse loadView() {
      Endpoint.check(
          endpoints,
          Endpoint.V1_LOAD_VIEW,
          () ->
              new NoSuchViewException(
                  "Unable to load view %s.%s: Server does not support endpoint %s",
                  name(), identifier, Endpoint.V1_LOAD_VIEW));

      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      return client
          .withAuthSession(contextualSession)
          .get(
              paths.view(identifier),
              LoadViewResponse.class,
              Map.of(),
              ErrorHandlers.viewErrorHandler());
    }

    private View replace(LoadViewResponse response) {
      Endpoint.check(endpoints, Endpoint.V1_UPDATE_VIEW);
      Preconditions.checkState(
          !representations.isEmpty(), "Cannot replace view without specifying a query");
      Preconditions.checkState(null != schema, "Cannot replace view without specifying schema");
      Preconditions.checkState(
          null != defaultNamespace, "Cannot replace view without specifying a default namespace");

      ViewMetadata metadata = response.metadata();

      int maxVersionId =
          metadata.versions().stream()
              .map(ViewVersion::versionId)
              .max(Integer::compareTo)
              .orElseGet(metadata::currentVersionId);

      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(maxVersionId + 1)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      properties.putAll(viewOverrideProperties());

      ViewMetadata.Builder builder =
          ViewMetadata.buildFrom(metadata)
              .setProperties(properties)
              .setCurrentVersion(viewVersion, schema);

      if (null != location) {
        builder.setLocation(location);
      }

      ViewMetadata replacement = builder.build();

      Map<String, String> tableConf = response.config();
      AuthSession contextualSession = authManager.contextualSession(context, catalogAuth);
      AuthSession tableSession = authManager.tableSession(identifier, tableConf, contextualSession);
      RESTViewOperations ops =
          newViewOps(
              client.withAuthSession(tableSession),
              paths.view(identifier),
              Map::of,
              mutationHeaders,
              metadata,
              endpoints);

      ops.commit(metadata, replacement);

      return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
    }
  }
}
