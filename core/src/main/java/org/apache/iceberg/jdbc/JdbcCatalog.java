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
package org.apache.iceberg.jdbc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.ViewOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCatalog extends BaseMetastoreViewCatalog
    implements Configurable<Object>, SupportsNamespaces {

  public static final String PROPERTY_PREFIX = "jdbc.";
  private static final String NAMESPACE_EXISTS_PROPERTY = "exists";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  static final String VIEW_WARNING_LOG_MESSAGE =
      "JDBC catalog is initialized without view support. To auto-migrate the database's schema and enable view support, set jdbc.schema-version=V1";

  private FileIO io;
  private String catalogName = "jdbc";
  private String warehouseLocation;
  private Object conf;
  private JdbcClientPool connections;
  private Map<String, String> catalogProperties;
  private final Function<Map<String, String>, FileIO> ioBuilder;
  private final Function<Map<String, String>, JdbcClientPool> clientPoolBuilder;
  private boolean initializeCatalogTables;
  private CloseableGroup closeableGroup;
  private JdbcUtil.SchemaVersion schemaVersion = JdbcUtil.SchemaVersion.V0;

  public JdbcCatalog() {
    this(null, null, true);
  }

  public JdbcCatalog(
      Function<Map<String, String>, FileIO> ioBuilder,
      Function<Map<String, String>, JdbcClientPool> clientPoolBuilder,
      boolean initializeCatalogTables) {
    this.ioBuilder = ioBuilder;
    this.clientPoolBuilder = clientPoolBuilder;
    this.initializeCatalogTables = initializeCatalogTables;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid catalog properties: null");
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");

    String inputWarehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(inputWarehouseLocation),
        "Cannot initialize JDBCCatalog because warehousePath must not be null or empty");

    this.warehouseLocation = LocationUtil.stripTrailingSlash(inputWarehouseLocation);
    this.catalogProperties = ImmutableMap.copyOf(properties);

    if (name != null) {
      this.catalogName = name;
    }

    if (null != ioBuilder) {
      this.io = ioBuilder.apply(properties);
    } else {
      String ioImpl =
          properties.getOrDefault(
              CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
      this.io = CatalogUtil.loadFileIO(ioImpl, properties, conf);
    }

    LOG.debug("Connecting to JDBC database {}", uri);
    if (null != clientPoolBuilder) {
      this.connections = clientPoolBuilder.apply(properties);
    } else {
      this.connections = new JdbcClientPool(uri, properties);
    }

    this.initializeCatalogTables =
        PropertyUtil.propertyAsBoolean(
            properties, JdbcUtil.INIT_CATALOG_TABLES_PROPERTY, initializeCatalogTables);
    if (initializeCatalogTables) {
      initializeCatalogTables();
    }

    updateSchemaIfRequired();

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.addCloseable(connections);
    closeableGroup.setSuppressCloseFailure(true);
  }

  private void initializeCatalogTables() {
    LOG.trace("Creating database tables (if missing) to store iceberg catalog");
    try {
      connections.run(
          conn -> {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists =
                dbMeta.getTables(
                    null /* catalog name */,
                    null /* schemaPattern */,
                    JdbcUtil.CATALOG_TABLE_VIEW_NAME /* tableNamePattern */,
                    null /* types */);
            if (tableExists.next()) {
              return true;
            }

            LOG.debug(
                "Creating table {} to store iceberg catalog tables",
                JdbcUtil.CATALOG_TABLE_VIEW_NAME);
            return conn.prepareStatement(JdbcUtil.V0_CREATE_CATALOG_SQL).execute();
          });

      connections.run(
          conn -> {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet tableExists =
                dbMeta.getTables(
                    null /* catalog name */,
                    null /* schemaPattern */,
                    JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME /* tableNamePattern */,
                    null /* types */);

            if (tableExists.next()) {
              return true;
            }

            LOG.debug(
                "Creating table {} to store iceberg catalog namespace properties",
                JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME);
            return conn.prepareStatement(JdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE_SQL).execute();
          });

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot initialize JDBC catalog");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
    }
  }

  private void updateSchemaIfRequired() {
    try {
      connections.run(
          conn -> {
            DatabaseMetaData dbMeta = conn.getMetaData();
            ResultSet typeColumn =
                dbMeta.getColumns(
                    null, null, JdbcUtil.CATALOG_TABLE_VIEW_NAME, JdbcUtil.RECORD_TYPE);
            if (typeColumn.next()) {
              LOG.debug("{} already supports views", JdbcUtil.CATALOG_TABLE_VIEW_NAME);
              schemaVersion = JdbcUtil.SchemaVersion.V1;
              return true;
            } else {
              if (PropertyUtil.propertyAsString(
                      catalogProperties,
                      JdbcUtil.SCHEMA_VERSION_PROPERTY,
                      JdbcUtil.SchemaVersion.V0.name())
                  .equalsIgnoreCase(JdbcUtil.SchemaVersion.V1.name())) {
                LOG.debug("{} is being updated to support views", JdbcUtil.CATALOG_TABLE_VIEW_NAME);
                schemaVersion = JdbcUtil.SchemaVersion.V1;
                return conn.prepareStatement(JdbcUtil.V1_UPDATE_CATALOG_SQL).execute();
              } else {
                LOG.warn(VIEW_WARNING_LOG_MESSAGE);
                return true;
              }
            }
          });
    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Cannot update JDBC catalog: Query timed out");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Cannot update JDBC catalog: Connection failed");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot check and eventually update SQL schema");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to initialize");
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(
        connections, io, catalogName, tableIdentifier, catalogProperties, schemaVersion);
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier viewIdentifier) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }
    return new JdbcViewOperations(connections, io, catalogName, viewIdentifier, catalogProperties);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    return SLASH.join(defaultNamespaceLocation(table.namespace()), table.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = null;
    if (purge) {
      try {
        lastMetadata = ops.current();
      } catch (NotFoundException e) {
        LOG.warn(
            "Failed to load table metadata for table: {}, continuing drop without purge",
            identifier,
            e);
      }
    }

    int deletedRecords =
        execute(
            (schemaVersion == JdbcUtil.SchemaVersion.V1)
                ? JdbcUtil.V1_DROP_TABLE_SQL
                : JdbcUtil.V0_DROP_TABLE_SQL,
            catalogName,
            JdbcUtil.namespaceToString(identifier.namespace()),
            identifier.name());

    if (deletedRecords == 0) {
      LOG.info("Skipping drop, table does not exist: {}", identifier);
      return false;
    }

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    LOG.info("Dropped table: {}", identifier);
    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return fetch(
        row ->
            JdbcUtil.stringToTableIdentifier(
                row.getString(JdbcUtil.TABLE_NAMESPACE), row.getString(JdbcUtil.TABLE_NAME)),
        (schemaVersion == JdbcUtil.SchemaVersion.V1)
            ? JdbcUtil.V1_LIST_TABLE_SQL
            : JdbcUtil.V0_LIST_TABLE_SQL,
        catalogName,
        JdbcUtil.namespaceToString(namespace));
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    if (!tableExists(from)) {
      throw new NoSuchTableException("Table does not exist: %s", from);
    }

    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", to.namespace());
    }

    if (schemaVersion == JdbcUtil.SchemaVersion.V1 && viewExists(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
    }

    if (tableExists(to)) {
      throw new AlreadyExistsException("Table already exists: %s", to);
    }

    int updatedRecords =
        execute(
            err -> {
              // SQLite doesn't set SQLState or throw SQLIntegrityConstraintViolationException
              if (err instanceof SQLIntegrityConstraintViolationException
                  || (err.getMessage() != null && err.getMessage().contains("constraint failed"))) {
                throw new AlreadyExistsException("Table already exists: %s", to);
              }
            },
            (schemaVersion == JdbcUtil.SchemaVersion.V1)
                ? JdbcUtil.V1_RENAME_TABLE_SQL
                : JdbcUtil.V0_RENAME_TABLE_SQL,
            JdbcUtil.namespaceToString(to.namespace()),
            to.name(),
            catalogName,
            JdbcUtil.namespaceToString(from.namespace()),
            from.name());

    if (updatedRecords == 1) {
      LOG.info("Renamed table from {}, to {}", from, to);
    } else if (updatedRecords == 0) {
      throw new NoSuchTableException("Table does not exist: %s", from);
    } else {
      LOG.warn(
          "Rename operation affected {} rows: the catalog table's primary key assumption has been violated",
          updatedRecords);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    Map<String, String> createMetadata;
    if (metadata == null || metadata.isEmpty()) {
      createMetadata = ImmutableMap.of(NAMESPACE_EXISTS_PROPERTY, "true");
    } else {
      createMetadata =
          ImmutableMap.<String, String>builder()
              .putAll(metadata)
              .put(NAMESPACE_EXISTS_PROPERTY, "true")
              .buildOrThrow();
    }

    insertProperties(namespace, createMetadata);
  }

  @Override
  public List<Namespace> listNamespaces() {
    List<Namespace> namespaces = Lists.newArrayList();
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.TABLE_NAMESPACE)),
            JdbcUtil.LIST_ALL_NAMESPACES_SQL,
            catalogName));
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.NAMESPACE_NAME)),
            JdbcUtil.LIST_ALL_PROPERTY_NAMESPACES_SQL,
            catalogName));

    namespaces =
        namespaces.stream()
            // only get sub namespaces/children
            .filter(n -> n.levels().length >= 1)
            // only get sub namespaces/children
            .map(n -> Namespace.of(Arrays.stream(n.levels()).limit(1).toArray(String[]::new)))
            // remove duplicates
            .distinct()
            .collect(Collectors.toList());

    return namespaces;
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (namespace.isEmpty()) {
      return listNamespaces();
    }

    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<Namespace> namespaces = Lists.newArrayList();
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.TABLE_NAMESPACE)),
            JdbcUtil.LIST_NAMESPACES_SQL,
            catalogName,
            JdbcUtil.namespaceToString(namespace) + "%"));
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.NAMESPACE_NAME)),
            JdbcUtil.LIST_PROPERTY_NAMESPACES_SQL,
            catalogName,
            JdbcUtil.namespaceToString(namespace) + "%"));

    int subNamespaceLevelLength = namespace.levels().length + 1;
    namespaces =
        namespaces.stream()
            // exclude itself
            .filter(n -> !n.equals(namespace))
            // only get sub namespaces/children
            .filter(n -> n.levels().length >= subNamespaceLevelLength)
            // only get sub namespaces/children
            .map(
                n ->
                    Namespace.of(
                        Arrays.stream(n.levels())
                            .limit(subNamespaceLevelLength)
                            .toArray(String[]::new)))
            // remove duplicates
            .distinct()
            // exclude fuzzy matches when `namespace` contains `%` or `_`
            .filter(
                n -> {
                  for (int i = 0; i < namespace.levels().length; i++) {
                    if (!n.levels()[i].equals(namespace.levels()[i])) {
                      return false;
                    }
                  }
                  return true;
                })
            .collect(Collectors.toList());

    return namespaces;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Map<String, String> properties = Maps.newHashMap();
    properties.putAll(fetchProperties(namespace));
    if (!properties.containsKey("location")) {
      properties.put("location", defaultNamespaceLocation(namespace));
    }
    properties.remove(NAMESPACE_EXISTS_PROPERTY); // do not return reserved existence property

    return ImmutableMap.copyOf(properties);
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespaceExists(namespace)) {
      return false;
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException(
          "Namespace %s is not empty. %s tables exist.", namespace, tableIdentifiers.size());
    }

    int deletedRows =
        execute(
            JdbcUtil.DELETE_ALL_NAMESPACE_PROPERTIES_SQL,
            catalogName,
            JdbcUtil.namespaceToString(namespace));

    return deletedRows > 0;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Preconditions.checkNotNull(properties, "Invalid properties to set: null");

    if (properties.isEmpty()) {
      return false;
    }

    Preconditions.checkArgument(
        !properties.containsKey(NAMESPACE_EXISTS_PROPERTY),
        "Cannot set reserved property: %s",
        NAMESPACE_EXISTS_PROPERTY);

    Map<String, String> startingProperties = fetchProperties(namespace);
    Map<String, String> inserts = Maps.newHashMap();
    Map<String, String> updates = Maps.newHashMap();

    for (String key : properties.keySet()) {
      String value = properties.get(key);
      if (startingProperties.containsKey(key)) {
        updates.put(key, value);
      } else {
        inserts.put(key, value);
      }
    }

    boolean hadInserts = false;
    if (!inserts.isEmpty()) {
      hadInserts = insertProperties(namespace, inserts);
    }

    boolean hadUpdates = false;
    if (!updates.isEmpty()) {
      hadUpdates = updateProperties(namespace, updates);
    }

    return hadInserts || hadUpdates;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Preconditions.checkNotNull(properties, "Invalid properties to remove: null");

    if (properties.isEmpty()) {
      return false;
    }

    return deleteProperties(namespace, properties);
  }

  @Override
  public void close() {
    if (closeableGroup != null) {
      try {
        closeableGroup.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return JdbcUtil.namespaceExists(catalogName, connections, namespace);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }

    int deletedRecords =
        execute(
            JdbcUtil.DROP_VIEW_SQL,
            catalogName,
            JdbcUtil.namespaceToString(identifier.namespace()),
            identifier.name());

    if (deletedRecords == 0) {
      LOG.info("Skipping drop, view does not exist: {}", identifier);
      return false;
    }

    LOG.info("Dropped view: {}", identifier);
    return true;
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }

    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return fetch(
        row ->
            JdbcUtil.stringToTableIdentifier(
                row.getString(JdbcUtil.TABLE_NAMESPACE), row.getString(JdbcUtil.TABLE_NAME)),
        JdbcUtil.LIST_VIEW_SQL,
        catalogName,
        JdbcUtil.namespaceToString(namespace));
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }

    if (from.equals(to)) {
      return;
    }

    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", to.namespace());
    }

    if (!viewExists(from)) {
      throw new NoSuchViewException("View does not exist");
    }

    if (tableExists(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
    }

    if (viewExists(to)) {
      throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
    }

    int updatedRecords =
        execute(
            err -> {
              // SQLite doesn't set SQLState or throw SQLIntegrityConstraintViolationException
              if (err instanceof SQLIntegrityConstraintViolationException
                  || (err.getMessage() != null && err.getMessage().contains("constraint failed"))) {
                throw new AlreadyExistsException(
                    "Cannot rename %s to %s. View already exists", from, to);
              }
            },
            JdbcUtil.RENAME_VIEW_SQL,
            JdbcUtil.namespaceToString(to.namespace()),
            to.name(),
            catalogName,
            JdbcUtil.namespaceToString(from.namespace()),
            from.name());

    if (updatedRecords == 1) {
      LOG.info("Renamed view from {}, to {}", from, to);
    } else if (updatedRecords == 0) {
      throw new NoSuchViewException("View does not exist: %s", from);
    } else {
      LOG.warn(
          "Rename operation affected {} rows: the catalog view's primary key assumption has been violated",
          updatedRecords);
    }
  }

  @VisibleForTesting
  JdbcClientPool connectionPool() {
    return connections;
  }

  private int execute(String sql, String... args) {
    return execute(err -> {}, sql, args);
  }

  private int execute(Consumer<SQLException> sqlErrorHandler, String sql, String... args) {
    try {
      return connections.run(
          conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                preparedStatement.setString(pos + 1, args[pos]);
              }

              return preparedStatement.executeUpdate();
            }
          });
    } catch (SQLException e) {
      sqlErrorHandler.accept(e);
      throw new UncheckedSQLException(e, "Failed to execute: %s", sql);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted in SQL command");
    }
  }

  @FunctionalInterface
  interface RowProducer<R> {
    R apply(ResultSet result) throws SQLException;
  }

  @SuppressWarnings("checkstyle:NestedTryDepth")
  private <R> List<R> fetch(RowProducer<R> toRow, String sql, String... args) {
    try {
      return connections.run(
          conn -> {
            List<R> result = Lists.newArrayList();

            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                preparedStatement.setString(pos + 1, args[pos]);
              }

              try (ResultSet rs = preparedStatement.executeQuery()) {
                while (rs.next()) {
                  result.add(toRow.apply(rs));
                }
              }
            }

            return result;
          });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to execute query: %s", sql);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
    }
  }

  private Map<String, String> fetchProperties(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    String namespaceName = JdbcUtil.namespaceToString(namespace);

    List<Map.Entry<String, String>> entries =
        fetch(
            row ->
                new AbstractMap.SimpleImmutableEntry<>(
                    row.getString(JdbcUtil.NAMESPACE_PROPERTY_KEY),
                    row.getString(JdbcUtil.NAMESPACE_PROPERTY_VALUE)),
            JdbcUtil.GET_ALL_NAMESPACE_PROPERTIES_SQL,
            catalogName,
            namespaceName);

    return ImmutableMap.<String, String>builder().putAll(entries).buildOrThrow();
  }

  private boolean insertProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);
    String[] args =
        properties.entrySet().stream()
            .flatMap(
                entry -> Stream.of(catalogName, namespaceName, entry.getKey(), entry.getValue()))
            .toArray(String[]::new);

    int insertedRecords = execute(JdbcUtil.insertPropertiesStatement(properties.size()), args);

    if (insertedRecords == properties.size()) {
      return true;
    }

    throw new IllegalStateException(
        String.format("Failed to insert: %d of %d succeeded", insertedRecords, properties.size()));
  }

  private boolean updateProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);
    Stream<String> caseArgs =
        properties.entrySet().stream()
            .flatMap(entry -> Stream.of(entry.getKey(), entry.getValue()));
    Stream<String> whereArgs =
        Stream.concat(Stream.of(catalogName, namespaceName), properties.keySet().stream());

    String[] args = Stream.concat(caseArgs, whereArgs).toArray(String[]::new);

    int updatedRecords = execute(JdbcUtil.updatePropertiesStatement(properties.size()), args);

    if (updatedRecords == properties.size()) {
      return true;
    }

    throw new IllegalStateException(
        String.format("Failed to update: %d of %d succeeded", updatedRecords, properties.size()));
  }

  private boolean deleteProperties(Namespace namespace, Set<String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);
    String[] args =
        Stream.concat(Stream.of(catalogName, namespaceName), properties.stream())
            .toArray(String[]::new);

    return execute(JdbcUtil.deletePropertiesStatement(properties), args) > 0;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new ViewAwareTableBuilder(identifier, schema);
  }

  /**
   * The purpose of this class is to add view detection only when SchemaVersion.V1 schema is used
   * when replacing a table.
   */
  protected class ViewAwareTableBuilder extends BaseMetastoreCatalogTableBuilder {

    private final TableIdentifier identifier;

    public ViewAwareTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public Transaction replaceTransaction() {
      if (schemaVersion == JdbcUtil.SchemaVersion.V1 && viewExists(identifier)) {
        throw new AlreadyExistsException("View with same name already exists: %s", identifier);
      }

      return super.replaceTransaction();
    }
  }
}
