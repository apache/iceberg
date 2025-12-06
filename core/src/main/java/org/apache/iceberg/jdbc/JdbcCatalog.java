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
import java.sql.Connection;
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
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
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
import org.apache.iceberg.view.ViewMetadata;
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

  @SuppressWarnings(value = "UnusedVariable")
  private String schemaName = null;

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

    this.schemaName =
        PropertyUtil.propertyAsString(properties, JdbcUtil.SCHEMA_NAME_PROPERTY, null);

    this.initializeCatalogTables =
        PropertyUtil.propertyAsBoolean(
            properties, JdbcUtil.INIT_CATALOG_TABLES_PROPERTY, initializeCatalogTables);
    if (initializeCatalogTables) {
      initializeCatalogTables();
    }

    final boolean updateSchemaIfRequired =
        PropertyUtil.propertyAsBoolean(
            properties,
            JdbcUtil.UPDATE_CATALOG_SCHEMA_IF_NECESSARY_PROPERTY,
            initializeCatalogTables);
    if (updateSchemaIfRequired) {
      updateSchemaIfRequired();
    } else {
      schemaVersion =
          JdbcUtil.SchemaVersion.valueOf(
              PropertyUtil.propertyAsString(
                  properties, JdbcUtil.SCHEMA_VERSION_PROPERTY, JdbcUtil.SchemaVersion.V0.name()));
    }

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.addCloseable(connections);
    closeableGroup.addCloseable(io);
    closeableGroup.setSuppressCloseFailure(true);
  }

  private void atomicCreateTable(String tableName, String sqlCommand, String reason)
      throws SQLException, InterruptedException {
    connections.run(
        conn -> {

          // check the existence of a table name
          Predicate<String> tableTest =
              name -> {
                try {
                  return tableExists(conn, tableName);
                } catch (SQLException e) {
                  return false;
                }
              };

          if (tableTest.test(tableName)) {
            return true;
          }

          LOG.debug("Creating table {} {}", tableName, reason);
          try {
            conn.prepareStatement(sqlCommand).execute();
            return true;
          } catch (SQLException e) {
            // see if table was created by another thread or process.
            if (tableTest.test(tableName)) {
              return true;
            }
            throw e;
          }
        });
  }

  private void initializeCatalogTables() {
    LOG.trace("Creating database tables (if missing) to store iceberg catalog");

    try {
      atomicCreateTable(
          JdbcUtil.tableName(JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
          JdbcUtil.withTableName(
              JdbcUtil.V0_CREATE_CATALOG_SQL_TEMPLATE,
              JdbcUtil.CATALOG_TABLE_VIEW_NAME,
              schemaName),
          "to store iceberg catalog tables");
      atomicCreateTable(
          JdbcUtil.tableName(JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME, schemaName),
          JdbcUtil.withTableName(
              JdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE_SQL_TEMPLATE,
              JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
              schemaName),
          "to store iceberg catalog namespace properties");
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
            if (columnExists(
                conn,
                JdbcUtil.tableName(JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
                JdbcUtil.RECORD_TYPE)) {
              LOG.debug("{} already supports views", JdbcUtil.CATALOG_TABLE_VIEW_NAME);
              schemaVersion = JdbcUtil.SchemaVersion.V1;
              return true;
            } else {
              if (PropertyUtil.propertyAsString(
                      catalogProperties,
                      JdbcUtil.SCHEMA_VERSION_PROPERTY,
                      JdbcUtil.SchemaVersion.V0.name())
                  .equalsIgnoreCase(JdbcUtil.SchemaVersion.V1.name())) {
                return updateSchemaToV1(conn);
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

  private boolean updateSchemaToV1(Connection conn) throws SQLException {
    LOG.debug("{} is being updated to support views", JdbcUtil.CATALOG_TABLE_VIEW_NAME);
    schemaVersion = JdbcUtil.SchemaVersion.V1;
    if (isOracle(conn)) {
      return conn.prepareStatement(
              JdbcUtil.withTableName(
                  JdbcUtil.V1_UPDATE_CATALOG_ORACLE_SQL_TEMPLATE,
                  JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                  schemaName))
          .execute();
    } else {
      return conn.prepareStatement(
              JdbcUtil.withTableName(
                  JdbcUtil.V1_UPDATE_CATALOG_SQL_TEMPLATE,
                  JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                  schemaName))
          .execute();
    }
  }

  private static boolean isOracle(Connection conn) {
    try {
      String databaseProductName = conn.getMetaData().getDatabaseProductName();
      return databaseProductName != null
          && databaseProductName.toLowerCase(Locale.ROOT).contains("oracle");
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Cannot determine database product name");
    }
  }

  private static boolean tableExists(Connection conn, String tableNamePattern) throws SQLException {
    DatabaseMetaData dbMeta = conn.getMetaData();

    if (tableNamePattern == null) {
      throw new IllegalArgumentException("tableNamePattern cannot be null");
    }

    String[] tableVariants =
        new String[] {
          tableNamePattern.toUpperCase(Locale.ROOT), tableNamePattern.toLowerCase(Locale.ROOT)
        };

    for (String t : tableVariants) {
      try (ResultSet rs = dbMeta.getColumns(null, null, t, null)) {
        if (rs.next()) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean columnExists(
      Connection conn, String tableNamePattern, String columnNamePattern) throws SQLException {
    DatabaseMetaData dbMeta = conn.getMetaData();

    if (tableNamePattern == null || columnNamePattern == null) {
      throw new IllegalArgumentException(
          "Neither tableNamePattern nor columnNamePattern can be null");
    }

    String[] tableVariants =
        new String[] {
          tableNamePattern.toUpperCase(Locale.ROOT), tableNamePattern.toLowerCase(Locale.ROOT)
        };
    String[] columnVariants =
        new String[] {
          columnNamePattern.toUpperCase(Locale.ROOT), columnNamePattern.toLowerCase(Locale.ROOT)
        };

    for (String t : tableVariants) {
      for (String c : columnVariants) {
        try (ResultSet rs = dbMeta.getColumns(null, null, t, c)) {
          if (rs.next()) {
            return true;
          }
        }
      }
    }
    return false;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(
        connections,
        io,
        catalogName,
        tableIdentifier,
        catalogProperties,
        schemaName,
        schemaVersion);
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier viewIdentifier) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }
    return new JdbcViewOperations(
        connections, io, catalogName, viewIdentifier, catalogProperties, schemaName);
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
                ? JdbcUtil.withTableName(
                    JdbcUtil.V1_DROP_TABLE_SQL_TEMPLATE,
                    JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                    schemaName)
                : JdbcUtil.withTableName(
                    JdbcUtil.V0_DROP_TABLE_SQL_TEMPLATE,
                    JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                    schemaName),
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
            ? JdbcUtil.withTableName(
                JdbcUtil.V1_LIST_TABLE_SQL_TEMPLATE, JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName)
            : JdbcUtil.withTableName(
                JdbcUtil.V0_LIST_TABLE_SQL_TEMPLATE, JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
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
                ? JdbcUtil.withTableName(
                    JdbcUtil.V1_RENAME_TABLE_SQL_TEMPLATE,
                    JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                    schemaName)
                : JdbcUtil.withTableName(
                    JdbcUtil.V0_RENAME_TABLE_SQL_TEMPLATE,
                    JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                    schemaName),
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
            JdbcUtil.withTableName(
                JdbcUtil.LIST_ALL_NAMESPACES_SQL_TEMPLATE,
                JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                schemaName),
            catalogName));
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.NAMESPACE_NAME)),
            JdbcUtil.withTableName(
                JdbcUtil.LIST_ALL_PROPERTY_NAMESPACES_SQL_TEMPLATE,
                JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                schemaName),
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
            JdbcUtil.withTableName(
                JdbcUtil.LIST_NAMESPACES_SQL_TEMPLATE,
                JdbcUtil.CATALOG_TABLE_VIEW_NAME,
                schemaName),
            catalogName,
            JdbcUtil.namespaceToString(namespace) + "%"));
    namespaces.addAll(
        fetch(
            row -> JdbcUtil.stringToNamespace(row.getString(JdbcUtil.NAMESPACE_NAME)),
            JdbcUtil.withTableName(
                JdbcUtil.LIST_PROPERTY_NAMESPACES_SQL_TEMPLATE,
                JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                schemaName),
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
          "Namespace %s is not empty. Contains %d table(s).", namespace, tableIdentifiers.size());
    }

    if (schemaVersion != JdbcUtil.SchemaVersion.V0) {
      List<TableIdentifier> viewIdentifiers = listViews(namespace);
      if (viewIdentifiers != null && !viewIdentifiers.isEmpty()) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d view(s).", namespace, viewIdentifiers.size());
      }
    }

    int deletedRows =
        execute(
            JdbcUtil.withTableName(
                JdbcUtil.DELETE_ALL_NAMESPACE_PROPERTIES_SQL_TEMPLATE,
                JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                schemaName),
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
    return JdbcUtil.namespaceExists(schemaName, catalogName, connections, namespace);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    if (schemaVersion != JdbcUtil.SchemaVersion.V1) {
      throw new UnsupportedOperationException(VIEW_WARNING_LOG_MESSAGE);
    }

    JdbcViewOperations ops = (JdbcViewOperations) newViewOps(identifier);
    ViewMetadata lastViewMetadata = null;
    try {
      lastViewMetadata = ops.current();
    } catch (NotFoundException e) {
      LOG.warn("Failed to load view metadata for view: {}", identifier, e);
    }

    int deletedRecords =
        execute(
            JdbcUtil.withTableName(
                JdbcUtil.DROP_VIEW_SQL_TEMPLATE, JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
            catalogName,
            JdbcUtil.namespaceToString(identifier.namespace()),
            identifier.name());

    if (deletedRecords == 0) {
      LOG.info("Skipping drop, view does not exist: {}", identifier);
      return false;
    }

    if (lastViewMetadata != null) {
      CatalogUtil.dropViewMetadata(ops.io(), lastViewMetadata);
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
        JdbcUtil.withTableName(
            JdbcUtil.LIST_VIEW_SQL_TEMPLATE, JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
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
            JdbcUtil.withTableName(
                JdbcUtil.RENAME_VIEW_SQL_TEMPLATE, JdbcUtil.CATALOG_TABLE_VIEW_NAME, schemaName),
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
            JdbcUtil.withTableName(
                JdbcUtil.GET_ALL_NAMESPACE_PROPERTIES_SQL_TEMPLATE,
                JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                schemaName),
            catalogName,
            namespaceName);

    return ImmutableMap.<String, String>builder().putAll(entries).buildOrThrow();
  }

  private boolean insertProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);

    try {
      int[] insertedRecords =
          connections.run(
              conn -> {
                try (PreparedStatement preparedStatement =
                    conn.prepareStatement(
                        JdbcUtil.withTableName(
                            JdbcUtil.INSERT_NAMESPACE_PROPERTIES_SQL_TEMPLATE,
                            JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                            schemaName))) {
                  for (Map.Entry<String, String> entry : properties.entrySet()) {
                    preparedStatement.setString(1, catalogName);
                    preparedStatement.setString(2, namespaceName);
                    preparedStatement.setString(3, entry.getKey());
                    preparedStatement.setString(4, entry.getValue());
                    preparedStatement.addBatch();
                  }
                  return preparedStatement.executeBatch();
                }
              });

      int successCount = 0;
      for (int result : insertedRecords) {
        if (result >= 0) {
          successCount++;
        }
      }

      if (successCount == properties.size()) {
        return true;
      }

      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Failed to insert: %d of %d succeeded",
              successCount,
              properties.size()));
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to insert properties for namespace: %s", namespace);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted in SQL command");
    }
  }

  private boolean updateProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);

    try {
      int[] updatedRecords =
          connections.run(
              conn -> {
                try (PreparedStatement preparedStatement =
                    conn.prepareStatement(
                        JdbcUtil.withTableName(
                            JdbcUtil.UPDATE_NAMESPACE_PROPERTY_SQL_TEMPLATE,
                            JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME,
                            schemaName))) {
                  for (Map.Entry<String, String> entry : properties.entrySet()) {
                    preparedStatement.setString(1, entry.getValue());
                    preparedStatement.setString(2, catalogName);
                    preparedStatement.setString(3, namespaceName);
                    preparedStatement.setString(4, entry.getKey());
                    preparedStatement.addBatch();
                  }
                  return preparedStatement.executeBatch();
                }
              });

      int successCount = 0;
      for (int result : updatedRecords) {
        if (result >= 0) {
          successCount++;
        }
      }

      if (successCount == properties.size()) {
        return true;
      }

      throw new IllegalStateException(
          String.format(
              Locale.ROOT,
              "Failed to update: %d of %d succeeded",
              successCount,
              properties.size()));
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to update properties for namespace: %s", namespace);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted in SQL command");
    }
  }

  private boolean deleteProperties(Namespace namespace, Set<String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);
    String[] args =
        Stream.concat(Stream.of(catalogName, namespaceName), properties.stream())
            .toArray(String[]::new);

    return execute(JdbcUtil.deletePropertiesStatement(properties, schemaName), args) > 0;
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
