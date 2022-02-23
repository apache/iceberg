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

import java.io.Closeable;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCatalog extends BaseMetastoreCatalog
    implements Configurable<Configuration>, SupportsNamespaces, Closeable {

  public static final String PROPERTY_PREFIX = "jdbc.";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private FileIO io;
  private String catalogName = "jdbc";
  private String warehouseLocation;
  private Object conf;
  private JdbcClientPool connections;

  public JdbcCatalog() {
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");

    String warehouse = properties.get(CatalogProperties.WAREHOUSE_LOCATION);
    Preconditions.checkNotNull(warehouse, "JDBC warehouse location is required");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");

    if (name != null) {
      this.catalogName = name;
    }

    String fileIOImpl = properties.getOrDefault(
        CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    this.io = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    try {
      LOG.debug("Connecting to JDBC database {}", properties.get(CatalogProperties.URI));
      connections = new JdbcClientPool(uri, properties);
      initializeCatalogTables();
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

  private void initializeCatalogTables() throws InterruptedException, SQLException {
    LOG.trace("Creating database tables (if missing) to store iceberg catalog");
    connections.run(conn -> {
      DatabaseMetaData dbMeta = conn.getMetaData();
      ResultSet tableExists = dbMeta.getTables(null /* catalog name */, null /* schemaPattern */,
          JdbcUtil.CATALOG_TABLE_NAME /* tableNamePattern */,  null /* types */);
      if (tableExists.next()) {
        return true;
      }

      LOG.debug("Creating table {} to store iceberg catalog", JdbcUtil.CATALOG_TABLE_NAME);
      return conn.prepareStatement(JdbcUtil.CREATE_CATALOG_TABLE).execute();
    });

    connections.run(conn -> {
      DatabaseMetaData dbMeta = conn.getMetaData();
      ResultSet tableExists = dbMeta.getTables(null /* catalog name */, null /* schemaPattern */,
          JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME /* tableNamePattern */, null /* types */);

      if (tableExists.next()) {
        return true;
      }

      LOG.debug("Creating table {} to store iceberg catalog namespace properties",
          JdbcUtil.NAMESPACE_PROPERTIES_TABLE_NAME);
      return conn.prepareStatement(JdbcUtil.CREATE_NAMESPACE_PROPERTIES_TABLE).execute();
    });
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(connections, io, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    return SLASH.join(defaultNamespaceLocation(table.namespace()), table.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    int deletedRecords;
    try {
      deletedRecords = connections.run(conn -> {
        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.DROP_TABLE_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(identifier.namespace()));
          sql.setString(3, identifier.name());
          return sql.executeUpdate();
        }
      });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to drop %s", identifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to dropTable");
    }

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

    try {
      return connections.run(conn -> {
        List<TableIdentifier> results = Lists.newArrayList();
        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.LIST_TABLES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace));

          ResultSet rs = sql.executeQuery();
          while (rs.next()) {
            results.add(JdbcUtil.stringToTableIdentifier(rs.getString(JdbcUtil.TABLE_NAMESPACE),
                rs.getString(JdbcUtil.TABLE_NAME))
            );
          }

          return results;
        }
      });

    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to list tables in namespace: %s", namespace);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during JDBC operation");
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      int updatedRecords = connections.run(conn -> {
        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.RENAME_TABLE_SQL)) {
          // SET
          sql.setString(1, JdbcUtil.namespaceToString(to.namespace()));
          sql.setString(2, to.name());
          // WHERE
          sql.setString(3, catalogName);
          sql.setString(4, JdbcUtil.namespaceToString(from.namespace()));
          sql.setString(5, from.name());
          return sql.executeUpdate();
        }
      });

      if (updatedRecords == 1) {
        LOG.info("Renamed table from {}, to {}", from, to);
      } else if (updatedRecords == 0) {
        throw new NoSuchTableException("Table does not exist: %s", from);
      } else {
        LOG.warn("Rename operation affected {} rows: the catalog table's primary key assumption has been violated",
            updatedRecords);
      }

    } catch (SQLIntegrityConstraintViolationException e) {
      throw new AlreadyExistsException("Table already exists: %s", to);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to rename %s to %s", from, to);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to rename");
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  private boolean insertProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);

    try {
      int insertedRecords = connections.run(conn -> {
        String sqlStatement = JdbcUtil.insertPropertiesStatement(properties.size());

        try (PreparedStatement sql = conn.prepareStatement(sqlStatement)) {
          int rowIndex = 0;
          for (Map.Entry<String, String> keyValue : properties.entrySet()) {
            sql.setString(rowIndex + 1, catalogName);
            sql.setString(rowIndex + 2, namespaceName);
            sql.setString(rowIndex + 3, keyValue.getKey());
            sql.setString(rowIndex + 4, keyValue.getValue());
            rowIndex += 4;
          }
          return sql.executeUpdate();
        }
      });

      if (insertedRecords == properties.size()) {
        LOG.debug("Successfully inserted {} properties for namespace {}", properties, namespaceName);
        return true;
      } else {
        throw new IllegalStateException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to insertProperties(namespace, properties) " +
          "Namespace: %s", namespace);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to insert properties to namespace: %s in catalog: %s", namespace,
          catalogName);
    }
  }

  private boolean updateProperties(Namespace namespace, Map<String, String> properties) {
    String namespaceName = JdbcUtil.namespaceToString(namespace);

    try {
      int updatedRecords = connections.run(conn -> {
        String sqlStatement = JdbcUtil.updatePropertiesStatement(properties.size());

        try (PreparedStatement sql = conn.prepareStatement(sqlStatement)) {
          int rowIndex = 0;
          for (Map.Entry<String, String> keyValue : properties.entrySet()) {
            sql.setString(rowIndex + 1, keyValue.getKey());
            sql.setString(rowIndex + 2, keyValue.getValue());
            rowIndex += 2;
          }
          sql.setString(rowIndex + 1, catalogName);
          sql.setString(rowIndex + 2, namespaceName);
          rowIndex += 2;
          for (String key : properties.keySet()) {
            sql.setString(rowIndex + 1, key);
            rowIndex += 1;
          }
          LOG.debug("Running query to update namespace properties: {}", sql);
          return sql.executeUpdate();
        }
      });

      if (updatedRecords == properties.size()) {
        LOG.debug("Successfully updated {} to new namespace: {}", properties, namespaceName);
        return true;
      } else {
        throw new IllegalStateException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e,
          "Interrupted in call to updateProperties(namespace, properties) Namespace: %s", namespace);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to update properties to namespace: %s in catalog: %s", namespace,
              catalogName);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException("Namespace already exists: %s", namespace);
    }

    if (metadata == null || metadata.isEmpty()) {
      throw new IllegalArgumentException("Cannot create a namespace with null or empty metadata");
    }

    insertProperties(namespace, metadata);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      List<Namespace> namespaces = connections.run(conn -> {
        List<Namespace> result = Lists.newArrayList();

        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.LIST_NAMESPACES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
          ResultSet rs = sql.executeQuery();
          while (rs.next()) {
            result.add(JdbcUtil.stringToNamespace(rs.getString(JdbcUtil.TABLE_NAMESPACE)));
          }
          rs.close();
        }

        return result;
      });

      int subNamespaceLevelLength = namespace.levels().length + 1;
      namespaces = namespaces.stream()
          // exclude itself
          .filter(n -> !n.equals(namespace))
          // only get sub namespaces/children
          .filter(n -> n.levels().length >= subNamespaceLevelLength)
          // only get sub namespaces/children
          .map(n -> Namespace.of(
              Arrays.stream(n.levels()).limit(subNamespaceLevelLength).toArray(String[]::new)
              )
          )
          // remove duplicates
          .distinct()
          .collect(Collectors.toList());

      return namespaces;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e,
          "Interrupted in call to listNamespaces(namespace) Namespace: %s", namespace);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to list all namespace: %s in catalog", namespace);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.of("location", defaultNamespaceLocation(namespace));
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
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException(
          "Namespace %s is not empty. %s tables exist.", namespace, tableIdentifiers.size());
    }

    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws
      NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("Cannot set properties to a namespace with null or empty properties");
    }

    Map<String, String> namespaceProperties = fetchProperties(namespace);
    Map<String, String> propertiesToInsert = Maps.newHashMap();
    Map<String, String> propertiesToUpdate = Maps.newHashMap();

    for (String key : properties.keySet()) {
      String value = properties.get(key);
      if (namespaceProperties.containsKey(key)) {
        if (!namespaceProperties.get(key).equals(value)) {
          propertiesToUpdate.put(key, value);
        }
      } else {
        propertiesToInsert.put(key, value);
      }
    }

    boolean insertedProperties = true;
    if (!propertiesToInsert.isEmpty()) {
      insertedProperties = insertProperties(namespace, propertiesToInsert);
    }

    boolean updatedProperties = true;
    if (!propertiesToUpdate.isEmpty()) {
      updatedProperties = updateProperties(namespace, propertiesToUpdate);
    }

    return (insertedProperties && updatedProperties);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    if (properties == null || properties.isEmpty()) {
      throw new IllegalArgumentException("Cannot remove properties with null or empty properties");
    }

    String namespaceName = JdbcUtil.namespaceToString(namespace);

    try {
      int deletedRecords = connections.run(conn -> {
        String sqlStatement = JdbcUtil.deletePropertiesStatement(properties);

        try (PreparedStatement sql = conn.prepareStatement(sqlStatement)) {
          sql.setString(1, catalogName);
          sql.setString(2, namespaceName);
          int valueIndex = 2;
          for (String property : properties) {
            sql.setString(++valueIndex, property);
          }
          return sql.executeUpdate();
        }
      });

      if (deletedRecords == properties.size()) {
        LOG.info("Successfully committed to namespace: {}", namespaceName);
        return true;
      } else {
        throw new IllegalStateException();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e,
              "Interrupted in call to removeProperties(namespace, properties) Namespace: %s", namespace);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to remove properties for namespace: %s in catalog: %s", namespace,
              catalogName);
    }
  }

  public Map<String, String> fetchProperties(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      return connections.run(conn -> {
        Map<String, String> properties = Maps.newHashMap();
        String namespaceName = JdbcUtil.namespaceToString(namespace);

        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.GET_ALL_NAMESPACE_PROPERTIES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, namespaceName);
          ResultSet rs = sql.executeQuery();

          while (rs.next()) {
            properties.put(rs.getString(JdbcUtil.NAMESPACE_PROPERTY_KEY),
                    rs.getString(JdbcUtil.NAMESPACE_PROPERTY_VALUE));
          }

          rs.close();
        }

        return properties;
      });
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e,
              "Interrupted in call to fetchProperties(namespace) Namespace: %s", namespace);
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to fetch properties for namespace: %s in catalog: %s", namespace,
              catalogName);
    }

  }

  @Override
  public void close() {
    connections.close();
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    try {
      return connections.run(conn -> {
        boolean tableExists = false;

        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.GET_NAMESPACE_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
          ResultSet rs = sql.executeQuery();
          if (rs.next()) {
            tableExists = true;
          }

          rs.close();
        }

        if (tableExists) {
          return true;
        }

        boolean namespacePropertyExists = false;

        try (PreparedStatement sql = conn.prepareStatement(JdbcUtil.GET_NAMESPACE_PROPERTIES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
          ResultSet rs = sql.executeQuery();
          if (rs.next()) {
            namespacePropertyExists = true;
          }

          rs.close();
        }

        return namespacePropertyExists;
      });

    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to get namespace %s", namespace);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in call to namespaceExists(namespace)");
    }
  }

}
