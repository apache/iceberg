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
import org.apache.hadoop.conf.Configurable;
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
import org.apache.iceberg.exceptions.UncheckedSQLException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCatalog extends BaseMetastoreCatalog implements Configurable, SupportsNamespaces, Closeable {

  public static final String SQL_TABLE_NAME = "iceberg_tables";
  public static final String SQL_CREATE_CATALOG_TABLE =
      "CREATE TABLE " + SQL_TABLE_NAME +
          "(catalog_name VARCHAR(1255) NOT NULL," +
          "table_namespace VARCHAR(1255) NOT NULL," +
          "table_name VARCHAR(1255) NOT NULL," +
          "metadata_location VARCHAR(32768)," +
          "previous_metadata_location VARCHAR(32768)," +
          "PRIMARY KEY (catalog_name, table_namespace, table_name)" +
          ")";
  public static final String LOAD_TABLE_SQL = "SELECT * FROM " + SQL_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String LIST_TABLES_SQL = "SELECT * FROM " + SQL_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ?";
  public static final String RENAME_TABLE_SQL = "UPDATE " + SQL_TABLE_NAME +
      " SET table_namespace = ? , table_name = ? " +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String DROP_TABLE_SQL = "DELETE FROM " + SQL_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String GET_NAMESPACE_SQL = "SELECT table_namespace FROM " + SQL_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace LIKE ? LIMIT 1";
  public static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT table_namespace FROM " + SQL_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace LIKE ?";
  public static final String JDBC_PARAM_PREFIX = "connection.parameter.";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private FileIO io;
  private String catalogName = "jdbc";
  private String warehouseLocation;
  private Configuration conf;
  private JdbcClientPool connections;

  public JdbcCatalog() {
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkArgument(!properties.getOrDefault(CatalogProperties.HIVE_URI, "").isEmpty(),
        "No connection url provided for jdbc catalog!");
    Preconditions.checkArgument(!properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "").isEmpty(),
        "Cannot initialize Jdbc Catalog because warehousePath must not be null!");

    this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION).replaceAll("/$", "");
    this.catalogName = name == null ? "jdbc" : name;
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.io = fileIOImpl == null ?
        new HadoopFileIO(conf) :
        CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    try {
      initializeConnection(properties);
    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException("Database Connection timeout!", e);
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException("Database Connection failed!", e);
    } catch (SQLException e) {
      throw new UncheckedSQLException("Failed to initialize catalog!", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to initialize!", e);
    }
  }

  private void initializeConnection(Map<String, String> properties) throws SQLException, InterruptedException {
    LOG.debug("Connecting to Jdbc database {}", properties.get(CatalogProperties.HIVE_URI));
    connections = new JdbcClientPool(properties.get(CatalogProperties.HIVE_URI), properties);
    initializeCatalogTables();
  }

  private void initializeCatalogTables() throws InterruptedException, SQLException {
    // need to check multiple times because some databases are using different naming standard.
    // ex: H2db keeping table names as uppercase, PostgreSQL is keeping lowercase

    boolean exists = connections.run(conn -> {
      boolean foundTable = false;
      DatabaseMetaData dbMeta = conn.getMetaData();
      ResultSet tables = dbMeta.getTables(null, null, SQL_TABLE_NAME, null);
      if (tables.next()) {
        foundTable = true;
      }
      tables.close();
      ResultSet tablesUpper = dbMeta.getTables(null, null, SQL_TABLE_NAME.toUpperCase(), null);
      if (tablesUpper.next()) {
        foundTable = true;
      }
      tablesUpper.close();
      ResultSet tablesLower = dbMeta.getTables(null, null, SQL_TABLE_NAME.toLowerCase(), null);
      if (tablesLower.next()) {
        foundTable = true;
      }
      tablesLower.close();
      return foundTable;
    });

    // create table if not exits
    if (!exists) {
      connections.run(conn -> conn.prepareStatement(SQL_CREATE_CATALOG_TABLE).execute());
      LOG.debug("Created table {} to store iceberg tables!", SQL_TABLE_NAME);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(connections, io, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    if (table.hasNamespace()) {
      return SLASH.join(warehouseLocation, SLASH.join(table.namespace().levels()), table.name());
    }
    return SLASH.join(warehouseLocation, table.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {

    int deletedRecords;
    try {
      deletedRecords = connections.run(conn -> {
        try (PreparedStatement sql = conn.prepareStatement(DROP_TABLE_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(identifier.namespace()));
          sql.setString(3, identifier.name());
          return sql.executeUpdate();
        }
      });
    } catch (SQLException e) {
      throw new UncheckedSQLException("Failed to drop " + identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropTable", e);
    }

    if (deletedRecords > 0) {
      LOG.debug("Successfully dropped table {}.", identifier);
    } else {
      LOG.debug("Cannot drop table: {}! table not found in the catalog.", identifier);
      return false;
    }

    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = ops.current();

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
      LOG.info("Table {} data purged!", identifier);
    }
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
        try (PreparedStatement sql = conn.prepareStatement(LIST_TABLES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace));

          ResultSet rs = sql.executeQuery();
          while (rs.next()) {
            results.add(JdbcUtil.stringToTableIdentifier(rs.getString("table_namespace"), rs.getString("table_name")));
          }

          return results;
        }
      });

    } catch (SQLException e) {
      throw new UncheckedSQLException(String.format("Failed to list tables in namespace: %s", namespace), e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during JDBC operation", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      int updatedRecords = connections.run(conn -> {
        try (PreparedStatement sql = conn.prepareStatement(RENAME_TABLE_SQL)) {
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
        LOG.debug("Successfully renamed table from {} to {}!", from, to);
      } else if (updatedRecords == 0) {
        throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
      } else {
        throw new RuntimeException("Failed to rename table! Rename operation Failed");
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      throw new AlreadyExistsException("Table with name '%s' already exists in the catalog!", to);
    } catch (SQLException e) {
      throw new UncheckedSQLException("Failed to rename table!", e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException("Cannot create namespace " + namespace +
        ": createNamespace is not supported");
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    try {

      List<Namespace> namespaces = connections.run(conn -> {
        List<Namespace> result = Lists.newArrayList();

        try (PreparedStatement sql = conn.prepareStatement(LIST_NAMESPACES_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
          ResultSet rs = sql.executeQuery();
          while (rs.next()) {
            result.add(JdbcUtil.stringToNamespace(rs.getString("table_namespace")));
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

      LOG.debug("From the namespace '{}' found: {}", namespace, namespaces);
      return namespaces;

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listNamespaces(namespace) Namespace: " + namespace, e);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to list all namespace: " + namespace + " in catalog!", e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace) || namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.of("location", SLASH.join(warehouseLocation, SLASH.join(namespace.levels())));
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Cannot drop namespace %s because it is not found!", namespace);
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot drop namespace %s because it is not empty. " +
          "The following tables still exist under the namespace: %s", namespace, tableIdentifiers);
    }
    // namespaces are created/deleted by tables by default return true
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws
      NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot set properties " + namespace + " : setProperties is not supported");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot remove properties " + namespace + " : removeProperties is not supported");
  }

  @Override
  public void close() {
    connections.close();
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    try {
      return connections.run(conn -> {
        boolean exists = false;

        try (PreparedStatement sql = conn.prepareStatement(GET_NAMESPACE_SQL)) {
          sql.setString(1, catalogName);
          sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
          ResultSet rs = sql.executeQuery();
          if (rs.next()) {
            exists = true;
          }
          rs.close();
        }

        return exists;
      });

    } catch (SQLException e) {
      LOG.warn("SQLException! ", e);
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("Interrupted in call to namespaceExists(namespace)! ", e);
      return false;
    }
  }

}
