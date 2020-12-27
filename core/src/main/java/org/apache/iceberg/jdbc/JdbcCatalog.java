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
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.iceberg.exceptions.UncheckedIOException;
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
  public static final String SQL_TABLE_DDL =
          "CREATE TABLE " + SQL_TABLE_NAME +
                  "(catalog_name VARCHAR(1255) NOT NULL," +
                  "table_namespace VARCHAR(1255) NOT NULL," +
                  "table_name VARCHAR(1255) NOT NULL," +
                  "metadata_location VARCHAR(32768)," +
                  "previous_metadata_location VARCHAR(32768)," +
                  "PRIMARY KEY (catalog_name, table_namespace, table_name)" +
                  ")";
  public static final String SQL_SELECT_TABLE = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_SELECT_ALL = "SELECT * FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ?";
  public static final String SQL_UPDATE_TABLE_NAME = "UPDATE " + SQL_TABLE_NAME +
          " SET table_namespace = ? , table_name = ? " +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_DELETE_TABLE = "DELETE FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  public static final String SQL_SELECT_NAMESPACE = "SELECT table_namespace FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace LIKE ? LIMIT 1";
  public static final String SQL_SELECT_NAMESPACES = "SELECT DISTINCT table_namespace FROM " + SQL_TABLE_NAME +
          " WHERE catalog_name = ? AND table_namespace LIKE ?";
  public static final String JDBC_PARAM_PREFIX = "connection.parameter.";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private FileIO fileIO;
  private String catalogName = "jdbc";
  private String warehouseLocation;
  private Configuration hadoopConf;
  private JdbcClientPool dbConnPool;

  public JdbcCatalog() {
  }

  @SuppressWarnings("checkstyle:HiddenField")
  @Override
  public void initialize(String name, Map<String, String> properties) throws java.io.UncheckedIOException {
    Preconditions.checkArgument(!properties.getOrDefault(CatalogProperties.HIVE_URI, "").isEmpty(),
            "No connection url provided for jdbc catalog!");
    Preconditions.checkArgument(!properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "").isEmpty(),
            "Cannot initialize Jdbc Catalog because warehousePath must not be null!");

    this.warehouseLocation = properties.get(CatalogProperties.WAREHOUSE_LOCATION).replaceAll("/$", "");
    this.catalogName = name == null ? "jdbc" : name;
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ?
            new HadoopFileIO(hadoopConf) :
            CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    initializeConnection(properties);
  }

  private void initializeConnection(Map<String, String> properties) throws java.io.UncheckedIOException {
    try {
      LOG.debug("Connecting to Jdbc database {}", properties.get(CatalogProperties.HIVE_URI));
      Properties dbProps = new Properties();
      for (Map.Entry<String, String> prop : properties.entrySet()) {
        if (prop.getKey().startsWith(JDBC_PARAM_PREFIX)) {
          dbProps.put(prop.getKey().substring(JDBC_PARAM_PREFIX.length()), prop.getValue());
        }
      }
      dbConnPool = new JdbcClientPool(properties.get(CatalogProperties.HIVE_URI), dbProps);
      initializeCatalogTables();
    } catch (SQLTimeoutException e) {
      throw new UncheckedIOException("Database Connection timeout!", e);
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedIOException("Database Connection failed!", e);
    } catch (SQLWarning e) {
      throw new UncheckedIOException("Database connection warning!", e);
    } catch (SQLException | InterruptedException e) {
      throw new UncheckedIOException("Failed to initialize Jdbc Catalog!", e);
    }
  }

  private void initializeCatalogTables() throws InterruptedException, SQLException {
    // need to check multiple times because some databases are using different naming standard. ex: H2db keeping
    // table names as uppercase
    boolean exists = false;
    DatabaseMetaData dbMeta = dbConnPool.run(Connection::getMetaData);
    ResultSet tables = dbMeta.getTables(null, null, SQL_TABLE_NAME, null);
    if (tables.next()) {
      exists = true;
    }
    tables.close();
    ResultSet tablesUpper = dbMeta.getTables(null, null, SQL_TABLE_NAME.toUpperCase(), null);
    if (tablesUpper.next()) {
      exists = true;
    }
    tablesUpper.close();
    ResultSet tablesLower = dbMeta.getTables(null, null, SQL_TABLE_NAME.toLowerCase(), null);
    if (tablesLower.next()) {
      exists = true;
    }
    tablesLower.close();

    // create table if not exits
    if (!exists) {
      dbConnPool.run(conn -> conn.prepareStatement(SQL_TABLE_DDL).execute());
      LOG.debug("Created table {} to store iceberg tables!", SQL_TABLE_NAME);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(dbConnPool, fileIO, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    if (table.hasNamespace()) {
      return SLASH.join(warehouseLocation, table.namespace().levels(), table.name());
    }
    return SLASH.join(warehouseLocation, table.name());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata = ops.current();
    try {
      PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_DELETE_TABLE));
      sql.setString(1, catalogName);
      sql.setString(2, JdbcUtil.namespaceToString(identifier.namespace()));
      sql.setString(3, identifier.name());
      int deletedRecords = sql.executeUpdate();

      if (deletedRecords > 0) {
        LOG.debug("Successfully dropped table {}.", identifier);
      } else {
        throw new NoSuchTableException("Cannot drop table %s! table not found in the catalog.", identifier);
      }

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        LOG.info("Table {} data purged!", identifier);
      }
      return true;
    } catch (SQLException | InterruptedException e) {
      throw new UncheckedIOException("Failed to drop table!", e);
    }
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!this.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist!", namespace);
    }
    try {
      List<TableIdentifier> results = Lists.newArrayList();
      PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_SELECT_ALL));
      sql.setString(1, catalogName);
      sql.setString(2, JdbcUtil.namespaceToString(namespace));
      ResultSet rs = sql.executeQuery();

      while (rs.next()) {
        final TableIdentifier table = JdbcUtil.stringToTableIdentifier(
                rs.getString("table_namespace"), rs.getString("table_name"));
        results.add(table);
      }
      rs.close();
      return results;

    } catch (SQLException | InterruptedException e) {
      LOG.error("Failed to list tables!", e);
      return null;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_UPDATE_TABLE_NAME));
      // SET
      sql.setString(1, JdbcUtil.namespaceToString(to.namespace()));
      sql.setString(2, to.name());
      // WHERE
      sql.setString(3, catalogName);
      sql.setString(4, JdbcUtil.namespaceToString(from.namespace()));
      sql.setString(5, from.name());
      int updatedRecords = sql.executeUpdate();

      if (updatedRecords == 1) {
        LOG.debug("Successfully renamed table from {} to {}!", from, to);
      } else if (updatedRecords == 0) {
        throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
      } else {
        throw new UncheckedIOException("Failed to rename table! Rename operation Failed");
      }
    } catch (SQLIntegrityConstraintViolationException e) {
      throw new AlreadyExistsException("Table with name '%s' already exists in the catalog!", to);
    } catch (DataTruncation e) {
      throw new UncheckedIOException("Database data truncation error!", e);
    } catch (SQLWarning e) {
      throw new UncheckedIOException("Database warning!", e);
    } catch (SQLException e) {
      throw new UncheckedIOException("Failed to rename table!", e);
    } catch (InterruptedException e) {
      throw new UncheckedIOException("Database Connection interrupted!", e);
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Configuration getConf() {
    return this.hadoopConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException("Cannot create namespace " + namespace +
            ": createNamespace is not supported");
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!this.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist %s", namespace);
    }
    try {
      List<Namespace> namespaces = Lists.newArrayList();
      PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_SELECT_NAMESPACES));
      sql.setString(1, catalogName);
      if (namespace.isEmpty()) {
        sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
      } else {
        sql.setString(2, JdbcUtil.namespaceToString(namespace) + ".%");
      }
      ResultSet rs = sql.executeQuery();
      while (rs.next()) {
        rs.getString("table_namespace");
        namespaces.add(JdbcUtil.stringToNamespace(rs.getString("table_namespace")));
      }
      rs.close();
      int subNamespaceLevelLength = namespace.levels().length + 1;
      namespaces = namespaces.stream()
              // exclude itself
              .filter(n -> !n.equals(namespace))
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
    } catch (Exception e) {
      LOG.error("Failed to list namespace!", e);
      return null;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Path nsPath = new Path(warehouseLocation, JdbcUtil.JOINER_DOT.join(namespace.levels()));
    if (!this.namespaceExists(namespace) || namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.of("location", nsPath.toString());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!this.namespaceExists(namespace)) {
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
    this.dbConnPool.close();
  }

  public boolean namespaceExists(Namespace namespace) {
    boolean exists = false;
    try {
      PreparedStatement sql = dbConnPool.run(c -> c.prepareStatement(SQL_SELECT_NAMESPACE));
      sql.setString(1, catalogName);
      sql.setString(2, JdbcUtil.namespaceToString(namespace) + "%");
      ResultSet rs = sql.executeQuery();
      if (rs.next()) {
        exists = true;
      }
      rs.close();
    } catch (SQLException | InterruptedException e) {
      LOG.warn("SQLException! ", e);
    }
    return exists;
  }

}
