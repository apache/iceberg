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
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.UncheckedIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JdbcCatalog extends BaseMetastoreCatalog implements Configurable, SupportsNamespaces, Closeable {

  public static final String JDBC_PARAM_PREFIX = "connection.parameter.";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private FileIO fileIO;
  private String name = "jdbc";
  private String warehouseLocation;
  private Configuration hadoopConf;
  private JdbcClientPool dbConnPool;
  private TableSQL tableSQL;

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
    this.name = name == null ? "jdbc" : name;
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
      tableSQL = new TableSQL(dbConnPool, name);
    } catch (SQLException | InterruptedException e) {
      throw new UncheckedIOException("Failed to initialize Jdbc Catalog!", e);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(tableSQL, fileIO, name, tableIdentifier);
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
      tableSQL.dropTable(identifier);
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
    if (!tableSQL.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace %s does not exist!", namespace);
    }
    try {
      return tableSQL.listTables(namespace);
    } catch (SQLException | InterruptedException e) {
      LOG.error("Failed to list tables!", e);
      return null;
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      if (tableSQL.getTable(from).isEmpty()) {
        throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
      }
      int updatedRecords = tableSQL.renameTable(from, to);
      if (updatedRecords == 1) {
        LOG.debug("Successfully renamed table from {} to {}!", from, to);
      } else if (updatedRecords == 0) {
        throw new NoSuchTableException("Failed to rename table! Table '%s' not found in the catalog!", from);
      } else {
        throw new UncheckedIOException("Failed to rename table! Rename operation Failed");
      }
      // validate rename operation succeeded
      if (!tableSQL.exists(to)) {
        throw new NoSuchTableException("Rename Operation Failed! Table '%s' not found after the rename!", to);
      }
    } catch (SQLException | InterruptedException e) {
      throw new UncheckedIOException("Failed to rename table!", e);
    }
  }

  @Override
  public String name() {
    return name;
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
    if (!namespace.isEmpty() && !tableSQL.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist %s", namespace);
    }
    try {
      List<Namespace> nsList = tableSQL.listNamespaces(namespace);
      LOG.debug("From the namespace '{}' found: {}", namespace, nsList);
      return nsList;
    } catch (Exception e) {
      LOG.error("Failed to list namespace!", e);
      return null;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Path nsPath = new Path(warehouseLocation, TableSQL.JOINER_DOT.join(namespace.levels()));
    if (!tableSQL.namespaceExists(namespace) || namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.of("location", nsPath.toString());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!tableSQL.namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Cannot drop namespace %s because it is not found!", namespace);
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (tableIdentifiers != null && !tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException("Cannot drop namespace %s because it is not empty. " +
              "The following tables still exist under the namespace: %s", namespace, tableIdentifiers);
    }
    // namespaces are created/deleted by tables
    // by default return true
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
}
