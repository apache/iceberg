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
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.jdbc.JdbcUtil.toIcebergExceptionIfPossible;

public class JdbcCatalog extends BaseMetastoreCatalog implements Configurable, SupportsNamespaces, Closeable {

  public static final String PROPERTY_PREFIX = "jdbc.";
  private static final Logger LOG = LoggerFactory.getLogger(JdbcCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");

  private FileIO io;
  private String catalogName = "jdbc";
  private String warehouseLocation;
  private Configuration conf;
  private CatalogDb db;

  public JdbcCatalog(FileIO io, CatalogDb db) {
    this.io = Preconditions.checkNotNull(io);
    this.db = Preconditions.checkNotNull(db);
  }

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

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.io = fileIOImpl == null ? new HadoopFileIO(conf) : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    try {
      LOG.debug("Connecting to JDBC database {}", properties.get(CatalogProperties.URI));
      this.db = new DefaultCatalogDb(catalogName, new JdbcClientPool(uri, properties));
      LOG.trace("Creating database tables (if missing) to store iceberg catalog");
      db.initialize();
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, null, null, null);
    }
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new JdbcTableOperations(db, io, catalogName, tableIdentifier);
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

    try {
      if (!db.dropTable(JdbcUtil.namespaceToString(identifier.namespace()), identifier.name())) {
        return false;
      }
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, null, null, null);
    }

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    LOG.info("Dropped table: {}", identifier);
    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    try {
      List<TableIdentifier> tables = db.listTables(JdbcUtil.namespaceToString(namespace))
              .stream()
              .map(tableName -> TableIdentifier.of(namespace, tableName))
              .collect(Collectors.toList());
      if (tables.isEmpty()) {
        throw new NoSuchNamespaceException("Namespace %s is empty.", namespace);
      }
      return tables;
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, namespace, null, null);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    try {
      db.renameTable(JdbcUtil.namespaceToString(from.namespace()), from.name(), JdbcUtil.namespaceToString(to.namespace()), to.name());
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, from.namespace(), from, to);
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
    try {
      verifyNamespaceExists(namespace);
      final int subNamespaceLevelLength = namespace.levels().length + 1;
      return db.listNamespaceByPrefix(JdbcUtil.namespaceToString(namespace))
              .stream()
              .map(JdbcUtil::stringToNamespace)
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
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, namespace, null, null);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    verifyNamespaceExists(namespace);
    // TODO load properties from CatalogDb
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
    try {
      verifyNamespaceExists(namespace);
      if (!db.isNamespaceEmpty(JdbcUtil.namespaceToString(namespace))) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty.", namespace);
      }
    } catch (CatalogDbException e) {
      throw toIcebergExceptionIfPossible(e, namespace, null, null);
    }

    // TODO implement drop

    return false;
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
    try {
      db.close();
    } catch (Exception e) {
      throw (e instanceof RuntimeException) ? (RuntimeException) e : new RuntimeException(e);
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    try {
      return db.namespaceExists(JdbcUtil.namespaceToString(namespace));
    } catch (CatalogDbException dbException) {
      throw toIcebergExceptionIfPossible(dbException, null, null, null);
    }
  }

  private void verifyNamespaceExists(Namespace namespace) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
  }
}
