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

package org.apache.iceberg.flink;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A Flink Catalog implementation that wraps an Iceberg {@link Catalog}.
 * <p>
 * The mapping between Flink database and Iceberg namespace:
 * Supplying a base namespace for a given catalog, so if you have a catalog that supports a 2-level namespace, you
 * would supply the first level in the catalog configuration and the second level would be exposed as Flink databases.
 * <p>
 * The Iceberg table manages its partitions by itself. The partition of the Iceberg table is independent of the
 * partition of Flink.
 */
public class FlinkCatalog extends AbstractCatalog {

  private final Catalog originalCatalog;
  private final Catalog icebergCatalog;
  private final String[] baseNamespace;
  private final SupportsNamespaces asNamespaceCatalog;

  public FlinkCatalog(
      String catalogName,
      String defaultDatabase,
      String[] baseNamespace,
      Catalog icebergCatalog,
      boolean cacheEnabled) {
    super(catalogName, defaultDatabase);
    this.originalCatalog = icebergCatalog;
    this.icebergCatalog = cacheEnabled ? CachingCatalog.wrap(icebergCatalog) : icebergCatalog;
    this.baseNamespace = baseNamespace;
    if (icebergCatalog instanceof SupportsNamespaces) {
      asNamespaceCatalog = (SupportsNamespaces) icebergCatalog;
    } else {
      asNamespaceCatalog = null;
    }
  }

  @Override
  public void open() throws CatalogException {
  }

  @Override
  public void close() throws CatalogException {
    if (originalCatalog instanceof Closeable) {
      try {
        ((Closeable) originalCatalog).close();
      } catch (IOException e) {
        throw new CatalogException(e);
      }
    }
  }

  private Namespace toNamespace(String database) {
    String[] namespace = new String[baseNamespace.length + 1];
    System.arraycopy(baseNamespace, 0, namespace, 0, baseNamespace.length);
    namespace[baseNamespace.length] = database;
    return Namespace.of(namespace);
  }

  private TableIdentifier toIdentifier(ObjectPath path) {
    return TableIdentifier.of(toNamespace(path.getDatabaseName()), path.getObjectName());
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    if (asNamespaceCatalog == null) {
      return Collections.singletonList(getDefaultDatabase());
    }

    return asNamespaceCatalog.listNamespaces(Namespace.of(baseNamespace)).stream()
        .map(n -> n.level(n.levels().length - 1))
        .collect(Collectors.toList());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName) throws DatabaseNotExistException, CatalogException {
    if (asNamespaceCatalog == null) {
      if (!getDefaultDatabase().equals(databaseName)) {
        throw new DatabaseNotExistException(getName(), databaseName);
      } else {
        return new CatalogDatabaseImpl(Maps.newHashMap(), "");
      }
    } else {
      try {
        Map<String, String> metadata =
            Maps.newHashMap(asNamespaceCatalog.loadNamespaceMetadata(toNamespace(databaseName)));
        String comment = metadata.remove("comment");
        return new CatalogDatabaseImpl(metadata, comment);
      } catch (NoSuchNamespaceException e) {
        throw new DatabaseNotExistException(getName(), databaseName, e);
      }
    }
  }

  @Override
  public boolean databaseExists(String databaseName) throws CatalogException {
    try {
      getDatabase(databaseName);
      return true;
    } catch (DatabaseNotExistException ignore) {
      return false;
    }
  }

  @Override
  public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (asNamespaceCatalog != null) {
      try {
        asNamespaceCatalog.createNamespace(
            toNamespace(name),
            mergeComment(database.getProperties(), database.getComment()));
      } catch (AlreadyExistsException e) {
        if (!ignoreIfExists) {
          throw new DatabaseAlreadyExistException(getName(), name, e);
        }
      }
    } else {
      throw new UnsupportedOperationException("Namespaces are not supported by catalog: " + getName());
    }
  }

  private Map<String, String> mergeComment(Map<String, String> metadata, String comment) {
    Map<String, String> ret = Maps.newHashMap(metadata);
    if (metadata.containsKey("comment")) {
      throw new CatalogException("Database properties should not contain key: 'comment'.");
    }

    if (!StringUtils.isNullOrWhitespaceOnly(comment)) {
      ret.put("comment", comment);
    }
    return ret;
  }

  @Override
  public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    if (asNamespaceCatalog != null) {
      try {
        boolean success = asNamespaceCatalog.dropNamespace(toNamespace(name));
        if (!success && !ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name);
        }
      } catch (NoSuchNamespaceException e) {
        if (!ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name, e);
        }
      } catch (NamespaceNotEmptyException e) {
        throw new DatabaseNotEmptyException(getName(), name, e);
      }
    } else {
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
    }
  }

  @Override
  public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
      throws DatabaseNotExistException, CatalogException {
    if (asNamespaceCatalog != null) {
      Namespace namespace = toNamespace(name);
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();

      try {
        Map<String, String> oldOptions = asNamespaceCatalog.loadNamespaceMetadata(namespace);
        Map<String, String> newOptions = mergeComment(newDatabase.getProperties(), newDatabase.getComment());

        for (String key : oldOptions.keySet()) {
          if (!newOptions.containsKey(key)) {
            removals.add(key);
          }
        }

        for (Map.Entry<String, String> entry : newOptions.entrySet()) {
          if (!entry.getValue().equals(oldOptions.get(entry.getKey()))) {
            updates.put(entry.getKey(), entry.getValue());
          }
        }

        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(namespace, updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(namespace, removals);
        }

      } catch (org.apache.iceberg.exceptions.NoSuchNamespaceException e) {
        if (!ignoreIfNotExists) {
          throw new DatabaseNotExistException(getName(), name, e);
        }
      }
    } else {
      if (getDefaultDatabase().equals(name)) {
        throw new CatalogException(
            "Can not alter the default database when the iceberg catalog doesn't support namespaces.");
      }
      if (!ignoreIfNotExists) {
        throw new DatabaseNotExistException(getName(), name);
      }
    }
  }

  @Override
  public List<String> listTables(String databaseName) throws DatabaseNotExistException, CatalogException {
    try {
      return icebergCatalog.listTables(toNamespace(databaseName)).stream()
          .map(TableIdentifier::name)
          .collect(Collectors.toList());
    } catch (NoSuchNamespaceException e) {
      throw new DatabaseNotExistException(getName(), databaseName, e);
    }
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath tablePath) throws TableNotExistException, CatalogException {
    try {
      Table table = icebergCatalog.loadTable(toIdentifier(tablePath));
      TableSchema tableSchema = FlinkSchemaUtil.toSchema(FlinkSchemaUtil.convert(table.schema()));

      // NOTE: We can not create a IcebergCatalogTable, because Flink optimizer may use CatalogTableImpl to copy a new
      // catalog table.
      // Let's re-loading table from Iceberg catalog when creating source/sink operators.
      return new CatalogTableImpl(tableSchema, table.properties(), null);
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new TableNotExistException(getName(), tablePath, e);
    }
  }

  @Override
  public boolean tableExists(ObjectPath tablePath) throws CatalogException {
    return icebergCatalog.tableExists(toIdentifier(tablePath));
  }

  @Override
  public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    try {
      icebergCatalog.dropTable(toIdentifier(tablePath));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new TableNotExistException(getName(), tablePath, e);
    }
  }

  @Override
  public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    try {
      icebergCatalog.renameTable(
          toIdentifier(tablePath),
          toIdentifier(new ObjectPath(tablePath.getDatabaseName(), newTableName)));
    } catch (org.apache.iceberg.exceptions.NoSuchTableException e) {
      throw new TableNotExistException(getName(), tablePath, e);
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistException(getName(), tablePath, e);
    }
  }

  /**
   * TODO Add partitioning to the Flink DDL parser.
   */
  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws CatalogException {
    throw new UnsupportedOperationException("Not support createTable now.");
  }

  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException("Not support alterTable now.");
  }

  // ------------------------------ Unsupported methods ---------------------------------------------

  @Override
  public List<String> listViews(String databaseName) throws CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition partition,
      boolean ignoreIfExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec, CatalogPartition newPartition,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String dbName) throws CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath) throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableStatistics(ObjectPath tablePath, CatalogTableStatistics tableStatistics,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(ObjectPath tablePath, CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics, boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(ObjectPath tablePath, List<Expression> filters)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  // After partition pruning and filter push down, the statistics have become very inaccurate, so the statistics from
  // here are of little significance.
  // Flink will support something like SupportsReportStatistics in future.

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
      throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }
}
