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

import static org.apache.flink.util.Preconditions.checkArgument;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.TableChange;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;
import org.apache.flink.util.StringUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.flink.util.FlinkAlterTableUtil;
import org.apache.iceberg.flink.util.FlinkCompatibilityUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

/**
 * A Flink Catalog implementation that wraps an Iceberg {@link Catalog}.
 *
 * <p>The mapping between Flink database and Iceberg namespace: Supplying a base namespace for a
 * given catalog, so if you have a catalog that supports a 2-level namespace, you would supply the
 * first level in the catalog configuration and the second level would be exposed as Flink
 * databases.
 *
 * <p>The Iceberg table manages its partitions by itself. The partition of the Iceberg table is
 * independent of the partition of Flink.
 */
public class FlinkCatalog extends AbstractCatalog {
  private final CatalogLoader catalogLoader;
  private final Catalog icebergCatalog;
  private final Namespace baseNamespace;
  private final SupportsNamespaces asNamespaceCatalog;
  private final Closeable closeable;
  private final boolean cacheEnabled;

  public FlinkCatalog(
      String catalogName,
      String defaultDatabase,
      Namespace baseNamespace,
      CatalogLoader catalogLoader,
      boolean cacheEnabled,
      long cacheExpirationIntervalMs) {
    super(catalogName, defaultDatabase);
    this.catalogLoader = catalogLoader;
    this.baseNamespace = baseNamespace;
    this.cacheEnabled = cacheEnabled;

    Catalog originalCatalog = catalogLoader.loadCatalog();
    icebergCatalog =
        cacheEnabled
            ? CachingCatalog.wrap(originalCatalog, cacheExpirationIntervalMs)
            : originalCatalog;
    asNamespaceCatalog =
        originalCatalog instanceof SupportsNamespaces ? (SupportsNamespaces) originalCatalog : null;
    closeable = originalCatalog instanceof Closeable ? (Closeable) originalCatalog : null;

    FlinkEnvironmentContext.init();
  }

  @Override
  public void open() throws CatalogException {}

  @Override
  public void close() throws CatalogException {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException e) {
        throw new CatalogException(e);
      }
    }
  }

  public Catalog catalog() {
    return icebergCatalog;
  }

  /** Append a new level to the base namespace */
  private static Namespace appendLevel(Namespace baseNamespace, String newLevel) {
    String[] namespace = new String[baseNamespace.levels().length + 1];
    System.arraycopy(baseNamespace.levels(), 0, namespace, 0, baseNamespace.levels().length);
    namespace[baseNamespace.levels().length] = newLevel;
    return Namespace.of(namespace);
  }

  TableIdentifier toIdentifier(ObjectPath path) {
    String objectName = path.getObjectName();
    List<String> tableName = Splitter.on('$').splitToList(objectName);

    if (tableName.size() == 1) {
      return TableIdentifier.of(
          appendLevel(baseNamespace, path.getDatabaseName()), path.getObjectName());
    } else if (tableName.size() == 2 && MetadataTableType.from(tableName.get(1)) != null) {
      return TableIdentifier.of(
          appendLevel(appendLevel(baseNamespace, path.getDatabaseName()), tableName.get(0)),
          tableName.get(1));
    } else {
      throw new IllegalArgumentException("Illegal table name:" + objectName);
    }
  }

  @Override
  public List<String> listDatabases() throws CatalogException {
    if (asNamespaceCatalog == null) {
      return Collections.singletonList(getDefaultDatabase());
    }

    return asNamespaceCatalog.listNamespaces(baseNamespace).stream()
        .map(n -> n.level(n.levels().length - 1))
        .collect(Collectors.toList());
  }

  @Override
  public CatalogDatabase getDatabase(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    if (asNamespaceCatalog == null) {
      if (!getDefaultDatabase().equals(databaseName)) {
        throw new DatabaseNotExistException(getName(), databaseName);
      } else {
        return new CatalogDatabaseImpl(Maps.newHashMap(), "");
      }
    } else {
      try {
        Map<String, String> metadata =
            Maps.newHashMap(
                asNamespaceCatalog.loadNamespaceMetadata(appendLevel(baseNamespace, databaseName)));
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
    createDatabase(
        name, mergeComment(database.getProperties(), database.getComment()), ignoreIfExists);
  }

  private void createDatabase(
      String databaseName, Map<String, String> metadata, boolean ignoreIfExists)
      throws DatabaseAlreadyExistException, CatalogException {
    if (asNamespaceCatalog != null) {
      try {
        asNamespaceCatalog.createNamespace(appendLevel(baseNamespace, databaseName), metadata);
      } catch (AlreadyExistsException e) {
        if (!ignoreIfExists) {
          throw new DatabaseAlreadyExistException(getName(), databaseName, e);
        }
      }
    } else {
      throw new UnsupportedOperationException(
          "Namespaces are not supported by catalog: " + getName());
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
        boolean success = asNamespaceCatalog.dropNamespace(appendLevel(baseNamespace, name));
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
      Namespace namespace = appendLevel(baseNamespace, name);
      Map<String, String> updates = Maps.newHashMap();
      Set<String> removals = Sets.newHashSet();

      try {
        Map<String, String> oldProperties = asNamespaceCatalog.loadNamespaceMetadata(namespace);
        Map<String, String> newProperties =
            mergeComment(newDatabase.getProperties(), newDatabase.getComment());

        for (String key : oldProperties.keySet()) {
          if (!newProperties.containsKey(key)) {
            removals.add(key);
          }
        }

        for (Map.Entry<String, String> entry : newProperties.entrySet()) {
          if (!entry.getValue().equals(oldProperties.get(entry.getKey()))) {
            updates.put(entry.getKey(), entry.getValue());
          }
        }

        if (!updates.isEmpty()) {
          asNamespaceCatalog.setProperties(namespace, updates);
        }

        if (!removals.isEmpty()) {
          asNamespaceCatalog.removeProperties(namespace, removals);
        }

      } catch (NoSuchNamespaceException e) {
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
  public List<String> listTables(String databaseName)
      throws DatabaseNotExistException, CatalogException {
    try {
      return icebergCatalog.listTables(appendLevel(baseNamespace, databaseName)).stream()
          .map(TableIdentifier::name)
          .collect(Collectors.toList());
    } catch (NoSuchNamespaceException e) {
      throw new DatabaseNotExistException(getName(), databaseName, e);
    }
  }

  @Override
  public CatalogTable getTable(ObjectPath tablePath)
      throws TableNotExistException, CatalogException {
    Table table = loadIcebergTable(tablePath);
    return toCatalogTable(table);
  }

  private Table loadIcebergTable(ObjectPath tablePath) throws TableNotExistException {
    try {
      Table table = icebergCatalog.loadTable(toIdentifier(tablePath));
      if (cacheEnabled) {
        table.refresh();
      }

      return table;
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
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath, e);
      }
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
      if (!ignoreIfNotExists) {
        throw new TableNotExistException(getName(), tablePath, e);
      }
    } catch (AlreadyExistsException e) {
      throw new TableAlreadyExistException(getName(), tablePath, e);
    }
  }

  @Override
  public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
      throws CatalogException, TableAlreadyExistException {
    if (Objects.equals(
        table.getOptions().get("connector"), FlinkDynamicTableFactory.FACTORY_IDENTIFIER)) {
      throw new IllegalArgumentException(
          "Cannot create the table with 'connector'='iceberg' table property in "
              + "an iceberg catalog, Please create table with 'connector'='iceberg' property in a non-iceberg catalog or "
              + "create table without 'connector'='iceberg' related properties in an iceberg table.");
    }
    checkArgument(table instanceof ResolvedCatalogTable, "table should be resolved");
    createIcebergTable(tablePath, (ResolvedCatalogTable) table, ignoreIfExists);
  }

  void createIcebergTable(ObjectPath tablePath, ResolvedCatalogTable table, boolean ignoreIfExists)
      throws CatalogException, TableAlreadyExistException {
    validateFlinkTable(table);
    Schema icebergSchema;
    icebergSchema = FlinkSchemaUtil.convert(table);
    PartitionSpec spec = toPartitionSpec(((CatalogTable) table).getPartitionKeys(), icebergSchema);
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    String location = null;
    for (Map.Entry<String, String> entry : table.getOptions().entrySet()) {
      if ("location".equalsIgnoreCase(entry.getKey())) {
        location = entry.getValue();
      } else {
        properties.put(entry.getKey(), entry.getValue());
      }
    }

    try {
      icebergCatalog.createTable(
          toIdentifier(tablePath), icebergSchema, spec, location, properties.build());
    } catch (AlreadyExistsException e) {
      if (!ignoreIfExists) {
        throw new TableAlreadyExistException(getName(), tablePath, e);
      }
    }
  }

  private static void validateTableSchemaAndPartition(CatalogTable ct1, CatalogTable ct2) {
    TableSchema ts1 = ct1.getSchema();
    TableSchema ts2 = ct2.getSchema();
    boolean equalsPrimary = false;

    if (ts1.getPrimaryKey().isPresent() && ts2.getPrimaryKey().isPresent()) {
      equalsPrimary =
          Objects.equals(ts1.getPrimaryKey().get().getType(), ts2.getPrimaryKey().get().getType())
              && Objects.equals(
                  ts1.getPrimaryKey().get().getColumns(), ts2.getPrimaryKey().get().getColumns());
    } else if (!ts1.getPrimaryKey().isPresent() && !ts2.getPrimaryKey().isPresent()) {
      equalsPrimary = true;
    }

    if (!(Objects.equals(ts1.getTableColumns(), ts2.getTableColumns())
        && Objects.equals(ts1.getWatermarkSpecs(), ts2.getWatermarkSpecs())
        && equalsPrimary)) {
      throw new UnsupportedOperationException(
          "Altering schema is not supported in the old alterTable API. "
              + "To alter schema, use the other alterTable API and provide a list of TableChange's.");
    }

    validateTablePartition(ct1, ct2);
  }

  private static void validateTablePartition(CatalogTable ct1, CatalogTable ct2) {
    if (!ct1.getPartitionKeys().equals(ct2.getPartitionKeys())) {
      throw new UnsupportedOperationException("Altering partition keys is not supported yet.");
    }
  }

  /**
   * This alterTable API only supports altering table properties.
   *
   * <p>Support for adding/removing/renaming columns cannot be done by comparing CatalogTable
   * instances, unless the Flink schema contains Iceberg column IDs.
   *
   * <p>To alter columns, use the other alterTable API and provide a list of TableChange's.
   *
   * @param tablePath path of the table or view to be modified
   * @param newTable the new table definition
   * @param ignoreIfNotExists flag to specify behavior when the table or view does not exist: if set
   *     to false, throw an exception, if set to true, do nothing.
   * @throws CatalogException in case of any runtime exception
   * @throws TableNotExistException if the table does not exist
   */
  @Override
  public void alterTable(ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
      throws CatalogException, TableNotExistException {
    validateFlinkTable(newTable);

    Table icebergTable;
    try {
      icebergTable = loadIcebergTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      } else {
        return;
      }
    }

    CatalogTable table = toCatalogTable(icebergTable);
    validateTableSchemaAndPartition(table, (CatalogTable) newTable);

    Map<String, String> oldProperties = table.getOptions();
    Map<String, String> setProperties = Maps.newHashMap();

    String setLocation = null;
    String setSnapshotId = null;
    String pickSnapshotId = null;

    for (Map.Entry<String, String> entry : newTable.getOptions().entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if (Objects.equals(value, oldProperties.get(key))) {
        continue;
      }

      if ("location".equalsIgnoreCase(key)) {
        setLocation = value;
      } else if ("current-snapshot-id".equalsIgnoreCase(key)) {
        setSnapshotId = value;
      } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(key)) {
        pickSnapshotId = value;
      } else {
        setProperties.put(key, value);
      }
    }

    oldProperties
        .keySet()
        .forEach(
            k -> {
              if (!newTable.getOptions().containsKey(k)) {
                setProperties.put(k, null);
              }
            });

    FlinkAlterTableUtil.commitChanges(
        icebergTable, setLocation, setSnapshotId, pickSnapshotId, setProperties);
  }

  @Override
  public void alterTable(
      ObjectPath tablePath,
      CatalogBaseTable newTable,
      List<TableChange> tableChanges,
      boolean ignoreIfNotExists)
      throws TableNotExistException, CatalogException {
    validateFlinkTable(newTable);

    Table icebergTable;
    try {
      icebergTable = loadIcebergTable(tablePath);
    } catch (TableNotExistException e) {
      if (!ignoreIfNotExists) {
        throw e;
      } else {
        return;
      }
    }

    // Does not support altering partition yet.
    validateTablePartition(toCatalogTable(icebergTable), (CatalogTable) newTable);

    String setLocation = null;
    String setSnapshotId = null;
    String cherrypickSnapshotId = null;

    List<TableChange> propertyChanges = Lists.newArrayList();
    List<TableChange> schemaChanges = Lists.newArrayList();
    for (TableChange change : tableChanges) {
      if (change instanceof TableChange.SetOption) {
        TableChange.SetOption set = (TableChange.SetOption) change;

        if ("location".equalsIgnoreCase(set.getKey())) {
          setLocation = set.getValue();
        } else if ("current-snapshot-id".equalsIgnoreCase(set.getKey())) {
          setSnapshotId = set.getValue();
        } else if ("cherry-pick-snapshot-id".equalsIgnoreCase(set.getKey())) {
          cherrypickSnapshotId = set.getValue();
        } else {
          propertyChanges.add(change);
        }
      } else if (change instanceof TableChange.ResetOption) {
        propertyChanges.add(change);
      } else {
        schemaChanges.add(change);
      }
    }

    FlinkAlterTableUtil.commitChanges(
        icebergTable,
        setLocation,
        setSnapshotId,
        cherrypickSnapshotId,
        schemaChanges,
        propertyChanges);
  }

  private static void validateFlinkTable(CatalogBaseTable table) {
    Preconditions.checkArgument(
        table instanceof CatalogTable, "The Table should be a CatalogTable.");

    TableSchema schema = table.getSchema();
    schema
        .getTableColumns()
        .forEach(
            column -> {
              if (!FlinkCompatibilityUtil.isPhysicalColumn(column)) {
                throw new UnsupportedOperationException(
                    "Creating table with computed columns is not supported yet.");
              }
            });

    if (!schema.getWatermarkSpecs().isEmpty()) {
      throw new UnsupportedOperationException(
          "Creating table with watermark specs is not supported yet.");
    }
  }

  private static PartitionSpec toPartitionSpec(List<String> partitionKeys, Schema icebergSchema) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(icebergSchema);
    partitionKeys.forEach(builder::identity);
    return builder.build();
  }

  private static List<String> toPartitionKeys(PartitionSpec spec, Schema icebergSchema) {
    ImmutableList.Builder<String> partitionKeysBuilder = ImmutableList.builder();
    for (PartitionField field : spec.fields()) {
      if (field.transform().isIdentity()) {
        partitionKeysBuilder.add(icebergSchema.findColumnName(field.sourceId()));
      } else {
        // Not created by Flink SQL.
        // For compatibility with iceberg tables, return empty.
        // TODO modify this after Flink support partition transform.
        return Collections.emptyList();
      }
    }
    return partitionKeysBuilder.build();
  }

  static CatalogTable toCatalogTable(Table table) {
    TableSchema schema = FlinkSchemaUtil.toSchema(table.schema());
    List<String> partitionKeys = toPartitionKeys(table.spec(), table.schema());

    // NOTE: We can not create a IcebergCatalogTable extends CatalogTable, because Flink optimizer
    // may use
    // CatalogTableImpl to copy a new catalog table.
    // Let's re-loading table from Iceberg catalog when creating source/sink operators.
    // Iceberg does not have Table comment, so pass a null (Default comment value in Flink).
    return new CatalogTableImpl(schema, partitionKeys, table.properties(), null);
  }

  @Override
  public Optional<Factory> getFactory() {
    return Optional.of(new FlinkDynamicTableFactory(this));
  }

  CatalogLoader getCatalogLoader() {
    return catalogLoader;
  }

  // ------------------------------ Unsupported methods
  // ---------------------------------------------

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
  public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition partition,
      boolean ignoreIfExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogPartition newPartition,
      boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String dbName) throws CatalogException {
    return Collections.emptyList();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath functionPath)
      throws FunctionNotExistException, CatalogException {
    throw new FunctionNotExistException(getName(), functionPath);
  }

  @Override
  public boolean functionExists(ObjectPath functionPath) throws CatalogException {
    return false;
  }

  @Override
  public void createFunction(
      ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(
      ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableStatistics(
      ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath tablePath, CatalogColumnStatistics columnStatistics, boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogTableStatistics partitionStatistics,
      boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath tablePath,
      CatalogPartitionSpec partitionSpec,
      CatalogColumnStatistics columnStatistics,
      boolean ignoreIfNotExists)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    Table table = loadIcebergTable(tablePath);

    if (table.spec().isUnpartitioned()) {
      throw new TableNotPartitionedException(icebergCatalog.name(), tablePath);
    }

    Set<CatalogPartitionSpec> set = Sets.newHashSet();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (DataFile dataFile : CloseableIterable.transform(tasks, FileScanTask::file)) {
        Map<String, String> map = Maps.newHashMap();
        StructLike structLike = dataFile.partition();
        PartitionSpec spec = table.specs().get(dataFile.specId());
        for (int i = 0; i < structLike.size(); i++) {
          map.put(spec.fields().get(i).name(), String.valueOf(structLike.get(i, Object.class)));
        }
        set.add(new CatalogPartitionSpec(map));
      }
    } catch (IOException e) {
      throw new CatalogException(
          String.format("Failed to list partitions of table %s", tablePath), e);
    }

    return Lists.newArrayList(set);
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath tablePath, List<Expression> filters) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  // After partition pruning and filter push down, the statistics have become very inaccurate, so
  // the statistics from
  // here are of little significance.
  // Flink will support something like SupportsReportStatistics in future.

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath tablePath) throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
      throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    return CatalogTableStatistics.UNKNOWN;
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath tablePath, CatalogPartitionSpec partitionSpec) throws CatalogException {
    return CatalogColumnStatistics.UNKNOWN;
  }
}
