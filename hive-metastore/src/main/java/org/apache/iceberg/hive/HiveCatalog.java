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
package org.apache.iceberg.hive;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileIOTracker;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, Configurable {
  public static final String LIST_ALL_TABLES = "list-all-tables";
  public static final String LIST_ALL_TABLES_DEFAULT = "false";

  public static final String HMS_TABLE_OWNER = "hive.metastore.table.owner";
  public static final String HMS_DB_OWNER = "hive.metastore.database.owner";
  public static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";

  // MetastoreConf is not available with current Hive version
  static final String HIVE_CONF_CATALOG = "metastore.catalog.default";

  private static final Logger LOG = LoggerFactory.getLogger(HiveCatalog.class);

  private String name;
  private Configuration conf;
  private FileIO fileIO;
  private ClientPool<IMetaStoreClient, TException> clients;
  private boolean listAllTables = false;
  private Map<String, String> catalogProperties;
  private FileIOTracker fileIOTracker;

  public HiveCatalog() {}

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.name = inputName;
    if (conf == null) {
      LOG.warn("No Hadoop Configuration was set, using the default environment Configuration");
      this.conf = new Configuration();
    }

    if (properties.containsKey(CatalogProperties.URI)) {
      this.conf.set(HiveConf.ConfVars.METASTOREURIS.varname, properties.get(CatalogProperties.URI));
    }

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.conf.set(
          HiveConf.ConfVars.METASTOREWAREHOUSE.varname,
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION)));
    }

    this.listAllTables =
        Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, LIST_ALL_TABLES_DEFAULT));

    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO =
        fileIOImpl == null
            ? new HadoopFileIO(conf)
            : CatalogUtil.loadFileIO(fileIOImpl, properties, conf);

    this.clients = new CachedClientPool(conf, properties);
    this.fileIOTracker = new FileIOTracker();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new ViewAwareTableBuilder(identifier, schema);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return new TableAwareViewBuilder(identifier);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    Preconditions.checkArgument(
        isValidateNamespace(namespace), "Missing database in namespace: %s", namespace);
    String database = namespace.level(0);

    try {
      List<String> tableNames = clients.run(client -> client.getAllTables(database));
      List<TableIdentifier> tableIdentifiers;

      if (listAllTables) {
        tableIdentifiers =
            tableNames.stream()
                .map(t -> TableIdentifier.of(namespace, t))
                .collect(Collectors.toList());
      } else {
        tableIdentifiers =
            listIcebergTables(
                tableNames, namespace, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
      }

      LOG.debug(
          "Listing of namespace: {} resulted in the following tables: {}",
          namespace,
          tableIdentifiers);
      return tableIdentifiers;

    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException("Failed to list all tables under namespace " + namespace, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listTables", e);
    }
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    Preconditions.checkArgument(
        isValidateNamespace(namespace), "Missing database in namespace: %s", namespace);

    try {
      String database = namespace.level(0);
      List<String> tableNames =
          clients.run(client -> client.getTables(database, "*", TableType.VIRTUAL_VIEW));

      // Retrieving the Table objects from HMS in batches to avoid OOM
      List<TableIdentifier> filteredTableIdentifiers = Lists.newArrayList();
      Iterable<List<String>> tableNameSets = Iterables.partition(tableNames, 100);

      for (List<String> tableNameSet : tableNameSets) {
        filteredTableIdentifiers.addAll(
            listIcebergTables(tableNameSet, namespace, HiveOperationsBase.ICEBERG_VIEW_TYPE_VALUE));
      }

      return filteredTableIdentifiers;
    } catch (UnknownDBException e) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException("Failed to list all views under namespace " + namespace, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to listViews", e);
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }

    String database = identifier.namespace().level(0);

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

    try {
      clients.run(
          client -> {
            client.dropTable(
                database,
                identifier.name(),
                false /* do not delete data */,
                false /* throw NoSuchObjectException if the table doesn't exist */);
            return null;
          });

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }

      LOG.info("Dropped table: {}", identifier);
      return true;

    } catch (NoSuchTableException | NoSuchObjectException e) {
      LOG.info("Skipping drop, table does not exist: {}", identifier, e);
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + identifier, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropTable", e);
    }
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    if (!isValidIdentifier(identifier)) {
      return false;
    }

    try {
      String database = identifier.namespace().level(0);
      String viewName = identifier.name();
      Table table = clients.run(client -> client.getTable(database, viewName));
      HiveOperationsBase.validateTableIsIcebergView(
          table, CatalogUtil.fullTableName(name, identifier));

      HiveViewOperations ops = (HiveViewOperations) newViewOps(identifier);
      ViewMetadata lastViewMetadata = null;

      try {
        lastViewMetadata = ops.current();
      } catch (NotFoundException e) {
        LOG.warn("Failed to load view metadata for view: {}", identifier, e);
      }

      clients.run(
          client -> {
            client.dropTable(database, viewName);
            return null;
          });

      if (lastViewMetadata != null) {
        CatalogUtil.dropViewMetadata(ops.io(), lastViewMetadata);
      }

      LOG.info("Dropped view: {}", identifier);
      return true;

    } catch (NoSuchObjectException e) {
      LOG.info("Skipping drop, view does not exist: {}", identifier, e);
      return false;
    } catch (TException e) {
      throw new RuntimeException("Failed to drop " + identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to dropView", e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier originalTo) {
    renameTableOrView(from, originalTo, HiveOperationsBase.ContentType.TABLE);
  }

  @Override
  @SuppressWarnings("FormatStringAnnotation")
  public void renameView(TableIdentifier from, TableIdentifier to) {
    renameTableOrView(from, to, HiveOperationsBase.ContentType.VIEW);
  }

  private List<TableIdentifier> listIcebergTables(
      List<String> tableNames, Namespace namespace, String tableTypeProp)
      throws TException, InterruptedException {
    List<Table> tableObjects =
        clients.run(client -> client.getTableObjectsByName(namespace.level(0), tableNames));
    return tableObjects.stream()
        .filter(
            table ->
                table.getParameters() != null
                    && tableTypeProp.equalsIgnoreCase(
                        table.getParameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP)))
        .map(table -> TableIdentifier.of(namespace, table.getTableName()))
        .collect(Collectors.toList());
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void renameTableOrView(
      TableIdentifier from,
      TableIdentifier originalTo,
      HiveOperationsBase.ContentType contentType) {
    if (!isValidIdentifier(from)) {
      switch (contentType) {
        case TABLE:
          throw new NoSuchTableException("Invalid identifier: %s", from);
        case VIEW:
          throw new NoSuchViewException("Invalid identifier: %s", from);
      }
    }

    TableIdentifier to = removeCatalogName(originalTo);
    Preconditions.checkArgument(isValidIdentifier(to), "Invalid identifier: %s", to);
    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException(
          "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
    }

    if (tableExists(to)) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          "Cannot rename %s to %s. Table already exists", from, to);
    }

    if (viewExists(to)) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          "Cannot rename %s to %s. View already exists", from, to);
    }

    String toDatabase = to.namespace().level(0);
    String fromDatabase = from.namespace().level(0);
    String fromName = from.name();

    try {
      Table table = clients.run(client -> client.getTable(fromDatabase, fromName));
      validateTableIsIcebergTableOrView(contentType, table, CatalogUtil.fullTableName(name, from));

      table.setDbName(toDatabase);
      table.setTableName(to.name());

      clients.run(
          client -> {
            MetastoreUtil.alterTable(client, fromDatabase, fromName, table);
            return null;
          });

      LOG.info("Renamed {} from {}, to {}", contentType.value(), from, to);

    } catch (NoSuchObjectException e) {
      switch (contentType) {
        case TABLE:
          throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
        case VIEW:
          throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      }

    } catch (InvalidOperationException e) {
      if (e.getMessage() != null
          && e.getMessage().contains(String.format("new table %s already exists", to))) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "Table already exists: %s", to);
      } else {
        throw new RuntimeException("Failed to rename " + from + " to " + to, e);
      }

    } catch (TException e) {
      throw new RuntimeException("Failed to rename " + from + " to " + to, e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to rename", e);
    }
  }

  private void validateTableIsIcebergTableOrView(
      HiveOperationsBase.ContentType contentType, Table table, String fullName) {
    switch (contentType) {
      case TABLE:
        HiveOperationsBase.validateTableIsIceberg(table, fullName);
        break;
      case VIEW:
        HiveOperationsBase.validateTableIsIcebergView(table, fullName);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> meta) {
    Preconditions.checkArgument(
        !namespace.isEmpty(), "Cannot create namespace with invalid name: %s", namespace);
    Preconditions.checkArgument(
        isValidateNamespace(namespace),
        "Cannot support multi part namespace in Hive Metastore: %s",
        namespace);
    Preconditions.checkArgument(
        meta.get(HMS_DB_OWNER_TYPE) == null || meta.get(HMS_DB_OWNER) != null,
        "Create namespace setting %s without setting %s is not allowed",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    try {
      clients.run(
          client -> {
            client.createDatabase(convertToDatabase(namespace, meta));
            return null;
          });

      LOG.info("Created namespace: {}", namespace);

    } catch (AlreadyExistsException e) {
      throw new org.apache.iceberg.exceptions.AlreadyExistsException(
          e, "Namespace already exists: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to create namespace " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to createDatabase(name) " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (!isValidateNamespace(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    if (!namespace.isEmpty()) {
      return ImmutableList.of();
    }
    try {
      List<Namespace> namespaces =
          clients.run(IMetaStoreClient::getAllDatabases).stream()
              .map(Namespace::of)
              .collect(Collectors.toList());

      LOG.debug("Listing namespace {} returned tables: {}", namespace, namespaces);
      return namespaces;

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list all namespace: " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to getAllDatabases() " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      return false;
    }

    try {
      clients.run(
          client -> {
            client.dropDatabase(
                namespace.level(0),
                false /* deleteData */,
                false /* ignoreUnknownDb */,
                false /* cascade */);
            return null;
          });

      LOG.info("Dropped namespace: {}", namespace);
      return true;

    } catch (InvalidOperationException e) {
      throw new NamespaceNotEmptyException(
          e, "Namespace %s is not empty. One or more tables exist.", namespace);

    } catch (NoSuchObjectException e) {
      return false;

    } catch (TException e) {
      throw new RuntimeException("Failed to drop namespace " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to drop dropDatabase(name) " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    Preconditions.checkArgument(
        (properties.get(HMS_DB_OWNER_TYPE) == null) == (properties.get(HMS_DB_OWNER) == null),
        "Setting %s and %s has to be performed together or not at all",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    Map<String, String> parameter = Maps.newHashMap();

    parameter.putAll(loadNamespaceMetadata(namespace));
    parameter.putAll(properties);
    Database database = convertToDatabase(namespace, parameter);

    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);

    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    Preconditions.checkArgument(
        properties.contains(HMS_DB_OWNER_TYPE) == properties.contains(HMS_DB_OWNER),
        "Removing %s and %s has to be performed together or not at all",
        HMS_DB_OWNER_TYPE,
        HMS_DB_OWNER);
    Map<String, String> parameter = Maps.newHashMap();

    parameter.putAll(loadNamespaceMetadata(namespace));
    properties.forEach(key -> parameter.put(key, null));
    Database database = convertToDatabase(namespace, parameter);

    alterHiveDataBase(namespace, database);
    LOG.debug("Successfully removed properties {} from {}", properties, namespace);

    // Always successful, otherwise exception is thrown
    return true;
  }

  private void alterHiveDataBase(Namespace namespace, Database database) {
    try {
      clients.run(
          client -> {
            client.alterDatabase(namespace.level(0), database);
            return null;
          });

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to getDatabase(name) " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    try {
      Database database = clients.run(client -> client.getDatabase(namespace.level(0)));
      Map<String, String> metadata = convertToMetadata(database);
      LOG.debug("Loaded metadata for namespace {} found {}", namespace, metadata.keySet());
      return metadata;

    } catch (NoSuchObjectException | UnknownDBException e) {
      throw new NoSuchNamespaceException(e, "Namespace does not exist: %s", namespace);

    } catch (TException e) {
      throw new RuntimeException(
          "Failed to list namespace under namespace: " + namespace + " in Hive Metastore", e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(
          "Interrupted in call to getDatabase(name) " + namespace + " in Hive Metastore", e);
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  private TableIdentifier removeCatalogName(TableIdentifier to) {
    if (isValidIdentifier(to)) {
      return to;
    }

    // check if the identifier includes the catalog name and remove it
    if (to.namespace().levels().length == 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
      return TableIdentifier.of(Namespace.of(to.namespace().level(1)), to.name());
    }

    // return the original unmodified
    return to;
  }

  private boolean isValidateNamespace(Namespace namespace) {
    return namespace.levels().length == 1;
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    HiveTableOperations ops =
        new HiveTableOperations(conf, clients, fileIO, name, dbName, tableName);
    fileIOTracker.track(ops);
    return ops;
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new HiveViewOperations(conf, clients, fileIO, name, identifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // This is a little edgy since we basically duplicate the HMS location generation logic.
    // Sadly I do not see a good way around this if we want to keep the order of events, like:
    // - Create meta files
    // - Create the metadata in HMS, and this way committing the changes

    // Create a new location based on the namespace / database if it is set on database level
    try {
      Database databaseData =
          clients.run(client -> client.getDatabase(tableIdentifier.namespace().levels()[0]));
      if (databaseData.getLocationUri() != null) {
        // If the database location is set use it as a base.
        return String.format("%s/%s", databaseData.getLocationUri(), tableIdentifier.name());
      }

    } catch (NoSuchObjectException e) {
      throw new NoSuchNamespaceException(
          e, "Namespace does not exist: %s", tableIdentifier.namespace().levels()[0]);
    } catch (TException e) {
      throw new RuntimeException(
          String.format("Metastore operation failed for %s", tableIdentifier), e);

    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during commit", e);
    }

    // Otherwise, stick to the {WAREHOUSE_DIR}/{DB_NAME}.db/{TABLE_NAME} path
    String databaseLocation = databaseLocation(tableIdentifier.namespace().levels()[0]);
    return String.format("%s/%s", databaseLocation, tableIdentifier.name());
  }

  private String databaseLocation(String databaseName) {
    String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    Preconditions.checkNotNull(
        warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    warehouseLocation = LocationUtil.stripTrailingSlash(warehouseLocation);
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  private Map<String, String> convertToMetadata(Database database) {

    Map<String, String> meta = Maps.newHashMap();

    meta.putAll(database.getParameters());
    meta.put("location", database.getLocationUri());
    if (database.getDescription() != null) {
      meta.put("comment", database.getDescription());
    }
    if (database.getOwnerName() != null) {
      meta.put(HMS_DB_OWNER, database.getOwnerName());
      if (database.getOwnerType() != null) {
        meta.put(HMS_DB_OWNER_TYPE, database.getOwnerType().name());
      }
    }

    return meta;
  }

  Database convertToDatabase(Namespace namespace, Map<String, String> meta) {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Database database = new Database();
    Map<String, String> parameter = Maps.newHashMap();

    database.setName(namespace.level(0));
    database.setLocationUri(databaseLocation(namespace.level(0)));

    meta.forEach(
        (key, value) -> {
          if (key.equals("comment")) {
            database.setDescription(value);
          } else if (key.equals("location")) {
            database.setLocationUri(value);
          } else if (key.equals(HMS_DB_OWNER)) {
            database.setOwnerName(value);
          } else if (key.equals(HMS_DB_OWNER_TYPE) && value != null) {
            database.setOwnerType(PrincipalType.valueOf(value));
          } else {
            if (value != null) {
              parameter.put(key, value);
            }
          }
        });

    if (database.getOwnerName() == null) {
      database.setOwnerName(HiveHadoopUtil.currentUser());
      database.setOwnerType(PrincipalType.USER);
    }

    database.setParameters(parameter);

    return database;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .add("uri", this.conf == null ? "" : this.conf.get(HiveConf.ConfVars.METASTOREURIS.varname))
        .toString();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (fileIOTracker != null) {
      fileIOTracker.close();
    }
  }

  @VisibleForTesting
  void setListAllTables(boolean listAllTables) {
    this.listAllTables = listAllTables;
  }

  @VisibleForTesting
  ClientPool<IMetaStoreClient, TException> clientPool() {
    return clients;
  }

  /**
   * The purpose of this class is to add view detection only for Hive-Specific tables. Hive catalog
   * follows checks at different levels: 1. During refresh, it validates if the table is an iceberg
   * table or not. 2. During commit, it validates if there is any concurrent commit with table or
   * table-name already exists. This class helps to do the validation on an early basis.
   */
  private class ViewAwareTableBuilder extends BaseMetastoreViewCatalogTableBuilder {

    private final TableIdentifier identifier;

    private ViewAwareTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      if (viewExists(identifier)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "View with same name already exists: %s", identifier);
      }
      return super.createOrReplaceTransaction();
    }

    @Override
    public org.apache.iceberg.Table create() {
      if (viewExists(identifier)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "View with same name already exists: %s", identifier);
      }
      return super.create();
    }
  }

  /**
   * The purpose of this class is to add table detection only for Hive-Specific view. Hive catalog
   * follows checks at different levels: 1. During refresh, it validates if the view is an iceberg
   * view or not. 2. During commit, it validates if there is any concurrent commit with view or
   * view-name already exists. This class helps to do the validation on an early basis.
   */
  private class TableAwareViewBuilder extends BaseViewBuilder {

    private final TableIdentifier identifier;

    private TableAwareViewBuilder(TableIdentifier identifier) {
      super(identifier);
      this.identifier = identifier;
    }

    @Override
    public View createOrReplace() {
      if (tableExists(identifier)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "Table with same name already exists: %s", identifier);
      }
      return super.createOrReplace();
    }

    @Override
    public View create() {
      if (tableExists(identifier)) {
        throw new org.apache.iceberg.exceptions.AlreadyExistsException(
            "Table with same name already exists: %s", identifier);
      }
      return super.create();
    }
  }
}
