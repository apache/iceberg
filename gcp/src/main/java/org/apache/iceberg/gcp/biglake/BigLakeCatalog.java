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
package org.apache.iceberg.gcp.biglake;

import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.HiveDatabaseOptions;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iceberg BigLake Metastore (BLMS) Catalog implementation. */
public final class BigLakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  public static final String DEFAULT_BIGLAKE_SERVICE_ENDPOINT = "biglake.googleapis.com:443";

  private static final Logger LOG = LoggerFactory.getLogger(BigLakeCatalog.class);

  // The name of this Iceberg catalog plugin.
  private String name;
  private Map<String, String> properties;
  private FileIO io;
  private Object conf;

  private BigLakeClient client;

  private String projectId;
  private String region;
  // BLMS catalog ID and fully qualified name.
  private String catalogId;
  private CatalogName catalogName;
  private String warehouseLocation;

  private CloseableGroup closeableGroup;

  // Must have a no-arg constructor to be dynamically loaded
  // initialize(String name, Map<String, String> properties) will be called to complete
  // initialization
  public BigLakeCatalog() {}

  @Override
  public void initialize(String initName, Map<String, String> initProperties) {
    BigLakeClient initClient;
    try {
      // CatalogProperties.URI specifies the endpoint of BigLake API.
      // TODO: to add more auth options of the client. Currently it uses default auth
      // (https://github.com/googleapis/google-cloud-java#application-default-credentials)
      // that works on GCP services (e.g., GCE, GKE, Dataproc).
      initClient =
          new BigLakeClient(
              initProperties.getOrDefault(CatalogProperties.URI, DEFAULT_BIGLAKE_SERVICE_ENDPOINT));
    } catch (IOException e) {
      throw new ServiceFailureException(e, "Failed to create BigLake client");
    }

    initialize(initName, initProperties, initClient);
  }

  @VisibleForTesting
  void initialize(String initName, Map<String, String> initProperties, BigLakeClient initClient) {
    this.name = initName;
    this.properties = ImmutableMap.copyOf(initProperties);

    Preconditions.checkArgument(initClient != null, "BigLake client must not be null");
    this.client = initClient;

    Preconditions.checkArgument(
        properties.containsKey(GCPProperties.PROJECT_ID), "GCP project ID must be specified");
    this.projectId = properties.get(GCPProperties.PROJECT_ID);

    Preconditions.checkArgument(
        properties.containsKey(GCPProperties.REGION), "GCP region must be specified");
    this.region = properties.get(GCPProperties.REGION);

    // Users can specify the BigLake catalog ID, otherwise catalog plugin name will be used.
    // For example, "spark.sql.catalog.<plugin-name>=org.apache.iceberg.spark.SparkCatalog"
    // specifies the plugin name "<plugin-name>".
    this.catalogId = properties.getOrDefault(GCPProperties.BIGLAKE_CATALOG_ID, initName);
    this.catalogName = CatalogName.of(projectId, region, catalogId);

    Preconditions.checkArgument(
        properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION),
        "Data warehouse location must be specified");
    this.warehouseLocation =
        LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION));

    String ioImpl =
        properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName());
    this.io = CatalogUtil.loadFileIO(ioImpl, properties, conf);

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(client);
    closeableGroup.addCloseable(io);
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    String dbId = databaseId(identifier.namespace());
    return new BigLakeTableOperations(client, io, name(), tableName(dbId, identifier.name()));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    String dbId = databaseId(identifier.namespace());
    String locationUri = loadDatabase(dbId).getHiveOptions().getLocationUri();
    return String.format(
        "%s/%s",
        Strings.isNullOrEmpty(locationUri) ? newDbLocation(dbId) : locationUri, identifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    ImmutableList<DatabaseName> dbNames;
    if (namespace.isEmpty()) {
      dbNames =
          Streams.stream(client.listDatabases(catalogName))
              .map(db -> DatabaseName.parse(db.getName()))
              .collect(ImmutableList.toImmutableList());
    } else {
      if (namespace.levels().length != 1) {
        throw new NoSuchNamespaceException(
            "Invalid namespace: %s", namespace.isEmpty() ? "empty" : namespace);
      }

      dbNames = ImmutableList.of(databaseName(databaseId(namespace)));
    }

    ImmutableList.Builder<TableIdentifier> result = ImmutableList.builder();
    return dbNames.stream()
        .flatMap(
            dbName ->
                Streams.stream(client.listTables(dbName)).map(BigLakeCatalog::tableIdentifier))
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    if (!isValidIdentifier(identifier)) {
      throw new NoSuchTableException("Invalid identifier: %s", identifier);
    }

    TableOperations ops = null;
    TableMetadata lastMetadata = null;
    if (purge) {
      ops = newTableOps(identifier);

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
      String dbId = databaseId(identifier.namespace());
      client.deleteTable(tableName(dbId, identifier.name()));
    } catch (NoSuchTableException e) {
      LOG.warn("Table not exist or permission denied", e);
      return false;
    }

    if (ops != null && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (!isValidIdentifier(from)) {
      throw new NoSuchTableException("Invalid identifier: %s", from);
    }

    if (!isValidIdentifier(to)) {
      throw new NoSuchTableException("Invalid identifier: %s", to);
    }

    String fromDb = databaseId(from.namespace());
    String toDb = databaseId(to.namespace());
    Preconditions.checkArgument(
        fromDb.equals(toDb),
        "Cannot rename table %s to %s: database must match",
        from.toString(),
        to.toString());

    client.renameTable(tableName(fromDb, from.name()), tableName(toDb, to.name()));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespace.levels().length > 1) {
      throw new IllegalArgumentException(
          String.format("Invalid namespace (too long): %s", namespace));
    }

    if (namespace.isEmpty()) {
      // Used by `CREATE NAMESPACE <catalog>`. Create a BLMS catalog linked with Iceberg catalog.
      client.createCatalog(catalogName, Catalog.getDefaultInstance());
      LOG.info("Created BigLake catalog: {}", catalogName.toString());
    } else if (namespace.levels().length == 1) {
      // Create a database.
      String dbId = databaseId(namespace);
      Database.Builder builder = Database.newBuilder().setType(Database.Type.HIVE);
      builder
          .getHiveOptionsBuilder()
          .putAllParameters(metadata)
          .setLocationUri(newDbLocation(dbId));

      client.createDatabase(DatabaseName.of(projectId, region, catalogId, dbId), builder.build());
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new NoSuchNamespaceException("Invalid namespace (too long): %s", namespace);
    }

    // Database namespace does not have nested namespaces.
    if (namespace.levels().length == 1) {
      return ImmutableList.of();
    }

    return Streams.stream(client.listDatabases(catalogName))
        .map(BigLakeCatalog::namespace)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    try {
      if (namespace.isEmpty()) {
        // Used by `DROP NAMESPACE <catalog>`. Deletes the BLMS catalog linked by Iceberg catalog.
        client.deleteCatalog(catalogName);
        LOG.info("Deleted BigLake catalog: {}", catalogName.toString());
      } else if (namespace.levels().length == 1) {
        client.deleteDatabase(databaseName(databaseId(namespace)));
        // Don't delete the data file folder for safety. It aligns with HMS's default behavior.
        // To support database or catalog level config controlling file deletion in future.
      } else {
        return false;
      }
    } catch (NoSuchNamespaceException e) {
      LOG.warn("Namespace not exist or permission denied", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> props) {
    if (namespace.levels().length != 1) {
      throw new NoSuchNamespaceException(
          "Invalid namespace: %s", namespace.isEmpty() ? "empty" : namespace);
    }

    String dbId = databaseId(namespace);
    HiveDatabaseOptions.Builder optionsBuilder =
        loadDatabase(dbId).toBuilder().getHiveOptionsBuilder();
    props.forEach(optionsBuilder::putParameters);
    client.updateDatabaseParameters(databaseName(dbId), optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> props) {
    if (namespace.levels().length != 1) {
      throw new NoSuchNamespaceException(
          "Invalid namespace: %s", namespace.isEmpty() ? "empty" : namespace);
    }

    String dbId = databaseId(namespace);
    HiveDatabaseOptions.Builder optionsBuilder =
        loadDatabase(dbId).toBuilder().getHiveOptionsBuilder();
    props.forEach(optionsBuilder::removeParameters);
    client.updateDatabaseParameters(databaseName(dbId), optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (namespace.levels().length > 1) {
      throw new NoSuchNamespaceException("Invalid namespace (too long): %s", namespace);
    }

    if (namespace.levels().length == 1) {
      return loadDatabase(databaseId(namespace)).getHiveOptions().getParametersMap();
    }

    // Calls catalog to check existence. BLMS catalog has no metadata today.
    client.catalog(catalogName);
    return ImmutableMap.of();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  protected Map<String, String> properties() {
    return properties == null ? ImmutableMap.of() : properties;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    return tableIdentifier.namespace().levels().length == 1;
  }

  /** Returns a new location path of a database, used when it is not already known. */
  private String newDbLocation(String dbId) {
    return String.format("%s/%s.db", warehouseLocation, dbId);
  }

  private static TableIdentifier tableIdentifier(Table table) {
    TableName tableName = TableName.parse(table.getName());
    return TableIdentifier.of(Namespace.of(tableName.getDatabase()), tableName.getTable());
  }

  private static Namespace namespace(Database db) {
    return Namespace.of(DatabaseName.parse(db.getName()).getDatabase());
  }

  private TableName tableName(String dbId, String tableId) {
    return TableName.of(projectId, region, catalogId, dbId, tableId);
  }

  private static String databaseId(Namespace namespace) {
    return namespace.levels().length == 1 ? namespace.level(0) : null;
  }

  private DatabaseName databaseName(String dbId) {
    return DatabaseName.of(projectId, region, catalogId, dbId);
  }

  private Database loadDatabase(String dbId) {
    return client.database(databaseName(dbId));
  }
}
