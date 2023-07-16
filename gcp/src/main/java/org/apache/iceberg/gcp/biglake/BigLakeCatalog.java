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

  private String projectId;
  private String region;
  // BLMS catalog ID and fully qualified name.
  private String catalogId;
  private CatalogName catalogName;
  private BigLakeClient client;

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
      throw new ServiceFailureException(e, "Creating BigLake client failed");
    }

    initialize(initName, initProperties, initClient);
  }

  @VisibleForTesting
  void initialize(String initName, Map<String, String> initProperties, BigLakeClient initClient) {
    this.name = initName;
    this.properties = ImmutableMap.copyOf(initProperties);

    Preconditions.checkNotNull(initClient, "BigLake client must not be null");
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

    String ioImpl =
        properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName());
    this.io = CatalogUtil.loadFileIO(ioImpl, properties, conf);

    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(io);
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    String db = databaseId(identifier.namespace());
    if (db == null) {
      throwInvalidDbNamespaceError(identifier.namespace());
    }

    return new BigLakeTableOperations(client, io, name(), tableName(db, identifier.name()));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    String db = databaseId(identifier.namespace());
    if (db == null) {
      throwInvalidDbNamespaceError(identifier.namespace());
    }

    String locationUri = loadDatabase(identifier.namespace()).getHiveOptions().getLocationUri();
    return String.format(
        "%s/%s",
        Strings.isNullOrEmpty(locationUri) ? databaseLocation(db) : locationUri, identifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    ImmutableList<DatabaseName> dbNames = ImmutableList.of();
    if (namespace.isEmpty()) {
      dbNames =
          Streams.stream(client.listDatabases(catalogName))
              .map(db -> DatabaseName.parse(db.getName()))
              .collect(ImmutableList.toImmutableList());
    } else if (namespace.levels().length == 1) {
      dbNames = ImmutableList.of(databaseName(namespace));
    } else {
      throwInvalidDbNamespaceError(namespace);
    }

    ImmutableList.Builder<TableIdentifier> result = ImmutableList.builder();
    dbNames.stream()
        .map(
            dbName ->
                Streams.stream(client.listTables(dbName))
                    .map(BigLakeCatalog::tableIdentifier)
                    .collect(ImmutableList.toImmutableList()))
        .forEach(result::addAll);
    return result.build();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    String db = databaseId(identifier.namespace());
    if (db == null) {
      throwInvalidDbNamespaceError(identifier.namespace());
    }

    TableOperations ops = null;
    TableMetadata lastMetadata = null;
    if (purge) {
      ops = newTableOps(identifier);
      // TODO: to catch NotFoundException as in https://github.com/apache/iceberg/pull/5510.
      lastMetadata = ops.current();
    }

    try {
      client.deleteTable(tableName(db, identifier.name()));
    } catch (NoSuchTableException e) {
      return false;
    }

    if (ops != null && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    String fromDb = databaseId(from.namespace());
    if (fromDb == null) {
      throwInvalidDbNamespaceError(from.namespace());
    }

    String toDb = databaseId(to.namespace());
    if (toDb == null) {
      throwInvalidDbNamespaceError(to.namespace());
    }

    Preconditions.checkArgument(
        fromDb.equals(toDb),
        "Cannot rename table %s to %s: database must match",
        from.toString(),
        to.toString());
    client.renameTable(tableName(fromDb, from.name()), tableName(toDb, to.name()));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
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
          .setLocationUri(databaseLocation(dbId));

      client.createDatabase(DatabaseName.of(projectId, region, catalogId, dbId), builder.build());
    } else {
      throw new IllegalArgumentException(
          String.format("Invalid namespace (too long): %s", namespace));
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (namespace.isEmpty()) {
      return Streams.stream(client.listDatabases(catalogName))
          .map(BigLakeCatalog::namespace)
          .collect(ImmutableList.toImmutableList());
    }

    // Database namespace does not have nested namespaces.
    if (namespace.levels().length == 1) {
      return ImmutableList.of();
    }

    throw new NoSuchNamespaceException("Invalid namespace: %s", namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    try {
      if (namespace.isEmpty()) {
        // Used by `DROP NAMESPACE <catalog>`. Deletes the BLMS catalog linked by Iceberg catalog.
        client.deleteCatalog(catalogName);
        LOG.info("Deleted BigLake catalog: {}", catalogName.toString());
      } else if (namespace.levels().length == 1) {
        client.deleteDatabase(databaseName(namespace));
        // Don't delete the data file folder for safety. It aligns with HMS's default behavior.
        // To support database or catalog level config controlling file deletion in future.
      } else {
        LOG.warn("Invalid namespace (too long): {}", namespace);
        return false;
      }
    } catch (NoSuchNamespaceException e) {
      LOG.warn("Failed to drop namespace", e);
      return false;
    }

    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> props) {
    DatabaseName dbName = databaseName(namespace);
    if (dbName == null) {
      throwInvalidDbNamespaceError(namespace);
    }

    HiveDatabaseOptions.Builder optionsBuilder =
        loadDatabase(namespace).toBuilder().getHiveOptionsBuilder();
    props.forEach(optionsBuilder::putParameters);
    client.updateDatabaseParameters(dbName, optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> props) {
    DatabaseName dbName = databaseName(namespace);
    if (dbName == null) {
      throwInvalidDbNamespaceError(namespace);
    }

    HiveDatabaseOptions.Builder optionsBuilder =
        loadDatabase(namespace).toBuilder().getHiveOptionsBuilder();
    props.forEach(optionsBuilder::removeParameters);
    client.updateDatabaseParameters(dbName, optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (namespace.isEmpty()) {
      // Calls getCatalog to check existence. BLMS catalog has no metadata today.
      client.getCatalog(catalogName);
      return ImmutableMap.of();
    } else if (namespace.levels().length == 1) {
      return loadDatabase(namespace).getHiveOptions().getParametersMap();
    } else {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
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

  private String databaseLocation(String dbId) {
    String warehouseLocation =
        LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    Preconditions.checkNotNull(warehouseLocation, "Data warehouse location is not set");
    return String.format("%s/%s.db", LocationUtil.stripTrailingSlash(warehouseLocation), dbId);
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

  private String databaseId(Namespace namespace) {
    return namespace.levels().length == 1 ? namespace.level(0) : null;
  }

  private DatabaseName databaseName(Namespace namespace) {
    String db = databaseId(namespace);
    return db == null ? null : DatabaseName.of(projectId, region, catalogId, db);
  }

  private Database loadDatabase(Namespace namespace) {
    return client.getDatabase(databaseName(namespace));
  }

  private void throwInvalidDbNamespaceError(Namespace namespace) {
    throw new NoSuchNamespaceException(
        "Invalid BigLake database namespace: %s", namespace.isEmpty() ? "empty" : namespace);
  }
}
