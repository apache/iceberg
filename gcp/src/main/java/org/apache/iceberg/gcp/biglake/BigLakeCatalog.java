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
import java.io.IOException;
import java.util.HashMap;
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
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iceberg BigLake Metastore (BLMS) Catalog implementation. */
public final class BigLakeCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Object> {

  // TODO: to move the configs to GCPProperties.java.
  // User provided properties.
  // The endpoint of BigLake API. Optional, default to DEFAULT_BIGLAKE_SERVICE_ENDPOINT.
  public static final String PROPERTIES_KEY_BIGLAKE_ENDPOINT = "biglake.endpoint";
  // The GCP project ID. Required.
  public static final String PROPERTIES_KEY_GCP_PROJECT = "biglake.project-id";
  // The GCP location (https://cloud.google.com/bigquery/docs/locations). Optional, default to
  // DEFAULT_GCP_LOCATION.
  public static final String PROPERTIES_KEY_GCP_LOCATION = "biglake.location";
  // The BLMS catalog ID. It is the container resource of databases and tables.
  // It links a BLMS catalog with this Iceberg catalog.
  public static final String PROPERTIES_KEY_BLMS_CATALOG = "biglake.catalog";

  public static final String DEFAULT_BIGLAKE_SERVICE_ENDPOINT = "biglake.googleapis.com:443";
  public static final String DEFAULT_GCP_LOCATION = "us";

  private static final Logger LOG = LoggerFactory.getLogger(BigLakeCatalog.class);

  // The name of this Iceberg catalog plugin: spark.sql.catalog.<catalog_plugin>.
  private String catalogPulginName;
  private Map<String, String> catalogProperties;
  private FileIO fileIO;
  private Object conf;
  private String projectId;
  private String location;
  // BLMS catalog ID and fully qualified name.
  private String catalogId;
  private CatalogName catalogName;
  private BigLakeClient client;

  // Must have a no-arg constructor to be dynamically loaded
  // initialize(String name, Map<String, String> properties) will be called to complete
  // initialization
  public BigLakeCatalog() {}

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(PROPERTIES_KEY_GCP_PROJECT), "GCP project must be specified");
    String propProjectId = properties.get(PROPERTIES_KEY_GCP_PROJECT);
    String propLocation =
        properties.getOrDefault(PROPERTIES_KEY_GCP_LOCATION, DEFAULT_GCP_LOCATION);
    BigLakeClient newClient;
    try {
      // TODO: to add more auth options of the client. Currently it uses default auth
      // (https://github.com/googleapis/google-cloud-java#application-default-credentials)
      // that works on GCP services (e.g., GCE, GKE, Dataproc).
      newClient =
          new BigLakeClientImpl(
              properties.getOrDefault(
                  PROPERTIES_KEY_BIGLAKE_ENDPOINT, DEFAULT_BIGLAKE_SERVICE_ENDPOINT),
              propProjectId,
              propLocation);
    } catch (IOException e) {
      throw new ServiceFailureException(e, "Creating BigLake client failed");
    }
    initialize(inputName, properties, propProjectId, propLocation, newClient);
  }

  @VisibleForTesting
  void initialize(
      String inputName,
      Map<String, String> properties,
      String propProjectId,
      String propLocation,
      BigLakeClient bigLakeClient) {
    this.catalogPulginName = inputName;
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.projectId = propProjectId;
    this.location = propLocation;
    Preconditions.checkNotNull(bigLakeClient, "BigLake client must not be null");
    this.client = bigLakeClient;

    // Users can specify the BigLake catalog ID, otherwise catalog plugin will be used.
    this.catalogId = properties.getOrDefault(PROPERTIES_KEY_BLMS_CATALOG, inputName);
    this.catalogName = CatalogName.of(projectId, location, catalogId);
    LOG.info("Use BigLake catalog: {}", catalogName.toString());

    String fileIOImpl =
        properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, ResolvingFileIO.class.getName());
    this.fileIO = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new BigLakeTableOperations(
        client,
        fileIO,
        getTableName(getDatabaseId(identifier.namespace()), /* tableId= */ identifier.name()));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    String locationUri = getDatabase(identifier.namespace()).getHiveOptions().getLocationUri();
    return String.format(
        "%s/%s",
        Strings.isNullOrEmpty(locationUri)
            ? getDatabaseLocation(getDatabaseId(identifier.namespace()))
            : locationUri,
        identifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    // When deleting a BLMS catalog via `DROP NAMESPACE <catalog>`, this method is called for
    // verifying catalog emptiness. `namespace` is empty in this case, we list databases in
    // this catalog instead.
    // TODO: to return all tables in all databases in a BLMS catalog instead of a "placeholder".
    if (namespace.levels().length == 0) {
      return Iterables.isEmpty(client.listDatabases(catalogName))
          ? ImmutableList.of()
          : ImmutableList.of(TableIdentifier.of("placeholder"));
    }
    return Streams.stream(client.listTables(getDatabaseName(namespace)))
        .map(BigLakeCatalog::getTableIdentifier)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    // TODO: to catch NotFoundException as in https://github.com/apache/iceberg/pull/5510.
    TableMetadata lastMetadata = ops.current();
    client.deleteTable(
        getTableName(getDatabaseId(identifier.namespace()), /* tableId= */ identifier.name()));
    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    String fromDbId = getDatabaseId(from.namespace());
    String toDbId = getDatabaseId(to.namespace());

    Preconditions.checkArgument(
        fromDbId.equals(toDbId), "New table name must be in the same database");
    client.renameTable(getTableName(fromDbId, from.name()), getTableName(toDbId, to.name()));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespace.levels().length == 0) {
      // Used by `CREATE NAMESPACE <catalog>`. Create a BLMS catalog linked with Iceberg catalog.
      client.createCatalog(catalogName, Catalog.getDefaultInstance());
      LOG.info("Created BigLake catalog: {}", catalogName.toString());
    } else if (namespace.levels().length == 1) {
      // Create a database.
      String dbId = namespace.level(0);
      Database.Builder builder = Database.newBuilder().setType(Database.Type.HIVE);
      builder
          .getHiveOptionsBuilder()
          .putAllParameters(metadata)
          .setLocationUri(getDatabaseLocation(dbId));

      client.createDatabase(DatabaseName.of(projectId, location, catalogId, dbId), builder.build());
    } else {
      throw new IllegalArgumentException(invalidNamespaceMessage(namespace));
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (namespace.levels().length != 0) {
      // BLMS does not support namespaces under database or tables, returns empty.
      // It is called when dropping a namespace to make sure it's empty (listTables is called as
      // well), returns empty to unblock deletion.
      return ImmutableList.of();
    }
    return Streams.stream(client.listDatabases(catalogName))
        .map(BigLakeCatalog::getNamespace)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    if (namespace.levels().length == 0) {
      // Used by `DROP NAMESPACE <catalog>`. Deletes the BLMS catalog linked by Iceberg catalog.
      client.deleteCatalog(catalogName);
      LOG.info("Deleted BigLake catalog: {}", catalogName.toString());
    } else if (namespace.levels().length == 1) {
      client.deleteDatabase(getDatabaseName(namespace));
      // We don't delete the data file folder for safety. It aligns with HMS's default behavior.
      // We can support database or catalog level config controlling file deletion in future.
    } else {
      throw new IllegalArgumentException(invalidNamespaceMessage(namespace));
    }
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    Database.Builder builder;
    try {
      builder = getDatabase(namespace).toBuilder();
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "setProperties is only supported for tables and databases, namespace {} is not supported",
          namespace.levels().length == 0 ? "empty" : namespace.toString(),
          e);
      return false;
    }
    HiveDatabaseOptions.Builder optionsBuilder = builder.getHiveOptionsBuilder();
    properties.forEach(optionsBuilder::putParameters);
    client.updateDatabaseParameters(getDatabaseName(namespace), optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    Database.Builder builder;
    try {
      builder = getDatabase(namespace).toBuilder();
    } catch (IllegalArgumentException e) {
      LOG.warn(
          "removeProperties is only supported for tables and databases, namespace {} is not"
              + " supported",
          namespace.levels().length == 0 ? "empty" : namespace.toString(),
          e);
      return false;
    }
    HiveDatabaseOptions.Builder optionsBuilder = builder.getHiveOptionsBuilder();
    properties.forEach(optionsBuilder::removeParameters);
    client.updateDatabaseParameters(getDatabaseName(namespace), optionsBuilder.getParametersMap());
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    if (namespace.levels().length == 0) {
      // Calls getCatalog to check existence. BLMS catalog has no metadata today.
      client.getCatalog(catalogName);
      return new HashMap<String, String>();
    } else if (namespace.levels().length == 1) {
      return getMetadata(getDatabase(namespace));
    } else {
      throw new IllegalArgumentException(invalidNamespaceMessage(namespace));
    }
  }

  @Override
  public String name() {
    return catalogPulginName;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  private String getDatabaseLocation(String dbId) {
    String warehouseLocation =
        LocationUtil.stripTrailingSlash(
            catalogProperties.get(CatalogProperties.WAREHOUSE_LOCATION));
    Preconditions.checkNotNull(warehouseLocation, "Data warehouse location is not set");
    return String.format("%s/%s.db", LocationUtil.stripTrailingSlash(warehouseLocation), dbId);
  }

  private static TableIdentifier getTableIdentifier(Table table) {
    TableName tableName = TableName.parse(table.getName());
    return TableIdentifier.of(Namespace.of(tableName.getDatabase()), tableName.getTable());
  }

  private static Namespace getNamespace(Database db) {
    return Namespace.of(DatabaseName.parse(db.getName()).getDatabase());
  }

  private TableName getTableName(String dbId, String tableId) {
    return TableName.of(projectId, location, catalogId, dbId, tableId);
  }

  private String getDatabaseId(Namespace namespace) {
    Preconditions.checkArgument(
        namespace.levels().length == 1,
        "BigLake database namespace must use format <catalog>.<database>, invalid namespace: %s",
        namespace);
    return namespace.level(0);
  }

  private DatabaseName getDatabaseName(Namespace namespace) {
    return DatabaseName.of(projectId, location, catalogId, getDatabaseId(namespace));
  }

  private Database getDatabase(Namespace namespace) {
    return client.getDatabase(getDatabaseName(namespace));
  }

  private static Map<String, String> getMetadata(Database db) {
    HiveDatabaseOptions options = db.getHiveOptions();
    Map<String, String> result = Maps.newHashMap();
    result.putAll(options.getParameters());
    result.put("location", options.getLocationUri());
    return result;
  }

  private static String invalidNamespaceMessage(Namespace namespace) {
    return String.format(
        "BigLake catalog namespace can have zero (catalog) or one level (database), invalid"
            + " namespace: %s",
        namespace);
  }
}
