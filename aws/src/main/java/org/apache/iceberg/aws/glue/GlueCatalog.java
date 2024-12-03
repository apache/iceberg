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
package org.apache.iceberg.aws.glue;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.Transactions;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIOTracker;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.LockManagers;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.InvalidInputException;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.glue.GlueCatalogExtensions;
import software.amazon.glue.GlueCatalogSessionExtensions;
import software.amazon.glue.GlueExtensionsEndpoint;
import software.amazon.glue.GlueExtensionsPaths;
import software.amazon.glue.GlueExtensionsProperties;
import software.amazon.glue.GlueUtil;

public class GlueCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Configuration> {

  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalog.class);

  private GlueClient glue;
  private Object hadoopConf;
  private String catalogName;
  private String warehousePath;
  private AwsProperties awsProperties;
  private S3FileIOProperties s3FileIOProperties;
  private LockManager lockManager;
  private CloseableGroup closeableGroup;
  private Map<String, String> catalogProperties;
  private FileIOTracker fileIOTracker;
  private GlueCatalogExtensions extensions;
  private boolean extensionsEnabled;

  // Attempt to set versionId if available on the path
  private static final DynMethods.UnboundMethod SET_VERSION_ID =
      DynMethods.builder("versionId")
          .hiddenImpl(
              "software.amazon.awssdk.services.glue.model.UpdateTableRequest$Builder", String.class)
          .orNoop()
          .build();

  /**
   * No-arg constructor to load the catalog dynamically.
   *
   * <p>All fields are initialized by calling {@link GlueCatalog#initialize(String, Map)} later.
   */
  public GlueCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogProperties = ImmutableMap.copyOf(properties);
    AwsClientFactory awsClientFactory;
    if (PropertyUtil.propertyAsBoolean(
        properties,
        AwsProperties.GLUE_LAKEFORMATION_ENABLED,
        AwsProperties.GLUE_LAKEFORMATION_ENABLED_DEFAULT)) {
      String factoryImpl =
          PropertyUtil.propertyAsString(properties, AwsProperties.CLIENT_FACTORY, null);
      ImmutableMap.Builder<String, String> builder =
          ImmutableMap.<String, String>builder().putAll(properties);
      if (factoryImpl == null) {
        builder.put(AwsProperties.CLIENT_FACTORY, LakeFormationAwsClientFactory.class.getName());
      }

      this.catalogProperties = builder.buildOrThrow();
      awsClientFactory = AwsClientFactories.from(catalogProperties);
      Preconditions.checkArgument(
          awsClientFactory instanceof LakeFormationAwsClientFactory,
          "Detected LakeFormation enabled for Glue catalog, should use a client factory that extends %s, but found %s",
          LakeFormationAwsClientFactory.class.getName(),
          factoryImpl);
    } else {
      awsClientFactory = AwsClientFactories.from(properties);
    }

    initialize(
        name,
        properties.get(CatalogProperties.WAREHOUSE_LOCATION),
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        awsClientFactory.glue(),
        initializeLockManager(properties));
  }

  private LockManager initializeLockManager(Map<String, String> properties) {
    if (properties.containsKey(CatalogProperties.LOCK_IMPL)) {
      return LockManagers.from(properties);
    } else if (SET_VERSION_ID.isNoop()) {
      LOG.warn(
          "Optimistic locking is not available in the environment. Using in-memory lock manager."
              + " To ensure atomic transaction, please configure a distributed lock manager"
              + " such as the DynamoDB lock manager.");
      return LockManagers.defaultLockManager();
    } else {
      LOG.debug("Using optimistic locking for Glue Data Catalog tables.");
    }
    return null;
  }

  @VisibleForTesting
  void initialize(
      String name,
      String path,
      AwsProperties properties,
      S3FileIOProperties s3Properties,
      GlueClient client,
      LockManager lock,
      Map<String, String> catalogProps) {
    this.catalogProperties = catalogProps;
    initialize(name, path, properties, s3Properties, client, lock);
  }

  @VisibleForTesting
  void initialize(
      String name,
      String path,
      AwsProperties properties,
      S3FileIOProperties s3Properties,
      GlueClient client,
      LockManager lock) {
    this.catalogName = name;
    this.awsProperties = properties;
    this.s3FileIOProperties = s3Properties;
    this.warehousePath = Strings.isNullOrEmpty(path) ? null : LocationUtil.stripTrailingSlash(path);
    this.glue = client;
    this.lockManager = lock;
    this.extensionsEnabled =
        PropertyUtil.propertyAsBoolean(
            catalogProperties, GlueExtensionsProperties.GLUE_EXTENSIONS_ENABLED, true);
    if (extensionsEnabled) {
      this.extensions =
          new GlueCatalogExtensions(
              new GlueCatalogSessionExtensions(
                  name,
                  catalogProperties,
                  new GlueExtensionsProperties(catalogProperties),
                  GlueExtensionsEndpoint.from(catalogProperties),
                  GlueExtensionsPaths.from(awsProperties.glueCatalogId()),
                  this));
    }

    this.closeableGroup = new CloseableGroup();
    this.fileIOTracker = new FileIOTracker();
    closeableGroup.addCloseable(glue);
    closeableGroup.addCloseable(lockManager);
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.addCloseable(fileIOTracker);
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    if (catalogProperties != null) {
      ImmutableMap.Builder<String, String> tableSpecificCatalogPropertiesBuilder =
          ImmutableMap.<String, String>builder().putAll(catalogProperties);
      boolean skipNameValidation = awsProperties.glueCatalogSkipNameValidation();

      if (s3FileIOProperties.writeTableTagEnabled()) {
        tableSpecificCatalogPropertiesBuilder.put(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(S3FileIOProperties.S3_TAG_ICEBERG_TABLE),
            IcebergToGlueConverter.getTableName(tableIdentifier, skipNameValidation));
      }

      if (s3FileIOProperties.isWriteNamespaceTagEnabled()) {
        tableSpecificCatalogPropertiesBuilder.put(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(
                S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE),
            IcebergToGlueConverter.getDatabaseName(tableIdentifier, skipNameValidation));
      }

      if (awsProperties.glueLakeFormationEnabled()) {
        tableSpecificCatalogPropertiesBuilder
            .put(
                AwsProperties.LAKE_FORMATION_DB_NAME,
                IcebergToGlueConverter.getDatabaseName(tableIdentifier, skipNameValidation))
            .put(
                AwsProperties.LAKE_FORMATION_TABLE_NAME,
                IcebergToGlueConverter.getTableName(tableIdentifier, skipNameValidation))
            .put(S3FileIOProperties.PRELOAD_CLIENT_ENABLED, String.valueOf(true));
      }

      // FileIO initialization depends on tableSpecificCatalogProperties, so a new FileIO is
      // initialized each time
      GlueTableOperations glueTableOperations =
          new GlueTableOperations(
              glue,
              lockManager,
              catalogName,
              awsProperties,
              tableSpecificCatalogPropertiesBuilder.buildOrThrow(),
              hadoopConf,
              tableIdentifier);
      fileIOTracker.track(glueTableOperations);
      return glueTableOperations;
    }

    GlueTableOperations glueTableOperations =
        new GlueTableOperations(
            glue,
            lockManager,
            catalogName,
            awsProperties,
            catalogProperties,
            hadoopConf,
            tableIdentifier);
    fileIOTracker.track(glueTableOperations);
    return glueTableOperations;
  }

  /**
   * This method produces the same result as using a HiveCatalog. If databaseUri exists for the Glue
   * database URI, the default location is databaseUri/tableName. If not, the default location is
   * warehousePath/databaseName.db/tableName
   *
   * @param tableIdentifier table id
   * @return default warehouse path
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    // check if value is set in database
    GetDatabaseResponse response =
        glue.getDatabase(
            GetDatabaseRequest.builder()
                .catalogId(awsProperties.glueCatalogId())
                .name(
                    IcebergToGlueConverter.getDatabaseName(
                        tableIdentifier, awsProperties.glueCatalogSkipNameValidation()))
                .build());
    String dbLocationUri = response.database().locationUri();
    if (dbLocationUri != null) {
      dbLocationUri = LocationUtil.stripTrailingSlash(dbLocationUri);
      return String.format("%s/%s", dbLocationUri, tableIdentifier.name());
    }

    ValidationException.check(
        !Strings.isNullOrEmpty(warehousePath),
        "Cannot derive default warehouse location, warehouse path must not be null or empty");

    return String.format(
        "%s/%s.db/%s",
        warehousePath,
        IcebergToGlueConverter.getDatabaseName(
            tableIdentifier, awsProperties.glueCatalogSkipNameValidation()),
        tableIdentifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    namespaceExists(namespace);
    // should be safe to list all before returning the list, instead of dynamically load the list.
    String nextToken = null;
    List<TableIdentifier> results = Lists.newArrayList();
    do {
      GetTablesResponse response =
          glue.getTables(
              GetTablesRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .databaseName(
                      IcebergToGlueConverter.toDatabaseName(
                          namespace, awsProperties.glueCatalogSkipNameValidation()))
                  .nextToken(nextToken)
                  .build());
      nextToken = response.nextToken();
      if (response.hasTableList()) {
        results.addAll(
            response.tableList().stream()
                .filter(this::isGlueIcebergTable)
                .map(GlueToIcebergConverter::toTableId)
                .collect(Collectors.toList()));
      }
    } while (nextToken != null);

    LOG.debug("Listing of namespace: {} resulted in the following tables: {}", namespace, results);
    return results;
  }

  private boolean isGlueIcebergTable(Table table) {
    return table.parameters() != null
        && BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(
            table.parameters().get(BaseMetastoreTableOperations.TABLE_TYPE_PROP));
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      GlueTableOperations ops = (GlueTableOperations) newTableOps(identifier);
      Table glueTable = ops.getGlueTable();
      if (extensionsEnabled && extensions.useExtensionsForGlueTable(glueTable)) {
        return extensions.dropTable(identifier, purge);
      }

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
      glue.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(
                  IcebergToGlueConverter.getDatabaseName(
                      identifier, awsProperties.glueCatalogSkipNameValidation()))
              .name(identifier.name())
              .build());
      LOG.info("Successfully dropped table {} from Glue", identifier);
      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
        LOG.info("Glue table {} data purged", identifier);
      }
      LOG.info("Dropped table: {}", identifier);
      return true;
    } catch (EntityNotFoundException e) {
      LOG.error("Cannot drop table {} because table not found or not accessible", identifier, e);
      return false;
    } catch (Exception e) {
      LOG.error(
          "Cannot complete drop table operation for {} due to unexpected exception", identifier, e);
      throw e;
    }
  }

  /**
   * Rename table in Glue is a drop table and create table.
   *
   * @param from identifier of the table to rename
   * @param to new table name
   */
  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    // check new namespace exists
    if (!namespaceExists(to.namespace())) {
      throw new NoSuchNamespaceException(
          "Cannot rename %s to %s because namespace %s does not exist", from, to, to.namespace());
    }
    // keep metadata
    Table fromTable;
    String fromTableDbName =
        IcebergToGlueConverter.getDatabaseName(from, awsProperties.glueCatalogSkipNameValidation());
    String fromTableName =
        IcebergToGlueConverter.getTableName(from, awsProperties.glueCatalogSkipNameValidation());
    String toTableDbName =
        IcebergToGlueConverter.getDatabaseName(to, awsProperties.glueCatalogSkipNameValidation());
    String toTableName =
        IcebergToGlueConverter.getTableName(to, awsProperties.glueCatalogSkipNameValidation());
    try {
      GetTableResponse response =
          glue.getTable(
              GetTableRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .databaseName(fromTableDbName)
                  .name(fromTableName)
                  .build());
      fromTable = response.table();
    } catch (EntityNotFoundException e) {
      throw new NoSuchTableException(
          e, "Cannot rename %s because the table does not exist in Glue", from);
    }

    if (extensionsEnabled && extensions.useExtensionsForGlueTable(fromTable)) {
      extensions.renameTable(from, to);
      return;
    }

    // use the same Glue info to create the new table, pointing to the old metadata
    TableInput.Builder tableInputBuilder =
        TableInput.builder()
            .owner(fromTable.owner())
            .tableType(fromTable.tableType())
            .parameters(fromTable.parameters())
            .storageDescriptor(fromTable.storageDescriptor());

    glue.createTable(
        CreateTableRequest.builder()
            .catalogId(awsProperties.glueCatalogId())
            .databaseName(toTableDbName)
            .tableInput(tableInputBuilder.name(toTableName).build())
            .build());
    LOG.info("created rename destination table {}", to);

    try {
      dropTable(from, false);
    } catch (Exception e) {
      // rollback, delete renamed table
      LOG.error(
          "Fail to drop old table {} after renaming to {}, rollback to use the old table",
          from,
          to,
          e);
      glue.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(toTableDbName)
              .name(toTableName)
              .build());
      throw e;
    }

    LOG.info("Successfully renamed table from {} to {}", from, to);
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (extensionsEnabled && GlueUtil.useExtensionsForIcebergNamespace(metadata)) {
      extensions.namespaces().createNamespace(namespace, metadata);
    }

    try {
      glue.createDatabase(
          CreateDatabaseRequest.builder()
              .catalogId(awsProperties.glueCatalogId())
              .databaseInput(
                  IcebergToGlueConverter.toDatabaseInput(
                      namespace, metadata, awsProperties.glueCatalogSkipNameValidation()))
              .build());
      LOG.info("Created namespace: {}", namespace);
    } catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
      throw new AlreadyExistsException(
          "Cannot create namespace %s because it already exists in Glue", namespace);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    if (!namespace.isEmpty()) {
      // if it is not a list all op, just check if the namespace exists and return empty.
      if (namespaceExists(namespace)) {
        return Lists.newArrayList();
      }
      throw new NoSuchNamespaceException(
          "Glue does not support nested namespace, cannot list namespaces under %s", namespace);
    }

    // should be safe to list all before returning the list, instead of dynamically load the list.
    String nextToken = null;
    List<Namespace> results = Lists.newArrayList();
    do {
      GetDatabasesResponse response =
          glue.getDatabases(
              GetDatabasesRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .nextToken(nextToken)
                  .build());
      nextToken = response.nextToken();
      if (response.hasDatabaseList()) {
        results.addAll(
            response.databaseList().stream()
                .map(GlueToIcebergConverter::toNamespace)
                .collect(Collectors.toList()));
      }
    } while (nextToken != null);

    LOG.debug("Listing namespace {} returned namespaces: {}", namespace, results);
    return results;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    String databaseName =
        IcebergToGlueConverter.toDatabaseName(
            namespace, awsProperties.glueCatalogSkipNameValidation());
    try {
      Database database =
          glue.getDatabase(
                  GetDatabaseRequest.builder()
                      .catalogId(awsProperties.glueCatalogId())
                      .name(databaseName)
                      .build())
              .database();
      Map<String, String> result = Maps.newHashMap(database.parameters());

      if (database.locationUri() != null) {
        result.put(
            IcebergToGlueConverter.GLUE_DB_LOCATION_KEY,
            LocationUtil.stripTrailingSlash(database.locationUri()));
      }

      if (database.description() != null) {
        result.put(IcebergToGlueConverter.GLUE_DESCRIPTION_KEY, database.description());
      }

      LOG.debug("Loaded metadata for namespace {} found {}", namespace, result);
      return result;
    } catch (InvalidInputException e) {
      throw new NoSuchNamespaceException(
          "invalid input for namespace %s, error message: %s", namespace, e.getMessage());
    } catch (EntityNotFoundException e) {
      throw new NoSuchNamespaceException(
          "fail to find Glue database for namespace %s, error message: %s",
          databaseName, e.getMessage());
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    String databaseName =
        IcebergToGlueConverter.toDatabaseName(
            namespace, awsProperties.glueCatalogSkipNameValidation());
    try {
      Database database =
          glue.getDatabase(
                  GetDatabaseRequest.builder()
                      .catalogId(awsProperties.glueCatalogId())
                      .name(databaseName)
                      .build())
              .database();

      if (extensionsEnabled && extensions.useExtensionsForGlueDatabase(database)) {
        return extensions.dropNamespace(namespace);
      }
    } catch (InvalidInputException e) {
      throw new NoSuchNamespaceException(
          "invalid input for namespace %s, error message: %s", namespace, e.getMessage());
    } catch (EntityNotFoundException e) {
      throw new NoSuchNamespaceException(
          "fail to find Glue database for namespace %s, error message: %s",
          databaseName, e.getMessage());
    }

    GetTablesResponse response =
        glue.getTables(
            GetTablesRequest.builder()
                .catalogId(awsProperties.glueCatalogId())
                .databaseName(
                    IcebergToGlueConverter.toDatabaseName(
                        namespace, awsProperties.glueCatalogSkipNameValidation()))
                .build());

    if (response.hasTableList() && !response.tableList().isEmpty()) {
      Table table = response.tableList().get(0);
      if (isGlueIcebergTable(table)) {
        throw new NamespaceNotEmptyException(
            "Cannot drop namespace %s because it still contains Iceberg tables", namespace);
      } else {
        throw new NamespaceNotEmptyException(
            "Cannot drop namespace %s because it still contains non-Iceberg tables", namespace);
      }
    }

    glue.deleteDatabase(
        DeleteDatabaseRequest.builder()
            .catalogId(awsProperties.glueCatalogId())
            .name(
                IcebergToGlueConverter.toDatabaseName(
                    namespace, awsProperties.glueCatalogSkipNameValidation()))
            .build());
    LOG.info("Dropped namespace: {}", namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(loadNamespaceMetadata(namespace));
    newProperties.putAll(properties);
    glue.updateDatabase(
        UpdateDatabaseRequest.builder()
            .catalogId(awsProperties.glueCatalogId())
            .name(
                IcebergToGlueConverter.toDatabaseName(
                    namespace, awsProperties.glueCatalogSkipNameValidation()))
            .databaseInput(
                IcebergToGlueConverter.toDatabaseInput(
                    namespace, newProperties, awsProperties.glueCatalogSkipNameValidation()))
            .build());
    LOG.debug("Successfully set properties {} for {}", properties.keySet(), namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    Map<String, String> metadata = Maps.newHashMap(loadNamespaceMetadata(namespace));
    for (String property : properties) {
      metadata.remove(property);
    }

    glue.updateDatabase(
        UpdateDatabaseRequest.builder()
            .catalogId(awsProperties.glueCatalogId())
            .name(
                IcebergToGlueConverter.toDatabaseName(
                    namespace, awsProperties.glueCatalogSkipNameValidation()))
            .databaseInput(
                IcebergToGlueConverter.toDatabaseInput(
                    namespace, metadata, awsProperties.glueCatalogSkipNameValidation()))
            .build());
    LOG.debug("Successfully removed properties {} from {}", properties, namespace);
    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  protected boolean isValidIdentifier(TableIdentifier tableIdentifier) {
    if (awsProperties.glueCatalogSkipNameValidation()) {
      return true;
    }

    return IcebergToGlueConverter.isValidNamespace(tableIdentifier.namespace())
        && IcebergToGlueConverter.isValidTableName(tableIdentifier.name());
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public org.apache.iceberg.Table loadTable(TableIdentifier identifier) {
    org.apache.iceberg.Table result;
    if (isValidIdentifier(identifier)) {
      GlueTableOperations ops = (GlueTableOperations) newTableOps(identifier);
      Table table = ops.getGlueTable();
      if (extensionsEnabled && extensions.useExtensionsForGlueTable(table)) {
        GlueTable glueTable = (GlueTable) extensions.loadTable(identifier);
        glueTable.setGlueColumnTypeMappingFromGlueServiceTable(table);
        glueTable.setOriginalOps(ops);
        return glueTable;
      }

      if (table != null) {
        if (!isGlueIcebergTable(table)) {
          throw new NoSuchIcebergTableException(
              "Glue table %s is not an Iceberg table", table.name());
        }
      }

      if (ops.current() == null) {
        // the identifier may be valid for both tables and metadata tables
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);

        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }

      } else {
        result = new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
      }

    } else if (isValidMetadataIdentifier(identifier)) {
      result = loadMetadataTable(identifier);

    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    LOG.info("Table loaded by catalog: {}", result);
    return result;
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  private org.apache.iceberg.Table loadMetadataTable(TableIdentifier identifier) {
    String tableName = identifier.name();
    MetadataTableType type = MetadataTableType.from(tableName);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      GlueTableOperations ops = (GlueTableOperations) newTableOps(baseTableIdentifier);
      Table glueTable = ops.getGlueTable();
      if (extensionsEnabled && extensions.useExtensionsForGlueTable(glueTable)) {
        return extensions.loadTable(identifier);
      }

      if (glueTable != null) {
        if (!isGlueIcebergTable(glueTable)) {
          throw new NoSuchIcebergTableException(
              "Glue table %s is not an Iceberg table", glueTable.name());
        }
      }

      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", baseTableIdentifier);
      }

      return MetadataTableUtils.createMetadataTableInstance(
          ops, name(), baseTableIdentifier, identifier, type);
    } else {
      throw new NoSuchTableException("Table does not exist: %s", identifier);
    }
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new GlueCatalogTableBuilder(identifier, schema);
  }

  @VisibleForTesting
  void setExtensions(GlueCatalogExtensions extensions) {
    this.extensions = extensions;
  }

  private class GlueCatalogTableBuilder implements TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final Map<String, String> tableProperties = Maps.newHashMap();
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();
    private String location = null;

    GlueCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid table identifier: %s", identifier);

      this.identifier = identifier;
      this.schema = schema;
      this.tableProperties.putAll(tableDefaultProperties());
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec newSpec) {
      this.spec = newSpec != null ? newSpec : PartitionSpec.unpartitioned();
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder newSortOrder) {
      this.sortOrder = newSortOrder != null ? newSortOrder : SortOrder.unsorted();
      return this;
    }

    @Override
    public TableBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      if (properties != null) {
        tableProperties.putAll(properties);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      tableProperties.put(key, value);
      return this;
    }

    @Override
    public org.apache.iceberg.Table create() {
      if (extensionsEnabled && GlueUtil.useExtensionsForIcebergTable(tableProperties)) {
        GlueTable glueTable =
            (GlueTable)
                extensions
                    .buildTable(identifier, schema)
                    .withLocation(location)
                    .withSortOrder(sortOrder)
                    .withPartitionSpec(spec)
                    .withProperties(tableProperties)
                    .create();
        GlueTableOperations ops = (GlueTableOperations) newTableOps(identifier);
        Table glueServiceTable = ops.getGlueTable();
        glueTable.setGlueColumnTypeMappingFromGlueServiceTable(glueServiceTable);
        glueTable.setOriginalOps(ops);
        return glueTable;
      }

      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);

      try {
        ops.commit(null, metadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("Table was created concurrently: %s", identifier);
      }

      return new BaseTable(ops, fullTableName(name(), identifier), metricsReporter());
    }

    @Override
    public Transaction createTransaction() {
      if (extensionsEnabled && GlueUtil.useExtensionsForIcebergTable(tableProperties)) {
        return extensions
            .buildTable(identifier, schema)
            .withLocation(location)
            .withSortOrder(sortOrder)
            .withPartitionSpec(spec)
            .withProperties(tableProperties)
            .createTransaction();
      }

      TableOperations ops = newTableOps(identifier);
      if (ops.current() != null) {
        throw new AlreadyExistsException("Table already exists: %s", identifier);
      }

      String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
      tableProperties.putAll(tableOverrideProperties());
      TableMetadata metadata =
          TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      return Transactions.createTableTransaction(identifier.toString(), ops, metadata);
    }

    @Override
    public Transaction replaceTransaction() {
      if (extensionsEnabled && GlueUtil.useExtensionsForIcebergTable(tableProperties)) {
        return extensions
            .buildTable(identifier, schema)
            .withLocation(location)
            .withSortOrder(sortOrder)
            .withPartitionSpec(spec)
            .withProperties(tableProperties)
            .replaceTransaction();
      }

      return newReplaceTableTransaction(false);
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      if (extensionsEnabled && GlueUtil.useExtensionsForIcebergTable(tableProperties)) {
        return extensions
            .buildTable(identifier, schema)
            .withLocation(location)
            .withSortOrder(sortOrder)
            .withPartitionSpec(spec)
            .withProperties(tableProperties)
            .createOrReplaceTransaction();
      }

      return newReplaceTableTransaction(true);
    }

    private Transaction newReplaceTableTransaction(boolean orCreate) {
      TableOperations ops = newTableOps(identifier);
      if (!orCreate && ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      TableMetadata metadata;
      tableProperties.putAll(tableOverrideProperties());
      if (ops.current() != null) {
        String baseLocation = location != null ? location : ops.current().location();
        metadata =
            ops.current().buildReplacement(schema, spec, sortOrder, baseLocation, tableProperties);
      } else {
        String baseLocation = location != null ? location : defaultWarehouseLocation(identifier);
        metadata =
            TableMetadata.newTableMetadata(schema, spec, sortOrder, baseLocation, tableProperties);
      }

      if (orCreate) {
        return Transactions.createOrReplaceTableTransaction(identifier.toString(), ops, metadata);
      } else {
        return Transactions.replaceTableTransaction(identifier.toString(), ops, metadata);
      }
    }

    private Map<String, String> tableDefaultProperties() {
      Map<String, String> tableDefaultProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_DEFAULT_PREFIX);
      LOG.info(
          "Table properties set at catalog level through catalog properties: {}",
          tableDefaultProperties);
      return tableDefaultProperties;
    }

    private Map<String, String> tableOverrideProperties() {
      Map<String, String> tableOverrideProperties =
          PropertyUtil.propertiesWithPrefix(properties(), CatalogProperties.TABLE_OVERRIDE_PREFIX);
      LOG.info(
          "Table properties enforced at catalog level through catalog properties: {}",
          tableOverrideProperties);
      return tableOverrideProperties;
    }
  }
}
