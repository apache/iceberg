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
package org.apache.iceberg.aws.s3tables;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIOTracker;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3tables.S3TablesClient;
import software.amazon.awssdk.services.s3tables.model.ConflictException;
import software.amazon.awssdk.services.s3tables.model.CreateNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.CreateTableRequest;
import software.amazon.awssdk.services.s3tables.model.DeleteNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.DeleteTableRequest;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceRequest;
import software.amazon.awssdk.services.s3tables.model.GetNamespaceResponse;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationRequest;
import software.amazon.awssdk.services.s3tables.model.GetTableMetadataLocationResponse;
import software.amazon.awssdk.services.s3tables.model.ListNamespacesRequest;
import software.amazon.awssdk.services.s3tables.model.ListNamespacesResponse;
import software.amazon.awssdk.services.s3tables.model.ListTablesRequest;
import software.amazon.awssdk.services.s3tables.model.ListTablesResponse;
import software.amazon.awssdk.services.s3tables.model.NotFoundException;
import software.amazon.awssdk.services.s3tables.model.OpenTableFormat;
import software.amazon.awssdk.services.s3tables.model.RenameTableRequest;

public class S3TablesCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(S3TablesCatalog.class);

  private String catalogName;
  private Map<String, String> catalogOptions;
  private CloseableGroup closeableGroup;
  private FileIOTracker fileIOTracker;

  private Object hadoopConf;
  private S3TablesClient tablesClient;

  private static final ImmutableMap<String, String> S3_TABLES_DEFAULT_PROPERTIES =
      ImmutableMap.of(
          // S3 Tables does not support deleting objects
          S3FileIOProperties.DELETE_ENABLED, "false");

  // must have a no-arg constructor to be dynamically loaded
  // initialize(String name, Map<String, String> properties) will be called to complete
  // initialization
  public S3TablesCatalog() {}

  /**
   * Overrides loadTable to return an instance of S3TablesTable rather than BaseTable. Some engines
   * use this to detect the type of the table and apply S3 Tables-specific behavior.
   */
  @Override
  public Table loadTable(TableIdentifier identifier) {
    Table result;
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        // the identifier may be valid for both tables and metadata tables
        if (isValidMetadataIdentifier(identifier)) {
          result = loadMetadataTable(identifier);
        } else {
          throw new NoSuchTableException("Table does not exist: %s", identifier);
        }
      } else {
        result = new S3TablesTable(ops, fullTableName(name(), identifier), metricsReporter());
      }
    } else if (isValidMetadataIdentifier(identifier)) {
      result = loadMetadataTable(identifier);
    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }

    LOG.info("Table loaded by catalog: {}", result);
    return result;
  }

  // Copied from BaseMetastoreCatalog, but private there
  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }

  // Copied from BaseMetastoreCatalog, but private there
  private Table loadMetadataTable(TableIdentifier identifier) {
    String tableName = identifier.name();
    MetadataTableType type = MetadataTableType.from(tableName);
    if (type != null) {
      TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      TableOperations ops = newTableOps(baseTableIdentifier);
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
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    if (tableIdentifier.namespace() == null || tableIdentifier.namespace().levels().length == 0) {
      throw new ValidationException("Namespace can't be null or empty");
    }
    validateSingleLevelNamespace(tableIdentifier.namespace());
    String namespaceName = tableIdentifier.namespace().toString();
    String tableName = tableIdentifier.name();

    S3TablesCatalogOperations s3TablesCatalogOperations =
        new S3TablesCatalogOperations(
            tablesClient,
            namespaceName,
            tableName,
            catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION),
            catalogOptions,
            hadoopConf);

    fileIOTracker.track(s3TablesCatalogOperations);
    return s3TablesCatalogOperations;
  }

  /**
   * Currently just checking with the Control Plane APIs to find if a table exists, if not then
   * create it.
   */
  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    validateSingleLevelNamespace(tableIdentifier.namespace());
    try {
      LOG.debug(
          "Trying to get TableMetadataLocation for namespace: {}, name: {}",
          tableIdentifier.namespace(),
          tableIdentifier.name());
      GetTableMetadataLocationResponse getTableMetadataLocationResponse =
          tablesClient.getTableMetadataLocation(
              GetTableMetadataLocationRequest.builder()
                  .name(tableIdentifier.name())
                  .namespace(tableIdentifier.namespace().toString())
                  .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                  .build());

      return getTableMetadataLocationResponse.warehouseLocation();
    } catch (NotFoundException ex) {
      LOG.info(
          "Table {} does not exist, creating table to retrieve warehouse location",
          tableIdentifier.name());

      try {
        tablesClient.createTable(
            CreateTableRequest.builder()
                .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                .name(tableIdentifier.name())
                .format(OpenTableFormat.ICEBERG)
                .namespace(tableIdentifier.namespace().toString())
                .build());

      } catch (Exception e) {
        LOG.error("Failed to create table {}", tableIdentifier.name(), e);
        throw new RuntimeException(e);
      }
      try {
        GetTableMetadataLocationResponse getTableResponse =
            tablesClient.getTableMetadataLocation(
                GetTableMetadataLocationRequest.builder()
                    .name(tableIdentifier.name())
                    .namespace(tableIdentifier.namespace().toString())
                    .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                    .build());
        return getTableResponse.warehouseLocation();
      } catch (Exception e) {
        LOG.error("Failed to get table {}", tableIdentifier.name(), e);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    LOG.debug("initialize {}", properties);
    AwsClientFactory awsClientFactory = AwsClientFactories.from(properties);
    initialize(name, properties, awsClientFactory.s3tables());
  }

  public void initialize(String name, Map<String, String> properties, S3TablesClient client) {
    if (properties.get(CatalogProperties.WAREHOUSE_LOCATION) == null) {
      throw new ValidationException(
          "No Warehouse location provided. Please specify the warehouse location, which should be the table bucket ARN");
    }

    validateUnsupportedCatalogProperties(properties);

    this.catalogOptions =
        ImmutableMap.<String, String>builder()
            .putAll(S3_TABLES_DEFAULT_PROPERTIES)
            .putAll(properties)
            .buildKeepingLast();

    this.closeableGroup = new CloseableGroup();
    this.catalogName = name;
    this.tablesClient = client;
    this.fileIOTracker = new FileIOTracker();

    closeableGroup.addCloseable(this.tablesClient);
    closeableGroup.addCloseable(fileIOTracker);
    closeableGroup.setSuppressCloseFailure(true);
  }

  /**
   * Validate some common properties that aren't supported by S3 Tables. We only log warnings rather
   * than failing to preserve potential forward compatibility.
   */
  private void validateUnsupportedCatalogProperties(Map<String, String> properties) {
    if (PropertyUtil.propertyAsBoolean(properties, S3FileIOProperties.DELETE_ENABLED, false)) {
      LOG.warn(
          "S3 Tables does not support DeleteObject requests; setting {}=true will cause failures",
          S3FileIOProperties.DELETE_ENABLED);
    }
    if (!PropertyUtil.propertiesWithPrefix(properties, S3FileIOProperties.DELETE_TAGS_PREFIX)
        .isEmpty()) {
      LOG.warn(
          "S3 Tables does not support tagging objects; setting {} properties will cause failures",
          S3FileIOProperties.DELETE_TAGS_PREFIX);
    }
    if (!PropertyUtil.propertiesWithPrefix(properties, S3FileIOProperties.WRITE_TAGS_PREFIX)
        .isEmpty()) {
      LOG.warn(
          "S3 Tables does not support tagging objects; setting {} properties will cause failures",
          S3FileIOProperties.WRITE_TAGS_PREFIX);
    }
    if (PropertyUtil.propertyAsBoolean(
        properties, S3FileIOProperties.S3_ACCESS_GRANTS_ENABLED, false)) {
      LOG.warn(
          "S3 Tables does not support S3 Access Grants; setting {}=true will cause failures",
          S3FileIOProperties.S3_ACCESS_GRANTS_ENABLED);
    }
    String sseConfig =
        PropertyUtil.propertyAsString(
            properties, S3FileIOProperties.SSE_TYPE, S3FileIOProperties.SSE_TYPE_NONE);
    if (!sseConfig.equals(S3FileIOProperties.SSE_TYPE_NONE)
        && !sseConfig.equals(S3FileIOProperties.SSE_TYPE_S3)) {
      LOG.warn(
          "S3 Tables does not support configuring SSE other than SSE-S3; setting {}={} will cause failures",
          S3FileIOProperties.SSE_TYPE,
          sseConfig);
    }
    String aclConfig = properties.get(S3FileIOProperties.ACL);
    if (aclConfig != null) {
      LOG.warn(
          "S3 Tables does not support ACLs; setting {}={} will cause failures",
          S3FileIOProperties.ACL,
          aclConfig);
    }
    String storageClassConfig = properties.get(S3FileIOProperties.WRITE_STORAGE_CLASS);
    if (storageClassConfig != null && !storageClassConfig.equals("STANDARD")) {
      LOG.warn(
          "S3 Tables does not support storage classes other than STANDARD; setting {}={} will cause failures",
          S3FileIOProperties.WRITE_STORAGE_CLASS,
          storageClassConfig);
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    validateSingleLevelNamespace(namespace);
    LOG.info("Creating namespace {} with metadata {}", namespace, metadata);
    try {
      tablesClient.createNamespace(
          CreateNamespaceRequest.builder()
              .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
              .namespace(Collections.singletonList(namespace.toString()))
              .build());
    } catch (ConflictException ex) {
      LOG.debug("Received exception: ", ex);
      LOG.info("Namespace {} already exists", namespace);
      throw new AlreadyExistsException("Namespace already exists");
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    LOG.debug("Listing namespaces for {}", namespace);
    if (!namespace.isEmpty()) {
      LOG.error("S3TablesCatalog does not support more than 1 level of namespace");
      throw new IllegalArgumentException(
          String.format(
              "S3TablesCatalog does not support more than 1 level of "
                  + "namespace, so can only list top-level namespaces, but got: %s",
              namespace));
    }
    List<Namespace> results = Lists.newArrayList();
    try {
      listWithToken(
          continuationToken -> {
            ListNamespacesResponse response =
                tablesClient.listNamespaces(
                    ListNamespacesRequest.builder()
                        .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                        .build());
            results.addAll(
                response.namespaces().stream()
                    .map(namespaceSummary -> Namespace.of(namespaceSummary.namespace().get(0)))
                    .collect(Collectors.toList()));
            return response.continuationToken();
          });
    } catch (Exception e) {
      LOG.error("Failed to list namespaces", e);
      throw new RuntimeException(e);
    }
    LOG.debug("Namespace results: {}", results);
    return results;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    validateSingleLevelNamespace(namespace);
    try {
      LOG.debug("Loading metadata for {}", namespace);
      GetNamespaceResponse getNamespaceResponse =
          tablesClient.getNamespace(
              GetNamespaceRequest.builder()
                  .namespace(namespace.toString())
                  .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                  .build());
      LOG.debug("Loaded metadata {}", getNamespaceResponse.toString());

      return ImmutableMap.of("namespaceName", getNamespaceResponse.toString());
    } catch (NotFoundException ex) {
      throw new NoSuchNamespaceException(ex, "Namespace not found!");
    } catch (Exception ex) {
      LOG.error("Failed to load namespace metadata", ex);
      throw new RuntimeException(
          String.format(
              "Failed to load namespace metadata for %s: %s",
              ex.getClass().getName(), ex.getMessage()));
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    try {
      LOG.debug("Loading namespaces for {} inorder to drop them", namespace);
      GetNamespaceResponse getNamespaceResponse =
          this.tablesClient.getNamespace(
              GetNamespaceRequest.builder()
                  .namespace(namespace.toString())
                  .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                  .build());

      getNamespaceResponse
          .namespace()
          .forEach(
              name -> {
                LOG.debug("Deleting namespace {}", Namespace.of(name));

                this.tablesClient.deleteNamespace(
                    DeleteNamespaceRequest.builder()
                        .namespace(namespace.toString())
                        .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                        .build());
              });

      return true;
    } catch (ConflictException | NamespaceNotEmptyException ex) {
      LOG.error("Failed to delete namespace because it is not empty", ex);
      throw ex;
    } catch (NotFoundException ex) {
      LOG.debug("Received exception: ", ex);
      LOG.debug("Namespace: {} not found", namespace);
      return false;
    } catch (Exception ex) {
      LOG.error("Failed to delete namespace", ex);
      throw ex;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    Map<String, String> newProperties = Maps.newHashMap();
    newProperties.putAll(loadNamespaceMetadata(namespace));
    newProperties.putAll(properties);

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

    // Always successful, otherwise exception is thrown
    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    LOG.debug("Listing tables for {}", namespace);
    namespaceExists(namespace);
    List<TableIdentifier> results = Lists.newArrayList();
    listWithToken(
        continuationToken -> {
          ListTablesResponse response =
              tablesClient.listTables(
                  ListTablesRequest.builder()
                      .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
                      .namespace(namespace.level(0))
                      .maxTables(100)
                      .build());
          results.addAll(
              response.tables().stream()
                  .map(tableSummary -> TableIdentifier.of(namespace, tableSummary.name()))
                  .collect(Collectors.toList()));
          return response.continuationToken();
        });
    LOG.debug("Found {} tables", results.size());
    return results;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    LOG.debug("Trying to delete table: {}", identifier);
    if (!purge) {
      LOG.error("not allowing drop table with purge=false");
      throw new UnsupportedOperationException(
          "S3 Tables does not support the dropTable operation with purge=false. Some versions of Spark always set this flag to false even when running DROP TABLE PURGE commands."
              + " You can retry with DROP TABLE PURGE or use the S3 Tables DeleteTable API to delete a table.");
    }
    try {
      validateSingleLevelNamespace(identifier.namespace());

      tablesClient.deleteTable(
          DeleteTableRequest.builder()
              .name(identifier.name())
              .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
              .namespace(identifier.namespace().toString())
              .build());
      LOG.info("Successfully deleted {}", identifier);
      return true;
    } catch (NotFoundException ex) {
      LOG.debug("Received exception: ", ex);
      LOG.info("Table {} not found", identifier);
      return false;
    } catch (Exception e) {
      LOG.error("Failed to drop table {}", identifier, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    /*
      To comes in with the full namespace which is expected to be 2 levels.
      e.g. it comes in as: ice_catalog.namespace_name instead of just namespace_name.
      This throws off our normal validateSingleLevelNamespace which normally just accounts for namespace_name.
    */
    validateSingleLevelNamespace(to.namespace(), 2);
    validateSingleLevelNamespace(from.namespace());

    LOG.info("Renaming table from {} to {}", from, to);

    String sourceNamespaceName = from.namespace().toString();
    String targetNamespaceName = null;
    /* Since Iceberg supports multiple namespace levels (noticed that for target namespace it considered ice_catalog.namespace_name)
      instead of just namespace_name. For now deriving the namespace level from the source TableIdentifier,
      and choosing the target namespace at the same level.
    */
    int sourceNamespaceLevel = from.namespace().levels().length;
    int targetNamespaceLevel = to.namespace().levels().length;

    if (targetNamespaceLevel > sourceNamespaceLevel) {
      targetNamespaceName = to.namespace().level(sourceNamespaceLevel);
    } else {
      targetNamespaceName = to.namespace().toString();
    }
    try {
      tablesClient.renameTable(
          RenameTableRequest.builder()
              .name(from.name())
              .newName(to.name())
              .namespace(sourceNamespaceName)
              .newNamespaceName(targetNamespaceName)
              .tableBucketARN(catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION))
              .build());
      LOG.info("Successfully renamed table from {} to {}", from.name(), to.name());
    } catch (AwsServiceException | SdkClientException e) {
      LOG.error("Failed to rename table {}", from, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }

  @Override
  public void setConf(Object configuration) {
    hadoopConf = configuration;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @VisibleForTesting
  static void validateSingleLevelNamespace(Namespace namespace, int maxLength) {
    if (namespace != null && namespace.levels().length > maxLength) {
      LOG.error(
          "Namespace {} has {} levels and S3 Tables only supports one",
          namespace,
          namespace.levels());
      throw new ValidationException(
          "S3TablesCatalog does not support more than 1 level of namespace");
    }
  }

  @VisibleForTesting
  static void validateSingleLevelNamespace(Namespace namespace) {
    validateSingleLevelNamespace(namespace, 1);
  }

  private static void listWithToken(Function<String, String> continuationTokenGenerator) {
    String continuationToken = null;
    do {
      continuationToken = continuationTokenGenerator.apply(continuationToken);
    } while (continuationToken != null);
  }

  @VisibleForTesting
  S3TablesClient getS3TablesClient() {
    return this.tablesClient;
  }
}
