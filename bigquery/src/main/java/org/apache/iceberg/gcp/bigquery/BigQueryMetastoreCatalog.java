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
package org.apache.iceberg.gcp.bigquery;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.ServiceOptions;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Locale;
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
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Iceberg Bigquery Metastore Catalog implementation. */
public class BigQueryMetastoreCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Object> {

  // User provided properties.
  public static final String PROJECT_ID = "gcp.bigquery.project-id";
  public static final String GCP_LOCATION = "gcp.bigquery.location";
  public static final String LIST_ALL_TABLES = "gcp.bigquery.list-all-tables";

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetastoreCatalog.class);

  private static final String DEFAULT_GCP_LOCATION = "us";

  private String catalogName;
  private Map<String, String> catalogProperties;
  private FileIO fileIO;
  private Object conf;
  private String projectId;
  private String projectLocation;
  private BigQueryMetastoreClient client;
  private boolean listAllTables;
  private String warehouseLocation;

  public BigQueryMetastoreCatalog() {}

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(PROJECT_ID),
        "Invalid GCP project: %s must be specified",
        PROJECT_ID);

    this.projectId = properties.get(PROJECT_ID);
    this.projectLocation = properties.getOrDefault(GCP_LOCATION, DEFAULT_GCP_LOCATION);

    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setLocation(projectLocation)
            .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
            .build();

    try {
      client = new BigQueryMetastoreClientImpl(options);
    } catch (IOException e) {
      throw new UncheckedIOException("Creating BigQuery client failed", e);
    } catch (GeneralSecurityException e) {
      throw new RuntimeException("Creating BigQuery client failed due to a security issue", e);
    }

    initialize(name, properties, projectId, projectLocation, client);
  }

  @VisibleForTesting
  void initialize(
      String name,
      Map<String, String> properties,
      String initialProjectId,
      String initialLocation,
      BigQueryMetastoreClient bigQueryMetaStoreClient) {
    Preconditions.checkArgument(bigQueryMetaStoreClient != null, "Invalid BigQuery client: null");
    this.catalogName = name;
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.projectId = initialProjectId;
    this.projectLocation = initialLocation;
    this.client = bigQueryMetaStoreClient;

    LOG.info("Using BigQuery Metastore Iceberg Catalog: {}", name);

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      this.warehouseLocation =
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION));
    }

    this.fileIO =
        CatalogUtil.loadFileIO(
            properties.getOrDefault(
                CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.io.ResolvingFileIO"),
            properties,
            conf);

    this.listAllTables = Boolean.parseBoolean(properties.getOrDefault(LIST_ALL_TABLES, "true"));
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new BigQueryTableOperations(client, fileIO, toTableReference(identifier));
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    String locationUri = null;
    DatasetReference datasetReference = toDatasetReference(identifier.namespace());
    Dataset dataset = client.load(datasetReference);
    if (dataset != null && dataset.getExternalCatalogDatasetOptions() != null) {
      locationUri = dataset.getExternalCatalogDatasetOptions().getDefaultStorageLocationUri();
    }

    return String.format(
        "%s/%s",
        Strings.isNullOrEmpty(locationUri)
            ? createDefaultStorageLocationUri(datasetReference.getDatasetId())
            : LocationUtil.stripTrailingSlash(locationUri),
        identifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    validateNamespace(namespace);

    return client.list(toDatasetReference(namespace), listAllTables).stream()
        .map(
            table -> TableIdentifier.of(namespace.level(0), table.getTableReference().getTableId()))
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = ops.current();

      client.delete(toTableReference(identifier));

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }
    } catch (NoSuchTableException e) {
      return false;
    }

    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    // TODO: Enable once supported by BigQuery API.
    throw new UnsupportedOperationException("Table rename operation is unsupported.");
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Dataset builder = new Dataset();
    DatasetReference datasetReference = toDatasetReference(namespace);
    builder.setLocation(this.projectLocation);
    builder.setDatasetReference(datasetReference);
    builder.setExternalCatalogDatasetOptions(
        BigQueryMetastoreUtils.createExternalCatalogDatasetOptions(
            createDefaultStorageLocationUri(datasetReference.getDatasetId()), metadata));

    client.create(builder);
  }

  @Override
  public List<Namespace> listNamespaces() {
    try {
      return listNamespaces(Namespace.empty());
    } catch (NoSuchNamespaceException e) {
      return ImmutableList.of();
    }
  }

  /**
   * Since this catalog only supports one-level namespaces, it always returns an empty list unless
   * passed an empty namespace to list all namespaces within the catalog.
   */
  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (!namespace.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<Datasets> allDatasets = client.list(projectId);

    ImmutableList<Namespace> namespaces =
        allDatasets.stream().map(this::toNamespace).collect(ImmutableList.toImmutableList());

    if (namespaces.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return namespaces;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    try {
      client.delete(toDatasetReference(namespace));
      // We don't delete the data folder for safety, which aligns with Hive Metastore's default
      // behavior.
      // We can support database or catalog level config controlling file deletion in the future.
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    return client.setParameters(toDatasetReference(namespace), properties);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    Preconditions.checkNotNull(properties, "Invalid properties to remove: null");

    if (properties.isEmpty()) {
      return false;
    }

    return client.removeParameters(toDatasetReference(namespace), properties);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    try {
      return toMetadata(client.load(toDatasetReference(namespace)));
    } catch (IllegalArgumentException e) {
      throw new NoSuchNamespaceException("%s", e.getMessage());
    }
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  private String createDefaultStorageLocationUri(String dbId) {
    Preconditions.checkArgument(
        warehouseLocation != null,
        String.format(
            "Invalid data warehouse location: %s not set", CatalogProperties.WAREHOUSE_LOCATION));
    return String.format("%s/%s.db", LocationUtil.stripTrailingSlash(warehouseLocation), dbId);
  }

  private Namespace toNamespace(Datasets dataset) {
    return Namespace.of(dataset.getDatasetReference().getDatasetId());
  }

  private DatasetReference toDatasetReference(Namespace namespace) {
    validateNamespace(namespace);
    return new DatasetReference().setProjectId(projectId).setDatasetId(namespace.level(0));
  }

  private TableReference toTableReference(TableIdentifier tableIdentifier) {
    DatasetReference datasetReference = toDatasetReference(tableIdentifier.namespace());
    return new TableReference()
        .setProjectId(datasetReference.getProjectId())
        .setDatasetId(datasetReference.getDatasetId())
        .setTableId(tableIdentifier.name());
  }

  private Map<String, String> toMetadata(Dataset dataset) {
    ExternalCatalogDatasetOptions options = dataset.getExternalCatalogDatasetOptions();
    Map<String, String> metadata = Maps.newHashMap();
    if (options != null) {
      if (options.getParameters() != null) {
        metadata.putAll(options.getParameters());
      }

      if (!Strings.isNullOrEmpty(options.getDefaultStorageLocationUri())) {
        metadata.put("location", options.getDefaultStorageLocationUri());
      }
    }

    return metadata;
  }

  private void validateNamespace(Namespace namespace) {
    Preconditions.checkArgument(
        namespace.levels().length == 1,
        String.format(
            Locale.ROOT,
            "BigQuery Metastore only supports single level namespaces. Invalid namespace: \"%s\" has %s"
                + " levels",
            namespace,
            namespace.levels().length));
  }
}
