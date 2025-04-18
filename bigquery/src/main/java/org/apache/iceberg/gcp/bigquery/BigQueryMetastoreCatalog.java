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
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ValidationException;
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
public final class BigQueryMetastoreCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable {

  // User provided properties.
  public static final String PROPERTIES_KEY_GCP_PROJECT = "gcp-project";
  public static final String PROPERTIES_KEY_GCP_LOCATION = "gcp-location";
  public static final String PROPERTIES_KEY_FILTER_UNSUPPORTED_TABLES = "filter-unsupported-tables";
  public static final String PROPERTIES_KEY_TESTING_ENABLED = "testing-enabled";

  public static final String HIVE_METASTORE_WAREHOUSE_DIR = "hive.metastore.warehouse.dir";

  private static final Logger LOG = LoggerFactory.getLogger(BigQueryMetastoreCatalog.class);

  private static final String DEFAULT_GCP_LOCATION = "us";

  private String catalogPluginName;
  private Map<String, String> catalogProperties;
  private FileIO fileIO;
  private Configuration conf;
  private String projectId;
  private String location;
  private BigQueryMetastoreClient client;
  private boolean filterUnsupportedTables;

  // Must have a no-arg constructor to be dynamically loaded
  // initialize(String name, Map<String, String> properties) will be called to complete
  // initialization
  public BigQueryMetastoreCatalog() {}

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    if (!properties.containsKey(PROPERTIES_KEY_GCP_PROJECT)) {
      throw new ValidationException("GCP project must be specified");
    }
    projectId = properties.get(PROPERTIES_KEY_GCP_PROJECT);
    location = properties.getOrDefault(PROPERTIES_KEY_GCP_LOCATION, DEFAULT_GCP_LOCATION);
    boolean testingEnabled =
        Boolean.parseBoolean(properties.getOrDefault(PROPERTIES_KEY_TESTING_ENABLED, "false"));

    BigQueryOptions options =
        BigQueryOptions.newBuilder()
            .setProjectId(projectId)
            .setLocation(location)
            .setRetrySettings(ServiceOptions.getDefaultRetrySettings())
            .build();

    try {
      if (testingEnabled) {
        client = new FakeBigQueryMetastoreClient(options);

      } else {
        client = new BigQueryMetastoreClientImpl(options);
      }
    } catch (IOException e) {
      throw new ServiceFailureException(e, "Creating BigQuery client failed");
    } catch (GeneralSecurityException e) {
      throw new ValidationException(e, "Creating BigQuery client failed due to a security issue");
    }
    initialize(inputName, properties, projectId, location, client);
  }

  @VisibleForTesting
  void initialize(
      String inputName,
      Map<String, String> properties,
      String initialProjectId,
      String initialLocation,
      BigQueryMetastoreClient bigQueryMetaStoreClient) {
    this.catalogPluginName = inputName;
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.projectId = initialProjectId;
    this.location = initialLocation;
    this.client =
        Preconditions.checkNotNull(bigQueryMetaStoreClient, "BigQuery client can not be null");

    if (this.conf == null) {
      LOG.warn("No configuration was set, using the default environment Configuration");
      this.conf = new Configuration();
    }

    LOG.info("Using BigQuery Metastore Iceberg Catalog: {}", inputName);

    if (properties.containsKey(CatalogProperties.WAREHOUSE_LOCATION)) {
      // Iceberg always removes trailing slash to avoid paths like "<folder>//data" in file systems
      // like s3.
      this.conf.set(
          HIVE_METASTORE_WAREHOUSE_DIR,
          LocationUtil.stripTrailingSlash(properties.get(CatalogProperties.WAREHOUSE_LOCATION)));
    }

    String fileIoImpl =
        properties.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    this.fileIO = CatalogUtil.loadFileIO(fileIoImpl, properties, conf);

    this.filterUnsupportedTables =
        Boolean.parseBoolean(
            properties.getOrDefault(PROPERTIES_KEY_FILTER_UNSUPPORTED_TABLES, "false"));
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new BigQueryTableOperations(
        client,
        fileIO,
        projectId,
        // Sometimes extensions have the namespace contain the table name too, so we are forced to
        // allow invalid namespace and just take the first part here like other catalog
        // implementations do.
        identifier.namespace().level(0),
        identifier.name(),
        conf);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    String locationUri = null;
    DatasetReference datasetReference = toDatasetReference(identifier.namespace());
    Dataset dataset = client.getDataset(datasetReference);
    if (dataset != null && dataset.getExternalCatalogDatasetOptions() != null) {
      locationUri = dataset.getExternalCatalogDatasetOptions().getDefaultStorageLocationUri();
    }
    return String.format(
        "%s/%s",
        Strings.isNullOrEmpty(locationUri)
            ? datasetReference.getDatasetId()
            : LocationUtil.stripTrailingSlash(locationUri),
        identifier.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    validateNamespace(namespace);

    return client.listTables(toDatasetReference(namespace), filterUnsupportedTables).stream()
        .map(
            table -> TableIdentifier.of(namespace.level(0), table.getTableReference().getTableId()))
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    try {
      TableOperations ops = newTableOps(identifier);
      TableMetadata lastMetadata = ops.current();

      client.deleteTable(toTableReference(identifier));

      if (purge && lastMetadata != null) {
        CatalogUtil.dropTableData(ops.io(), lastMetadata);
      }
    } catch (NoSuchTableException e) { // Not catching a NoSuchIcebergTableException on purpose
      return false; // The documentation says just return false in this case
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    validateNamespace(from.namespace());
    validateNamespace(to.namespace());

    try {
      client.getDataset(toDatasetReference(to.namespace()));
      client.getDataset(toDatasetReference(from.namespace()));
    } catch (NoSuchNamespaceException e) {
      throw e;
    }

    TableReference fromTableRef = toTableReference(from);
    TableReference toTableRef = toTableReference(to);

    // Check if the source table exists
    try {
      client.getTable(fromTableRef);
    } catch (NoSuchTableException e) {
      throw new NoSuchTableException("Table does not exist: %s", from.name());
    }

    // Check if the destination table already exists
    try {
      client.getTable(toTableRef);
      // If getTable succeeds, the destination table exists
      throw new AlreadyExistsException("Table already exists: %s", to.name());
    } catch (NoSuchTableException e) {
      // Destination table does not exist, proceed with throwing exception
    }

    if (!from.namespace().equals(to.namespace())) {
      throw new ValidationException("New table name must be in the same namespace");
    }

    // TODO(b/354981675): Enable once supported by the API.
    throw new UnsupportedOperationException("Table rename operation is unsupported.");
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Dataset builder = new Dataset();
    DatasetReference datasetReference = toDatasetReference(namespace);
    builder.setLocation(this.location);
    builder.setDatasetReference(datasetReference);
    builder.setExternalCatalogDatasetOptions(
        BigQueryMetastoreUtils.createExternalCatalogDatasetOptions(
            datasetReference.getDatasetId(), metadata));

    client.createDataset(builder);
  }

  /**
   * Since this catalog only supports one-level namespaces, it always returns an empty list unless
   * passed an empty namespace to list all namespaces within the catalog.
   */
  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    if (namespace.levels().length != 0) {
      // BQMS does not support namespaces under database or tables, returns empty.
      // It is called when dropping a namespace to make sure it's empty (listTables is called as
      // well), returns empty to unblock deletion.
      return ImmutableList.of();
    }
    return client.listDatasets(projectId).stream()
        .map(BigQueryMetastoreCatalog::getNamespace)
        .collect(ImmutableList.toImmutableList());
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    try {
      client.deleteDataset(toDatasetReference(namespace));
      /* We don't delete the data folder for safety, which aligns with Hive Metastore's default
       * behavior.
       * We can support database or catalog level config controlling file deletion in the future.
       */
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    Dataset dataset = client.getDataset(toDatasetReference(namespace));

    ExternalCatalogDatasetOptions existingOptions = dataset.getExternalCatalogDatasetOptions();
    Map<String, String> existingParameters =
        existingOptions != null ? existingOptions.getParameters() : null;

    Map<String, String> newParameters = Maps.newHashMap();
    if (existingParameters != null) {
      newParameters.putAll(existingParameters);
    }
    newParameters.putAll(properties);

    if (Objects.equals(existingParameters, newParameters)) {
      // No change in parameters detected
      return false;
    }

    client.setDatasetParameters(toDatasetReference(namespace), properties);
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    client.removeDatasetParameters(toDatasetReference(namespace), properties);
    return true;
  }

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
    try {
      return getMetadata(client.getDataset(toDatasetReference(namespace)));
    } catch (IllegalArgumentException e) {
      throw new NoSuchNamespaceException(e.getMessage());
    }
  }

  @Override
  public String name() {
    return catalogPluginName;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = new Configuration(conf);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  private static Namespace getNamespace(Datasets dataset) {
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

  private static Map<String, String> getMetadata(Dataset dataset) {
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

  private static void validateNamespace(Namespace namespace) {
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
