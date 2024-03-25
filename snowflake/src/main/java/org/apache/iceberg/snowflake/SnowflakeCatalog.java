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
package org.apache.iceberg.snowflake;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.IcebergBuild;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.CatalogProperties.METADATA_REFRESH_MAX_RETRIES;
import static org.apache.iceberg.CatalogProperties.METADATA_REFRESH_MAX_RETRIES_DEFAULT;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements SupportsNamespaces, Configurable<Object> {
  private static final String DEFAULT_CATALOG_NAME = "snowflake_catalog";
  private static final String DEFAULT_FILE_IO_IMPL = "org.apache.iceberg.io.ResolvingFileIO";
  // Specifies the name of a Snowflake's partner application to connect through JDBC.
  // https://docs.snowflake.com/en/user-guide/jdbc-parameters.html#application
  private static final String JDBC_APPLICATION_PROPERTY = "application";
  // Add a suffix to user agent header for the web requests made by the jdbc driver.
  private static final String JDBC_USER_AGENT_SUFFIX_PROPERTY = "user_agent_suffix";
  private static final String APP_IDENTIFIER = "iceberg-snowflake-catalog";
  // Specifies the max length of unique id for each catalog initialized session.
  private static final int UNIQUE_ID_LENGTH = 20;
  // Injectable factory for testing purposes.
  static class FileIOFactory {
    public FileIO newFileIO(String impl, Map<String, String> properties, Object hadoopConf) {
      return CatalogUtil.loadFileIO(impl, properties, hadoopConf);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);

  private CloseableGroup closeableGroup;
  private Object conf;
  private String catalogName;
  private Map<String, String> catalogProperties;
  private FileIOFactory fileIOFactory;
  private SnowflakeClient snowflakeClient;

  public SnowflakeCatalog() {}

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    SnowflakeIdentifier scope = NamespaceHelpers.toSnowflakeIdentifier(namespace);
    Preconditions.checkArgument(
        scope.type() == SnowflakeIdentifier.Type.SCHEMA,
        "listTables must be at SCHEMA level; got %s from namespace %s",
        scope,
        namespace);

    List<SnowflakeIdentifier> sfTables = snowflakeClient.listIcebergTables(scope);

    return sfTables.stream()
        .map(NamespaceHelpers::toIcebergTableIdentifier)
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support dropTable");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support renameTable");
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkArgument(null != uri, "JDBC connection URI is required");
    try {
      // We'll ensure the expected JDBC driver implementation class is initialized through
      // reflection regardless of which classloader ends up using this JdbcSnowflakeClient, but
      // we'll only warn if the expected driver fails to load, since users may use repackaged or
      // custom JDBC drivers for Snowflake communication.
      Class.forName(JdbcSnowflakeClient.EXPECTED_JDBC_IMPL);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn(
          "Failed to load expected JDBC SnowflakeDriver - if queries fail by failing"
              + " to find a suitable driver for jdbc:snowflake:// URIs, you must add the Snowflake "
              + " JDBC driver to your jars/packages",
          cnfe);
    }

    // The uniqueAppIdentifier should be less than 50 characters, so trimming the guid.
    String uniqueId = UUID.randomUUID().toString().replace("-", "").substring(0, UNIQUE_ID_LENGTH);
    String uniqueAppIdentifier = APP_IDENTIFIER + "_" + uniqueId;
    String userAgentSuffix = IcebergBuild.fullVersion() + " " + uniqueAppIdentifier;
    // Populate application identifier in jdbc client
    properties.put(JdbcCatalog.PROPERTY_PREFIX + JDBC_APPLICATION_PROPERTY, uniqueAppIdentifier);
    // Adds application identifier to the user agent header of the JDBC requests.
    properties.put(JdbcCatalog.PROPERTY_PREFIX + JDBC_USER_AGENT_SUFFIX_PROPERTY, userAgentSuffix);

    JdbcClientPool connectionPool = new JdbcClientPool(uri, properties);

    initialize(name, new JdbcSnowflakeClient(connectionPool), new FileIOFactory(), properties);
  }

  /**
   * Initialize using caller-supplied SnowflakeClient and FileIO.
   *
   * @param name The name of the catalog, defaults to "snowflake_catalog"
   * @param snowflakeClient The client encapsulating network communication with Snowflake
   * @param fileIOFactory The {@link FileIOFactory} to use to instantiate a new FileIO for each new
   *     table operation
   * @param properties The catalog options to use and propagate to dependencies
   */
  @SuppressWarnings("checkstyle:HiddenField")
  void initialize(
      String name,
      SnowflakeClient snowflakeClient,
      FileIOFactory fileIOFactory,
      Map<String, String> properties) {
    Preconditions.checkArgument(null != snowflakeClient, "snowflakeClient must be non-null");
    Preconditions.checkArgument(null != fileIOFactory, "fileIOFactory must be non-null");
    this.catalogName = name == null ? DEFAULT_CATALOG_NAME : name;
    this.snowflakeClient = snowflakeClient;
    this.fileIOFactory = fileIOFactory;
    this.catalogProperties = properties;
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(snowflakeClient);
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  public void close() throws IOException {
    if (null != closeableGroup) {
      closeableGroup.close();
    }
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support createNamespace");
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    SnowflakeIdentifier scope = NamespaceHelpers.toSnowflakeIdentifier(namespace);
    List<SnowflakeIdentifier> results = null;
    switch (scope.type()) {
      case ROOT:
        results = snowflakeClient.listDatabases();
        break;
      case DATABASE:
        results = snowflakeClient.listSchemas(scope);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "listNamespaces must be at either ROOT or DATABASE level; got %s from namespace %s",
                scope, namespace));
    }

    return results.stream().map(NamespaceHelpers::toIcebergNamespace).collect(Collectors.toList());
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    SnowflakeIdentifier id = NamespaceHelpers.toSnowflakeIdentifier(namespace);
    boolean namespaceExists;
    switch (id.type()) {
      case DATABASE:
        namespaceExists = snowflakeClient.databaseExists(id);
        break;
      case SCHEMA:
        namespaceExists = snowflakeClient.schemaExists(id);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "loadNamespaceMetadata must be at either DATABASE or SCHEMA level; got %s from namespace %s",
                id, namespace));
    }
    if (namespaceExists) {
      return ImmutableMap.of();
    } else {
      throw new NoSuchNamespaceException(
          "Namespace '%s' with snowflake identifier '%s' doesn't exist", namespace, id);
    }
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support dropNamespace");
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support setProperties");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support removeProperties");
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String fileIOImpl = DEFAULT_FILE_IO_IMPL;
    if (catalogProperties.containsKey(CatalogProperties.FILE_IO_IMPL)) {
      fileIOImpl = catalogProperties.get(CatalogProperties.FILE_IO_IMPL);
    }

    // Initialize a fresh FileIO for each TableOperations created, because some FileIO
    // implementations such as S3FileIO can become bound to a single S3 bucket. Additionally,
    // FileIO implementations often support only a finite set of one or more URI schemes (i.e.
    // S3FileIO only supports s3/s3a/s3n, and even ResolvingFileIO only supports the combination
    // of schemes registered for S3FileIO and HadoopFileIO). Individual catalogs may need to
    // support tables across different cloud/storage providers with disjoint FileIO implementations.
    FileIO fileIO = fileIOFactory.newFileIO(fileIOImpl, catalogProperties, conf);
    closeableGroup.addCloseable(fileIO);
    int metadataRefreshMaxRetries = PropertyUtil.propertyAsInt(
            catalogProperties, METADATA_REFRESH_MAX_RETRIES, METADATA_REFRESH_MAX_RETRIES_DEFAULT);

    return new SnowflakeTableOperations(
            snowflakeClient, fileIO, catalogName, tableIdentifier, metadataRefreshMaxRetries);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException(
        "SnowflakeCatalog does not currently support defaultWarehouseLocation");
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }
}
