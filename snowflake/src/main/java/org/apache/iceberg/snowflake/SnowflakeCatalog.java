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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);

  private CloseableGroup closeableGroup;
  private Object conf;
  private String catalogName;
  private Map<String, String> catalogProperties;
  private FileIO fileIO;
  private SnowflakeClient snowflakeClient;

  public SnowflakeCatalog() {}

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    LOG.debug("listTables with namespace: {}", namespace);
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't support more than %s levels of namespace, got %s",
        SnowflakeResources.MAX_NAMESPACE_DEPTH,
        namespace);

    List<SnowflakeTable> sfTables = snowflakeClient.listIcebergTables(namespace);

    return sfTables.stream()
        .map(
            table ->
                TableIdentifier.of(table.getDatabase(), table.getSchemaName(), table.getName()))
        .collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException(
        String.format("dropTable not supported; attempted for table '%s'", identifier));
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException(
        String.format("renameTable not supported; attempted from '%s' to '%s'", from, to));
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkNotNull(uri, "JDBC connection URI is required");
    try {
      // We'll ensure the expected JDBC driver implementation class is initialized through
      // reflection
      // regardless of which classloader ends up using this JdbcSnowflakeClient, but we'll only
      // warn if the expected driver fails to load, since users may use repackaged or custom
      // JDBC drivers for Snowflake communcation.
      Class.forName(JdbcSnowflakeClient.EXPECTED_JDBC_IMPL);
    } catch (ClassNotFoundException cnfe) {
      LOG.warn(
          "Failed to load expected JDBC SnowflakeDriver - if queries fail by failing"
              + " to find a suitable driver for jdbc:snowflake:// URIs, you must add the Snowflake "
              + " JDBC driver to your jars/packages",
          cnfe);
    }
    JdbcClientPool connectionPool = new JdbcClientPool(uri, properties);

    String fileIOImpl = SnowflakeResources.DEFAULT_FILE_IO_IMPL;
    if (properties.containsKey(CatalogProperties.FILE_IO_IMPL)) {
      fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    }

    initialize(
        name,
        new JdbcSnowflakeClient(connectionPool),
        CatalogUtil.loadFileIO(fileIOImpl, properties, conf),
        properties);
  }

  /**
   * Initialize using caller-supplied SnowflakeClient and FileIO.
   *
   * @param name The name of the catalog, defaults to "snowflake_catalog"
   * @param snowflakeClient The client encapsulating network communication with Snowflake
   * @param fileIO The {@link FileIO} to use for table operations
   * @param properties The catalog options to use and propagate to dependencies
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public void initialize(
      String name, SnowflakeClient snowflakeClient, FileIO fileIO, Map<String, String> properties) {
    Preconditions.checkArgument(null != snowflakeClient, "snowflakeClient must be non-null");
    Preconditions.checkArgument(null != fileIO, "fileIO must be non-null");
    this.catalogName = name == null ? SnowflakeResources.DEFAULT_CATALOG_NAME : name;
    this.snowflakeClient = snowflakeClient;
    this.fileIO = fileIO;
    this.catalogProperties = properties;
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(snowflakeClient);
    closeableGroup.addCloseable(fileIO);
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
        String.format("createNamespace not supported; attempted for namespace '%s'", namespace));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) {
    LOG.debug("listNamespaces with namespace: {}", namespace);
    Preconditions.checkArgument(
        namespace.length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH - 1,
        "Snowflake doesn't support more than %s levels of namespace, tried to list under %s",
        SnowflakeResources.MAX_NAMESPACE_DEPTH,
        namespace);
    List<SnowflakeSchema> sfSchemas = snowflakeClient.listSchemas(namespace);

    List<Namespace> namespaceList =
        sfSchemas.stream()
            .map(schema -> Namespace.of(schema.getDatabase(), schema.getName()))
            .collect(Collectors.toList());
    return namespaceList;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    LOG.debug("loadNamespaceMetadata with namespace: {}", namespace);
    return ImmutableMap.of();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) {
    throw new UnsupportedOperationException(
        String.format("dropNamespace not supported; attempted for namespace '%s'", namespace));
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        String.format("setProperties not supported; attempted for namespace '%s'", namespace));
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    throw new UnsupportedOperationException(
        String.format("removeProperties not supported; attempted for namespace '%s'", namespace));
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new SnowflakeTableOperations(
        snowflakeClient, fileIO, catalogProperties, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException(
        String.format(
            "defaultWarehouseLocation not supported; attempted for tableIdentifier '%s'",
            tableIdentifier));
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  public Object getConf() {
    return conf;
  }
}
