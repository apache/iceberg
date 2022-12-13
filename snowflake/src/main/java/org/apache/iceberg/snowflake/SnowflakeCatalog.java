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
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeCatalog extends BaseMetastoreCatalog
    implements Closeable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(SnowflakeCatalog.class);

  private Object conf;
  private String catalogName = SnowflakeResources.DEFAULT_CATALOG_NAME;
  private Map<String, String> catalogProperties = null;
  private FileIO fileIO;
  private SnowflakeClient snowflakeClient;

  public SnowflakeCatalog() {}

  @VisibleForTesting
  void setSnowflakeClient(SnowflakeClient snowflakeClient) {
    this.snowflakeClient = snowflakeClient;
  }

  @VisibleForTesting
  void setFileIO(FileIO fileIO) {
    this.fileIO = fileIO;
  }

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
    catalogProperties = properties;

    if (name != null) {
      this.catalogName = name;
    }

    if (snowflakeClient == null) {
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
      snowflakeClient = new JdbcSnowflakeClient(connectionPool);
    }

    if (fileIO == null) {
      String fileIOImpl = SnowflakeResources.DEFAULT_FILE_IO_IMPL;

      if (catalogProperties.containsKey(CatalogProperties.FILE_IO_IMPL)) {
        fileIOImpl = catalogProperties.get(CatalogProperties.FILE_IO_IMPL);
      }

      fileIO = CatalogUtil.loadFileIO(fileIOImpl, catalogProperties, conf);
    }
  }

  @Override
  public void close() {
    snowflakeClient.close();
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
    Map<String, String> nameSpaceMetadata = Maps.newHashMap();
    nameSpaceMetadata.put("name", namespace.toString());
    return nameSpaceMetadata;
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
    Preconditions.checkArgument(
        tableIdentifier.namespace().length() <= SnowflakeResources.MAX_NAMESPACE_DEPTH,
        "Snowflake doesn't support more than %s levels of namespace, got %s",
        SnowflakeResources.MAX_NAMESPACE_DEPTH,
        tableIdentifier);

    return new SnowflakeTableOperations(
        snowflakeClient, fileIO, catalogProperties, catalogName, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public void setConf(Object conf) {
    this.conf = conf;
  }

  public Object getConf() {
    return conf;
  }
}
