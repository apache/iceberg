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

package org.apache.iceberg.rest;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.DropNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.RESTCatalogConfigResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RESTCatalog
    implements Catalog, SupportsNamespaces, Closeable, Configurable<Configuration> {

  private static final Logger LOG = LoggerFactory.getLogger(RESTCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final ObjectMapper MAPPER = initializeObjectMapper();

  private String catalogName;
  private RESTClient client;
  private CloseableGroup closeableGroup;
  private FileIO fileIO;
  private Object hadoopConf;

  public RESTCatalog() {
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    Preconditions.checkNotNull(properties, "Invalid initial configuration: null");
    initialize(
        name,
        properties.get(CatalogProperties.URI),
        properties);
  }

  @VisibleForTesting
  void initialize(String name, String uri, Map<String, String> initialProperties) {
    this.catalogName = name;

    // Fetch one time config that we will use as our final config.
    Map<String, String> properties = fetchServerConfiguration(initialProperties).merge(initialProperties);

    // For now, assume all paths are V1.
    // TODO - Add a function V1Route(Route, prefix) that will insert the correct V1 prefix.
    String prefix = properties.get(RESTCatalogProperties.PREFIX);
    String v1BaseUri = SLASH.skipNulls().join(uri, "v1", prefix);
    LOG.info("Connecting to REST catalog at URI: {}", v1BaseUri);

    this.client = HttpRESTClient.builder()
        .uri(v1BaseUri)
        .withBearerAuth(properties.get(RESTCatalogProperties.AUTH_TOKEN))
        .mapper(MAPPER)
        .defaultErrorHandler(ErrorHandlers.defaultErrorHandler())
        .build();
    this.fileIO = initializeFileIO(properties);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(client);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.setSuppressCloseFailure(true);
  }

  private static ObjectMapper initializeObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    RESTSerializers.registerAll(mapper);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
    return mapper;
  }

  private RESTCatalogConfigResponse fetchServerConfiguration(Map<String, String> properties) {
    // Get prefix and ensure it's null if empty so the base uri doesn't have an additional trailing slash
    String prefix = properties.get(RESTCatalogProperties.PREFIX);
    prefix = prefix != null && !prefix.isEmpty() ? prefix : null;
    String baseUri = SLASH.skipNulls()
        .join(properties.get(CatalogProperties.URI), "v1", prefix);

    String route = "config";
    RESTClient singleUseClient = HttpRESTClient.builder()
        .withBearerAuth(properties.get(RESTCatalogProperties.AUTH_TOKEN))
        .uri(baseUri)
        .mapper(MAPPER)
        .build();

    RESTCatalogConfigResponse configResponse = singleUseClient
        .get(route, RESTCatalogConfigResponse.class, ErrorHandlers.defaultErrorHandler());

    try {
      singleUseClient.close();
    } catch (IOException e) {
      LOG.warn("Failed to close the single use http client used for fetching server configuration", e);
    }

    return configResponse;
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    String joined = RESTUtil.asURLVariable(namespace);
    ListTablesResponse response = client
        .get("namespaces/" + joined + "/tables", ListTablesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException("Not implemented: dropTable");
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Not implemented: renameTable");
  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    throw new UnsupportedOperationException("Not implemented: loadTable");
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .setProperties(metadata)
        .build();

    // for now, ignore the response because there is no way to return it
    client.post("namespaces", request, CreateNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    String path = "namespaces";
    if (!namespace.isEmpty()) {
      path = String.format("%s?parent=%s", path, RESTUtil.asURLVariable(namespace));
    }

    ListNamespacesResponse response = client
        .get(path, ListNamespacesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String joined = RESTUtil.asURLVariable(namespace);
    GetNamespaceResponse response = client
        .get("namespaces/" + joined, GetNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    String joined = RESTUtil.asURLVariable(namespace);
    DropNamespaceResponse response = client
        .delete("namespaces/" + joined, DropNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.isDropped();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    String joined = RESTUtil.asURLVariable(namespace);
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .updateAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "namespaces/" + joined + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    String joined = RESTUtil.asURLVariable(namespace);
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .removeAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "namespaces/" + joined + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.removed().isEmpty();
  }

  @Override
  public void close() throws IOException {
    closeableGroup.close();
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  private FileIO initializeFileIO(Map<String, String> props) {
    String fileIOImpl = props.get(CatalogProperties.FILE_IO_IMPL);
    return CatalogUtil.loadFileIO(fileIOImpl, props, hadoopConf);
  }
}
