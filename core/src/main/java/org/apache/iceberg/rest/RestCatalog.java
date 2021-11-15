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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.http.ErrorHandlers;
import org.apache.iceberg.rest.http.HttpClient;
import org.apache.iceberg.rest.http.RequestResponseSerializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RestCatalog extends BaseMetastoreCatalog implements Closeable, SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(RestCatalog.class);

  private String catalogName;
  private Map<String, String> properties;
  private ObjectMapper mapper;
  private String baseUrl;
  private Configuration hadoopConf;
  private FileIO fileIO;

  // TODO - Refactor to use interface, which doesn't currently have POST etc embedded into it.
  private HttpClient restClient;

  @Override
  public void initialize(String name, Map<String, String> props) {
    super.initialize(name, props);
    this.catalogName = name;
    this.properties = props;

    // TODO - Possibly authenticate with the server initially and then have the server return some of this information
    Preconditions.checkNotNull(
        properties.getOrDefault("baseUrl", null),
        "Cannot initialize the RestCatalog as the baseUrl is a required parameter.");

    this.baseUrl = properties.get("baseUrl");
    this.fileIO = initializeFileIO(properties);

    this.mapper = new ObjectMapper();
    RequestResponseSerializers.registerAll(mapper);

    // TODO - We can possibly handle multiple warehouses via one RestCatalog to reuse the connection pool
    //        and for cross database calls if users need to authenticate with each.
    restClient = HttpClient.builder()
        .baseUrl(String.format("%s/warehouse/%s", baseUrl, catalogName))
        .mapper(mapper)
        .defaultErrorHandler(ErrorHandlers.tableErrorHandler())
        .build();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, String location,
      Map<String, String> props) {
    throw new UnsupportedOperationException("Not implemented: createTable");
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    // Users might have not authenticated, possibly need to reauthenticate, or get authentication info per request.
    //   Though usually, auth tokens are persistent for at least some period of time.
    return newTableOps(tableIdentifier, null);
  }

  protected RestTableOperations newTableOps(TableIdentifier tableIdentifier, String authToken) {
    throw new UnsupportedOperationException("Not implemented: newTableOps");
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Not implemented: defaultWarehouseLocation");
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    throw new UnsupportedOperationException("Not implemented: listTables");
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    throw new UnsupportedOperationException("Not implemented: tableExists");
  }

  @Override
  public boolean dropTable(TableIdentifier identifier) {
    return dropTable(identifier, false);
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
  public void close() throws IOException {
    restClient.close();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest req = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .withProperties(metadata)
        .build();

    // TODO - This should come from the server side.
    String path = properties.getOrDefault("create-namespace-path", "databases");
    restClient.post(path, req, CreateNamespaceResponse.class, ErrorHandlers.databaseErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: listNamespaces");
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: loadNamespaceMetadata");
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    throw new UnsupportedOperationException("Not implemented: dropNamespace");
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> props) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: setProperties");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> props) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException("Not implemented: removeProperties");
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new RestTableBuilder(identifier, schema);
  }

  @Override
  public Configuration getConf() {
    return hadoopConf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.hadoopConf = conf;
  }

  private FileIO initializeFileIO(Map<String, String> props) {
    String fileIOImpl = props.get(CatalogProperties.FILE_IO_IMPL);
    return CatalogUtil.loadFileIO(fileIOImpl, props, hadoopConf);
  }

  private static class RestTableBuilder implements Catalog.TableBuilder {
    private final TableIdentifier identifier;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private String location;
    private PartitionSpec spec = PartitionSpec.unpartitioned();
    private SortOrder sortOrder = SortOrder.unsorted();

    protected RestTableBuilder(TableIdentifier identifier, Schema schema) {
      this.identifier = identifier;
      this.schema = schema;
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

    // TODO - Figure out why this is giving me HiddenField and see about turning that off.
    @Override
    public TableBuilder withLocation(String tableLocation) {
      this.location = tableLocation;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> props) {
      if (props != null) {
        propertiesBuilder.putAll(props);
      }
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      throw new UnsupportedOperationException("Not implemented: create");
    }

    @Override
    public Transaction createTransaction() {
      throw new UnsupportedOperationException("Not implemented: createTransaction");
    }

    @Override
    public Transaction replaceTransaction() {
      throw new UnsupportedOperationException("Replace currently not supported");
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      throw new UnsupportedOperationException("Replace currently not supported");
    }
  }

  // TODO - Modified from Dynamo catalog. Should probably share it.
  private void validateNamespace(Namespace namespace) {
    // We might sometimes allow Namespace.of()?
    ValidationException.check(!namespace.isEmpty(),
        "A namespace object with no levels is not a valid namespace for a REST request");
    for (String level : namespace.levels()) {
      ValidationException.check(level != null && !level.isEmpty(),
          "Namespace level must not be empty: %s", namespace);
      ValidationException.check(!level.contains("."),
          "Namespace level must not contain dot, but found %s in %s", level, namespace);
    }
  }
}
