/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.rest;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.DropNamespaceResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;

public class RESTCatalog implements Catalog, SupportsNamespaces {
  private static final Joiner NULL = Joiner.on('\0');
  private final RESTClient client;
  private String catalogName = null;

  RESTCatalog(RESTClient client) {
    this.client = client;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name;
  }

  @Override
  public String name() {
    return catalogName;
  }

  private String fullTableName(TableIdentifier ident) {
    return String.format("%s.%s", catalogName, ident);
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    String joined = NULL.join(namespace.levels());
    // TODO: URL-encode joined and add tests for / in the namespace name
    ListTablesResponse response = client
        .get("v1/namespaces/" + joined + "/tables", ListTablesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.identifiers();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {

  }

  @Override
  public Table loadTable(TableIdentifier identifier) {
    String joined = NULL.join(identifier.namespace().levels());
    LoadTableResponse response = client.get(
        "v1/namespaces/" + joined + "/tables/" + identifier.name(),
        LoadTableResponse.class, ErrorHandlers.tableErrorHandler());

    // TODO: implement initialize and fixup the name reported by the table
    return new BaseTable(new RESTTableOperations(response.tableMetadata()), fullTableName(identifier));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    CreateNamespaceRequest request = CreateNamespaceRequest.builder()
        .withNamespace(namespace)
        .setProperties(metadata)
        .build();

    // for now, ignore the response because there is no way to return it
    client.post("v1/namespaces", request, CreateNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Preconditions.checkArgument(namespace.isEmpty(), "Cannot list namespaces under parent: %s", namespace);
    // String joined = NULL.join(namespace.levels());
    ListNamespacesResponse response = client
        .get("v1/namespaces", ListNamespacesResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.namespaces();
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    String joined = NULL.join(namespace.levels());
    // TODO: rename to LoadNamespaceResponse?
    GetNamespaceResponse response = client
        .get("v1/namespaces/" + joined, GetNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.properties();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    String joined = NULL.join(namespace.levels());
    DropNamespaceResponse response = client
        .delete("v1/namespaces/" + joined, DropNamespaceResponse.class, ErrorHandlers.namespaceErrorHandler());
    return response.isDropped();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    String joined = NULL.join(namespace.levels());
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .updateAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "v1/namespaces/" + joined + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.updated().isEmpty();
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    String joined = NULL.join(namespace.levels());
    UpdateNamespacePropertiesRequest request = UpdateNamespacePropertiesRequest.builder()
        .removeAll(properties)
        .build();

    UpdateNamespacePropertiesResponse response = client.post(
        "v1/namespaces/" + joined + "/properties", request, UpdateNamespacePropertiesResponse.class,
        ErrorHandlers.namespaceErrorHandler());

    return !response.removed().isEmpty();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new Builder(identifier, schema);
  }

  private class Builder implements TableBuilder {
    private final TableIdentifier ident;
    private final Schema schema;
    private final ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.builder();
    private PartitionSpec spec = null;
    private SortOrder writeOrder = null;
    private String location = null;

    private Builder(TableIdentifier ident, Schema schema) {
      this.ident = ident;
      this.schema = schema;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec tableSpec) {
      this.spec = tableSpec;
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder tableWriteOrder) {
      this.writeOrder = tableWriteOrder;
      return this;
    }

    @Override
    public TableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      this.propertiesBuilder.putAll(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      this.propertiesBuilder.put(key, value);
      return this;
    }

    @Override
    public Table create() {
      String joined = NULL.join(ident.namespace().levels());
      CreateTableRequest request = CreateTableRequest.builder()
          .withName(ident.name())
          .withSchema(schema)
          .withPartitionSpec(spec)
          .withWriteOrder(writeOrder)
          .withLocation(location)
          .setProperties(propertiesBuilder.build())
          .build();

      LoadTableResponse response = client.post(
          "v1/namespaces/" + joined + "/tables", request, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());

      return new BaseTable(new RESTTableOperations(response.tableMetadata()), fullTableName(ident));
    }

    @Override
    public Transaction createTransaction() {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Transaction replaceTransaction() {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      throw new UnsupportedOperationException("Not implemented yet");
    }
  }

  private static class RESTTableOperations implements TableOperations {
    private final TableMetadata current;

    private RESTTableOperations(TableMetadata current) {
      this.current = current;
    }

    @Override
    public TableMetadata current() {
      return current;
    }

    @Override
    public TableMetadata refresh() {
      return current;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public FileIO io() {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public String metadataFileLocation(String fileName) {
      throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    public LocationProvider locationProvider() {
      throw new UnsupportedOperationException("Not implemented yet");
    }
  }
}
