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
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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

public class CatalogHandlers {
  private CatalogHandlers() {
  }

  static class UnprocessableEntityException extends RuntimeException {
    public UnprocessableEntityException(Set<String> commonKeys) {
      super(String.format("Invalid namespace update, cannot set and remove keys: %s", commonKeys));
    }
  }

  public static ListNamespacesResponse listNamespaces(SupportsNamespaces catalog, Namespace parent) {
    List<Namespace> results;
    if (parent.isEmpty()) {
      results = catalog.listNamespaces();
    } else {
      results = catalog.listNamespaces(parent);
    }

    return ListNamespacesResponse.builder().addAll(results).build();
  }

  public static CreateNamespaceResponse createNamespace(SupportsNamespaces catalog, CreateNamespaceRequest request) {
    Namespace namespace = request.namespace();
    catalog.createNamespace(namespace, request.properties());
    return CreateNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(catalog.loadNamespaceMetadata(namespace))
        .build();
  }

  public static GetNamespaceResponse loadNamespace(SupportsNamespaces catalog, Namespace namespace) {
    Map<String, String> properties = catalog.loadNamespaceMetadata(namespace);
    return GetNamespaceResponse.builder()
        .withNamespace(namespace)
        .setProperties(properties)
        .build();
  }

  public static DropNamespaceResponse dropNamespace(SupportsNamespaces catalog, Namespace namespace) {
    boolean dropped = catalog.dropNamespace(namespace);
    return DropNamespaceResponse.builder()
        .dropped(dropped)
        .build();
  }

  public static UpdateNamespacePropertiesResponse updateNamespaceProperties(
      SupportsNamespaces catalog, Namespace namespace, UpdateNamespacePropertiesRequest request) {
    Set<String> removals = Sets.newHashSet(request.removals());
    Map<String, String> updates = request.updates();

    Set<String> commonKeys = Sets.intersection(updates.keySet(), removals);
    if (!commonKeys.isEmpty()) {
      throw new UnprocessableEntityException(commonKeys);
    }

    Map<String, String> startProperties = catalog.loadNamespaceMetadata(namespace);
    Set<String> missing = Sets.difference(removals, startProperties.keySet());

    if (!updates.isEmpty()) {
      catalog.setProperties(namespace, updates);
    }

    if (!removals.isEmpty()) {
      // remove the original set just in case there was an update just after loading properties
      catalog.removeProperties(namespace, removals);
    }

    return UpdateNamespacePropertiesResponse.builder()
            .addMissing(missing)
            .addUpdated(updates.keySet())
            .addRemoved(Sets.difference(removals, missing))
            .build();
  }

  public static ListTablesResponse listTables(Catalog catalog, Namespace namespace) {
    List<TableIdentifier> idents = catalog.listTables(namespace);
    return ListTablesResponse.builder().addAll(idents).build();
  }

  public static LoadTableResponse createTable(Catalog catalog, Namespace namespace, CreateTableRequest request) {
    request.validate();

    TableIdentifier ident = TableIdentifier.of(namespace, request.name());
    Table table = catalog.buildTable(ident, request.schema())
        .withLocation(request.location())
        .withPartitionSpec(request.spec())
        .withSortOrder(request.writeOrder())
        .withProperties(request.properties())
        .create();

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }

  public static LoadTableResponse loadTable(Catalog catalog, TableIdentifier ident) {
    Table table = catalog.loadTable(ident);

    if (table instanceof BaseTable) {
      return LoadTableResponse.builder()
          .withTableMetadata(((BaseTable) table).operations().current())
          .build();
    }

    throw new IllegalStateException("Cannot wrap catalog that does not produce BaseTable");
  }
}
