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
package org.apache.iceberg.inmemory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * Catalog implementation that uses in-memory data-structures to store the namespaces and tables.
 * This class doesn't touch external resources and can be utilized to write unit tests without side
 * effects. It uses {@link InMemoryFileIO}.
 */
public class InMemoryCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Closeable {
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private final ConcurrentMap<Namespace, Map<String, String>> namespaces;
  private final ConcurrentMap<TableIdentifier, String> tables;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;

  public InMemoryCatalog() {
    this.namespaces = Maps.newConcurrentMap();
    this.tables = Maps.newConcurrentMap();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name != null ? name : InMemoryCatalog.class.getSimpleName();

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");
    this.io = new InMemoryFileIO();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new InMemoryTableOperations(io, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return SLASH.join(
        defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public boolean dropTable(TableIdentifier tableIdentifier, boolean purge) {
    TableOperations ops = newTableOps(tableIdentifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    if (null == tables.remove(tableIdentifier)) {
      return false;
    }

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }

    return tables.keySet().stream()
        .filter(t -> namespace.isEmpty() || t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public void renameTable(TableIdentifier fromTableIdentifier, TableIdentifier toTableIdentifier) {
    if (fromTableIdentifier.equals(toTableIdentifier)) {
      return;
    }

    if (!namespaceExists(toTableIdentifier.namespace())) {
      throw new NoSuchNamespaceException(
          "Cannot rename %s to %s. Namespace does not exist: %s",
          fromTableIdentifier, toTableIdentifier, toTableIdentifier.namespace());
    }

    if (!tables.containsKey(fromTableIdentifier)) {
      throw new NoSuchTableException(
          "Cannot rename %s to %s. Table does not exist", fromTableIdentifier, toTableIdentifier);
    }

    if (tables.containsKey(toTableIdentifier)) {
      throw new AlreadyExistsException(
          "Cannot rename %s to %s. Table already exists", fromTableIdentifier, toTableIdentifier);
    }

    String fromLocation = tables.remove(fromTableIdentifier);
    Preconditions.checkState(
        null != fromLocation,
        "Cannot rename from %s to %s. Source table does not exist",
        fromTableIdentifier,
        toTableIdentifier);

    tables.put(toTableIdentifier, fromLocation);
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException(
          "Cannot create namespace %s. Namespace already exists", namespace);
    }

    namespaces.put(namespace, ImmutableMap.copyOf(metadata));
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return namespaces.containsKey(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespaceExists(namespace)) {
      return false;
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (!tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException(
          "Namespace %s is not empty. Contains %d table(s).", namespace, tableIdentifiers.size());
    }

    return namespaces.remove(namespace) != null;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    namespaces.computeIfPresent(
        namespace,
        (k, v) ->
            ImmutableMap.<String, String>builder().putAll(v).putAll(properties).buildKeepingLast());

    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    namespaces.computeIfPresent(
        namespace,
        (k, v) -> {
          Map<String, String> newProperties = Maps.newHashMap(v);
          properties.forEach(newProperties::remove);
          return ImmutableMap.copyOf(newProperties);
        });

    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    Map<String, String> properties = namespaces.get(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.copyOf(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return namespaces.keySet().stream()
        .filter(n -> !n.isEmpty())
        .map(n -> n.level(0))
        .distinct()
        .sorted()
        .map(Namespace::of)
        .collect(Collectors.toList());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    final String searchNamespaceString =
        namespace.isEmpty() ? "" : DOT.join(namespace.levels()) + ".";
    final int searchNumberOfLevels = namespace.levels().length;

    List<Namespace> filteredNamespaces =
        namespaces.keySet().stream()
            .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
            .collect(Collectors.toList());

    // If the namespace does not exist and the namespace is not a prefix of another namespace,
    // throw an exception.
    if (!namespaces.containsKey(namespace) && filteredNamespaces.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return filteredNamespaces.stream()
        // List only the child-namespaces roots.
        .map(n -> Namespace.of(Arrays.copyOf(n.levels(), searchNumberOfLevels + 1)))
        .distinct()
        .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
        .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    namespaces.clear();
    tables.clear();
  }

  private class InMemoryTableOperations extends BaseMetastoreTableOperations {
    private final FileIO fileIO;
    private final TableIdentifier tableIdentifier;

    InMemoryTableOperations(FileIO fileIO, TableIdentifier tableIdentifier) {
      this.fileIO = fileIO;
      this.tableIdentifier = tableIdentifier;
    }

    @Override
    public void doRefresh() {
      String latestLocation = tables.get(tableIdentifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      String newLocation = writeNewMetadata(metadata, currentVersion() + 1);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      if (null == base && !namespaceExists(tableIdentifier.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot create table %s. Namespace does not exist: %s",
            tableIdentifier, tableIdentifier.namespace());
      }

      tables.compute(
          tableIdentifier,
          (k, existingLocation) -> {
            if (!Objects.equal(existingLocation, oldLocation)) {
              if (null == base) {
                throw new AlreadyExistsException("Table already exists: %s", tableName());
              }

              throw new CommitFailedException(
                  "Cannot commit to table %s metadata location from %s to %s "
                      + "because it has been concurrently modified to %s",
                  tableIdentifier, oldLocation, newLocation, existingLocation);
            }
            return newLocation;
          });
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    protected String tableName() {
      return tableIdentifier.toString();
    }
  }
}
