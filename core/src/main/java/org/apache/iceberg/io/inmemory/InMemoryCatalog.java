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

package org.apache.iceberg.io.inmemory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catalog implementation that uses in-memory data-structures to
 * store the namespaces and tables.
 * This class doesn't touch external resources and
 * can be utilized to write unit tests without side effects.
 * It uses {@link InMemoryFileIO}.
 * <p>
 * This catalog supports namespaces.
 * This catalog automatically creates namespaces if needed during table creation.
 */
public class InMemoryCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private final ConcurrentMap<Namespace, Map<String, String>> namespaceDb;
  private final ConcurrentMap<TableIdentifier, String> tableDb;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;

  public InMemoryCatalog() {
    namespaceDb = new ConcurrentHashMap<>();
    tableDb = new ConcurrentHashMap<>();
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
    return SLASH.join(defaultNamespaceLocation(tableIdentifier.namespace()), tableIdentifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public Table createTable(
      TableIdentifier tableIdentifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {

    // Create namespace automatically if it does not exist.
    if (!namespaceExists(tableIdentifier.namespace())) {
      createNamespace(tableIdentifier.namespace());
    }

    return super.createTable(tableIdentifier, schema, spec, location, properties);
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

    if (tableDb.remove(tableIdentifier) == null) {
      LOG.info("Table does not exist: cannot drop table {}", tableIdentifier);
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
          "Namespace does not exist: cannot list tables for namespace %s", namespace);
    }

    return tableDb.keySet().stream()
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
      throw new NoSuchNamespaceException("Cannot rename %s to %s because the namespace %s does not exist",
          fromTableIdentifier, toTableIdentifier, toTableIdentifier.namespace());
    }

    if (!tableDb.containsKey(fromTableIdentifier)) {
      throw new NoSuchTableException("Cannot rename %s to %s because the table %s does not exist",
          fromTableIdentifier, toTableIdentifier, fromTableIdentifier);
    }

    if (tableDb.containsKey(toTableIdentifier)) {
      throw new AlreadyExistsException("Cannot rename %s to %s because the table %s already exists",
          fromTableIdentifier, toTableIdentifier, toTableIdentifier);
    }

    String fromLocation = tableDb.remove(fromTableIdentifier);
    if (fromLocation != null) {
      tableDb.put(toTableIdentifier, fromLocation);
    } else {
      throw new IllegalStateException(String.format(
          "Cannot rename from %s to %s because source table does not exist",
          fromTableIdentifier, toTableIdentifier));
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException("Namespace already exists: %s. Cannot create namespace", namespace);
    }

    namespaceDb.put(namespace, ImmutableMap.copyOf(metadata));
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return namespaceDb.containsKey(namespace);
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

    return namespaceDb.remove(namespace) != null;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    namespaceDb.compute(namespace, (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("Namespace does not exist: " + namespace);
      } else {
        Map<String, String> newProperties = Maps.newHashMap(v);
        newProperties.putAll(properties);
        return newProperties;
      }
    });

    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    namespaceDb.compute(namespace, (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("Namespace does not exist: " + namespace);
      } else {
        Map<String, String> newProperties = Maps.newHashMap(v);
        properties.forEach(newProperties::remove);
        return newProperties;
      }
    });

    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Map<String, String> properties = namespaceDb.get(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return Maps.newHashMap(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return namespaceDb.keySet().stream()
        .filter(n -> !n.isEmpty())
        .map(n -> n.level(0))
        .distinct()
        .sorted()
        .map(Namespace::of)
        .collect(Collectors.toList());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    final String searchNamespaceString = namespace.isEmpty() ? "" : DOT.join(namespace.levels()) + ".";
    final int searchNumberOfLevels = namespace.levels().length;

    List<Namespace> filteredNamespaces = namespaceDb.keySet().stream()
        .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
        .collect(Collectors.toList());

    // If the namespace does not exist and the namespace is not a prefix of another namespace,
    // throw an exception.
    if (!namespaceDb.containsKey(namespace) && filteredNamespaces.isEmpty()) {
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
    namespaceDb.clear();
    tableDb.clear();
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
      String latestLocation = tableDb.get(tableIdentifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      // Create namespace automatically if it does not exist.
      // This is needed to handle transactions.
      if (!namespaceExists(tableIdentifier.namespace())) {
        createNamespace(tableIdentifier.namespace());
      }

      // Commit the table.
      String newLocation = writeNewMetadata(metadata, currentVersion() + 1);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      tableDb.compute(tableIdentifier, (k, existingLocation) -> {
        if (!Objects.equal(existingLocation, oldLocation)) {
          throw new CommitFailedException("Cannot create/update table %s metadata location from %s to %s" +
              " because it has been concurrently modified to %s",
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
