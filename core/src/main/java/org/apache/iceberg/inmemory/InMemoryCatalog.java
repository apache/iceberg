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
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.view.BaseMetastoreViewCatalog;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewOperations;
import org.apache.iceberg.view.ViewUtil;

/**
 * Catalog implementation that uses in-memory data-structures to store namespaces, tables, and
 * views. It uses {@link InMemoryFileIO} so it does not touch external resources, which makes it
 * well-suited to writing unit tests.
 *
 * <p>By default each instance owns a private store, and {@link #close()} drops it. Setting the
 * {@link #INSTANCE_ID} catalog property links instances initialized with the same value to a
 * shared, JVM-wide store: namespaces, tables, and views committed through one instance are visible
 * to others, and {@link #close()} leaves the shared store intact so the remaining instances
 * continue to observe it. The shared store survives until {@link #clearInstanceState} is called or
 * the JVM exits.
 */
public class InMemoryCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, Closeable {
  /**
   * Catalog property identifying a JVM-wide shared store. Two {@link InMemoryCatalog} instances
   * initialized with the same value of this property share the same namespaces, tables, views, and
   * synchronization monitor. When unset, each instance keeps its own private store.
   */
  public static final String INSTANCE_ID = "in-memory-catalog.instance-id";

  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private static final ConcurrentMap<String, Storage> SHARED_STORES = Maps.newConcurrentMap();

  private volatile Storage storage;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;
  private CloseableGroup closeableGroup;
  private boolean uniqueTableLocation;
  private Map<String, String> catalogProperties;

  public InMemoryCatalog() {
    this.storage = new Storage(false);
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name != null ? name : InMemoryCatalog.class.getSimpleName();
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.uniqueTableLocation =
        PropertyUtil.propertyAsBoolean(
            properties,
            CatalogProperties.UNIQUE_TABLE_LOCATION,
            CatalogProperties.UNIQUE_TABLE_LOCATION_DEFAULT);

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");

    String instanceId = properties.get(INSTANCE_ID);
    if (instanceId != null) {
      this.storage = SHARED_STORES.computeIfAbsent(instanceId, k -> new Storage(true));
    } else {
      this.storage = new Storage(false);
    }

    this.io = CatalogUtil.loadFileIO(InMemoryFileIO.class.getName(), properties, null);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);
  }

  /**
   * Drops the shared store associated with the given {@link #INSTANCE_ID} value. After this call,
   * subsequent instances initialized with that value start with an empty store. Calling this with
   * an unknown value is a no-op.
   */
  @VisibleForTesting
  static void clearInstanceState(String instanceId) {
    SHARED_STORES.remove(instanceId);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new InMemoryTableOperations(io, tableIdentifier);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String tableLocation = LocationUtil.tableLocation(tableIdentifier, uniqueTableLocation);
    return SLASH.join(defaultNamespaceLocation(tableIdentifier.namespace()), tableLocation);
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

    Storage current = storage;
    synchronized (current) {
      if (null == current.tables.remove(tableIdentifier)) {
        return false;
      }
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

    return storage.tables.keySet().stream()
        .filter(t -> t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    Storage current = storage;
    synchronized (current) {
      if (!current.namespaces.containsKey(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      String fromLocation = current.tables.get(from);
      if (null == fromLocation) {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }

      if (current.tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (current.views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      current.tables.put(to, fromLocation);
      current.tables.remove(from);
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Storage current = storage;
    synchronized (current) {
      if (current.namespaces.containsKey(namespace)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }

      current.namespaces.put(namespace, ImmutableMap.copyOf(metadata));
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return storage.namespaces.containsKey(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    Storage current = storage;
    synchronized (current) {
      if (!current.namespaces.containsKey(namespace)) {
        return false;
      }

      List<Namespace> childNamespaces = listNamespaces(namespace);
      if (!childNamespaces.isEmpty()) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d child namespace(s).",
            namespace, childNamespaces.size());
      }

      List<TableIdentifier> tableIdentifiers = listTables(namespace);
      if (!tableIdentifiers.isEmpty()) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d table(s).", namespace, tableIdentifiers.size());
      }

      List<TableIdentifier> viewIdentifiers = listViews(namespace);
      if (!viewIdentifiers.isEmpty()) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d view(s).", namespace, viewIdentifiers.size());
      }

      return current.namespaces.remove(namespace) != null;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    Storage current = storage;
    synchronized (current) {
      if (!current.namespaces.containsKey(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      current.namespaces.computeIfPresent(
          namespace,
          (k, v) ->
              ImmutableMap.<String, String>builder()
                  .putAll(v)
                  .putAll(properties)
                  .buildKeepingLast());

      return true;
    }
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    Storage current = storage;
    synchronized (current) {
      if (!current.namespaces.containsKey(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      current.namespaces.computeIfPresent(
          namespace,
          (k, v) -> {
            Map<String, String> newProperties = Maps.newHashMap(v);
            properties.forEach(newProperties::remove);
            return ImmutableMap.copyOf(newProperties);
          });

      return true;
    }
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    Map<String, String> properties = storage.namespaces.get(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.copyOf(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return storage.namespaces.keySet().stream()
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

    Storage current = storage;
    List<Namespace> filteredNamespaces =
        current.namespaces.keySet().stream()
            .filter(n -> !n.isEmpty())
            .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
            .collect(Collectors.toList());

    // If the namespace does not exist and the namespace is not a prefix of another namespace,
    // throw an exception.
    if (!current.namespaces.containsKey(namespace) && filteredNamespaces.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return filteredNamespaces.stream()
        .map(n -> Namespace.of(Arrays.copyOf(n.levels(), searchNumberOfLevels + 1)))
        .distinct()
        .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
        .collect(Collectors.toList());
  }

  /**
   * Closes resources held by this catalog. When the instance owns a private store the in-memory
   * namespaces, tables, and views are dropped. When it shares a store (initialized with {@link
   * #INSTANCE_ID}) the store is left intact so other instances continue to observe it; use {@link
   * #clearInstanceState} to drop it explicitly.
   */
  @Override
  public void close() throws IOException {
    closeableGroup.close();
    Storage current = storage;
    if (!current.shared) {
      current.namespaces.clear();
      current.tables.clear();
      current.views.clear();
    }
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list views for namespace. Namespace does not exist: %s", namespace);
    }

    return storage.views.keySet().stream()
        .filter(v -> v.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  protected ViewOperations newViewOps(TableIdentifier identifier) {
    return new InMemoryViewOperations(io, identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    Storage current = storage;
    synchronized (current) {
      return null != current.views.remove(identifier);
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    Storage current = storage;
    synchronized (current) {
      if (!current.namespaces.containsKey(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      String fromViewLocation = current.views.get(from);
      if (null == fromViewLocation) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      }

      if (current.tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (current.views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      current.views.put(to, fromViewLocation);
      current.views.remove(from);
    }
  }

  @Override
  protected Map<String, String> properties() {
    return catalogProperties == null ? ImmutableMap.of() : catalogProperties;
  }

  private class InMemoryTableOperations extends BaseMetastoreTableOperations {
    private final FileIO fileIO;
    private final TableIdentifier tableIdentifier;
    private final String fullTableName;

    InMemoryTableOperations(FileIO fileIO, TableIdentifier tableIdentifier) {
      this.fileIO = fileIO;
      this.tableIdentifier = tableIdentifier;
      this.fullTableName = fullTableName(catalogName, tableIdentifier);
    }

    @Override
    public void doRefresh() {
      String latestLocation = storage.tables.get(tableIdentifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(TableMetadata base, TableMetadata metadata) {
      String newLocation = writeNewMetadataIfRequired(base == null, metadata);
      String oldLocation = base == null ? null : base.metadataFileLocation();

      Storage current = storage;
      synchronized (current) {
        if (null == base && !current.namespaces.containsKey(tableIdentifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create table %s. Namespace does not exist: %s",
              tableIdentifier, tableIdentifier.namespace());
        }

        if (current.views.containsKey(tableIdentifier)) {
          throw new AlreadyExistsException(
              "View with same name already exists: %s", tableIdentifier);
        }

        current.tables.compute(
            tableIdentifier,
            (k, existingLocation) -> {
              if (!Objects.equal(existingLocation, oldLocation)) {
                if (null == base) {
                  throw new AlreadyExistsException("Table already exists: %s", tableName());
                }

                if (null == existingLocation) {
                  throw new NoSuchTableException("Table does not exist: %s", tableName());
                }

                throw new CommitFailedException(
                    "Cannot commit to table %s metadata location from %s to %s "
                        + "because it has been concurrently modified to %s",
                    tableIdentifier, oldLocation, newLocation, existingLocation);
              }
              return newLocation;
            });
      }
    }

    @Override
    public FileIO io() {
      return fileIO;
    }

    @Override
    protected String tableName() {
      return fullTableName;
    }
  }

  private class InMemoryViewOperations extends BaseViewOperations {
    private final FileIO io;
    private final TableIdentifier identifier;
    private final String fullViewName;

    InMemoryViewOperations(FileIO io, TableIdentifier identifier) {
      this.io = io;
      this.identifier = identifier;
      this.fullViewName = ViewUtil.fullViewName(catalogName, identifier);
    }

    @Override
    public void doRefresh() {
      String latestLocation = storage.views.get(identifier);
      if (latestLocation == null) {
        disableRefresh();
      } else {
        refreshFromMetadataLocation(latestLocation);
      }
    }

    @Override
    public void doCommit(ViewMetadata base, ViewMetadata metadata) {
      String newLocation = writeNewMetadataIfRequired(metadata);
      String oldLocation = base == null ? null : currentMetadataLocation();

      Storage current = storage;
      synchronized (current) {
        if (null == base && !current.namespaces.containsKey(identifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create view %s. Namespace does not exist: %s",
              identifier, identifier.namespace());
        }

        if (current.tables.containsKey(identifier)) {
          throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
        }

        current.views.compute(
            identifier,
            (k, existingLocation) -> {
              if (!Objects.equal(existingLocation, oldLocation)) {
                if (null == base) {
                  throw new AlreadyExistsException("View already exists: %s", identifier);
                }

                if (null == existingLocation) {
                  throw new NoSuchViewException("View does not exist: %s", identifier);
                }

                throw new CommitFailedException(
                    "Cannot commit to view %s metadata location from %s to %s "
                        + "because it has been concurrently modified to %s",
                    identifier, oldLocation, newLocation, existingLocation);
              }

              return newLocation;
            });
      }
    }

    @Override
    public FileIO io() {
      return io;
    }

    @Override
    protected String viewName() {
      return fullViewName;
    }
  }

  private static final class Storage {
    private final boolean shared;
    private final ConcurrentMap<Namespace, Map<String, String>> namespaces =
        Maps.newConcurrentMap();
    private final ConcurrentMap<TableIdentifier, String> tables = Maps.newConcurrentMap();
    private final ConcurrentMap<TableIdentifier, String> views = Maps.newConcurrentMap();

    Storage(boolean shared) {
      this.shared = shared;
    }
  }
}
