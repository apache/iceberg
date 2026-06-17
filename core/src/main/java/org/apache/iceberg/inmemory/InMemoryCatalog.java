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
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
 * <p>By default each instance owns a private store and {@link #close()} clears it. Setting the
 * {@link #SHARED_STORE_ID} catalog property links instances initialized with the same value to a
 * shared, JVM-wide store: namespaces, tables, and views committed through one instance are visible
 * to others, and {@link #close()} leaves the shared store intact so the remaining instances
 * continue to observe it. The shared store survives for the lifetime of the JVM unless {@link
 * #clearSharedStore} is called explicitly.
 *
 * <p>Mutating operations synchronize on the backing store rather than on this catalog instance, so
 * all instances pointing at the same shared store serialize through the same monitor. The catalog
 * instance's intrinsic lock is not the synchronization root and must not be relied on for external
 * coordination.
 */
public class InMemoryCatalog extends BaseMetastoreViewCatalog
    implements SupportsNamespaces, Closeable {
  /**
   * Catalog property whose value identifies a JVM-wide shared store. Two {@link InMemoryCatalog}
   * instances initialized with the same value of this property share the same namespaces, tables,
   * views, and synchronization monitor. When unset, each instance keeps a private store. Empty or
   * whitespace-only values are rejected because they are typically a misconfiguration produced by
   * config layers that map "missing" to the empty string.
   */
  public static final String SHARED_STORE_ID = "in-memory-catalog.shared-store-id";

  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private static final ConcurrentMap<String, Store> SHARED_STORES = Maps.newConcurrentMap();

  /**
   * Drops the shared store associated with the given {@link #SHARED_STORE_ID} value. Subsequent
   * instances initialized with that value start with an empty store; live instances that already
   * hold a reference to the previous store keep observing it. Calling this with a value that has no
   * shared store is a no-op. Intended for harnesses that need to release accumulated state between
   * runs.
   */
  public static void clearSharedStore(String sharedStoreId) {
    SHARED_STORES.remove(sharedStoreId);
  }

  private volatile Store store;
  private boolean ownsStore;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;
  private CloseableGroup closeableGroup;
  private boolean uniqueTableLocation;
  private Map<String, String> catalogProperties;

  public InMemoryCatalog() {
    this.store = new Store();
    this.ownsStore = true;
  }

  @Override
  public String name() {
    return catalogName;
  }

  /**
   * Initializes the catalog. May be called more than once on the same instance: a subsequent call
   * closes the previous {@link CloseableGroup} (and the {@link FileIO} / metrics reporter it holds)
   * and reselects the backing store from the new properties. There is no thread-safety guarantee
   * for re-initialization concurrent with in-flight catalog operations.
   */
  @Override
  public void initialize(String name, Map<String, String> properties) {
    String sharedStoreId = properties.get(SHARED_STORE_ID);
    if (sharedStoreId != null) {
      Preconditions.checkArgument(
          !sharedStoreId.trim().isEmpty(),
          "%s must not be empty or whitespace; either omit the property or provide a non-blank value",
          SHARED_STORE_ID);
    }

    closePreviousCloseableGroup();

    this.catalogName = name != null ? name : InMemoryCatalog.class.getSimpleName();
    this.catalogProperties = ImmutableMap.copyOf(properties);
    this.uniqueTableLocation =
        PropertyUtil.propertyAsBoolean(
            properties,
            CatalogProperties.UNIQUE_TABLE_LOCATION,
            CatalogProperties.UNIQUE_TABLE_LOCATION_DEFAULT);

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");

    if (sharedStoreId != null) {
      this.store = SHARED_STORES.computeIfAbsent(sharedStoreId, k -> new Store());
      this.ownsStore = false;
    } else {
      this.store = new Store();
      this.ownsStore = true;
    }

    this.io = CatalogUtil.loadFileIO(InMemoryFileIO.class.getName(), properties, null);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(metricsReporter());
    closeableGroup.setSuppressCloseFailure(true);
  }

  private void closePreviousCloseableGroup() {
    CloseableGroup previous = this.closeableGroup;
    this.closeableGroup = null;
    if (previous != null) {
      try {
        previous.close();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to close previous catalog resources", e);
      }
    }
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

    Store currentStore = store;
    synchronized (currentStore) {
      if (null == currentStore.tables.remove(tableIdentifier)) {
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
    Store currentStore = store;
    if (!currentStore.namespaces.containsKey(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace. Namespace does not exist: %s", namespace);
    }

    return currentStore.tables.keySet().stream()
        .filter(t -> t.namespace().equals(namespace))
        .sorted(Comparator.comparing(TableIdentifier::toString))
        .collect(Collectors.toList());
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    Store currentStore = store;
    synchronized (currentStore) {
      if (!currentStore.namespaces.containsKey(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      String fromLocation = currentStore.tables.get(from);
      if (null == fromLocation) {
        throw new NoSuchTableException("Cannot rename %s to %s. Table does not exist", from, to);
      }

      if (currentStore.tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (currentStore.views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      currentStore.tables.put(to, fromLocation);
      currentStore.tables.remove(from);
    }
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    Store currentStore = store;
    synchronized (currentStore) {
      if (currentStore.namespaces.containsKey(namespace)) {
        throw new AlreadyExistsException(
            "Cannot create namespace %s. Namespace already exists", namespace);
      }

      currentStore.namespaces.put(namespace, ImmutableMap.copyOf(metadata));
    }
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return store.namespaces.containsKey(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    Store currentStore = store;
    synchronized (currentStore) {
      if (!currentStore.namespaces.containsKey(namespace)) {
        return false;
      }

      long childNamespaces = childNamespacesOf(currentStore, namespace).count();
      if (childNamespaces > 0) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d child namespace(s).",
            namespace, childNamespaces);
      }

      long tablesInNamespace = countIdentifiersInNamespace(currentStore.tables, namespace);
      if (tablesInNamespace > 0) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d table(s).", namespace, tablesInNamespace);
      }

      long viewsInNamespace = countIdentifiersInNamespace(currentStore.views, namespace);
      if (viewsInNamespace > 0) {
        throw new NamespaceNotEmptyException(
            "Namespace %s is not empty. Contains %d view(s).", namespace, viewsInNamespace);
      }

      return currentStore.namespaces.remove(namespace) != null;
    }
  }

  /**
   * Returns the namespaces that are strict descendants of {@code parent} in {@code currentStore}.
   * Implemented as a single shared traversal so that {@link #dropNamespace(Namespace)} and {@link
   * #listNamespaces(Namespace)} cannot drift apart on prefix-collision edge cases (for example,
   * {@code a.b} as a parent must not match {@code a.bb.c}).
   */
  private static Stream<Namespace> childNamespacesOf(Store currentStore, Namespace parent) {
    int parentLength = parent.levels().length;
    String[] parentLevels = parent.levels();
    return currentStore.namespaces.keySet().stream()
        .filter(n -> n.levels().length > parentLength)
        .filter(n -> Arrays.equals(Arrays.copyOf(n.levels(), parentLength), parentLevels));
  }

  private static long countIdentifiersInNamespace(
      ConcurrentMap<TableIdentifier, String> identifiers, Namespace namespace) {
    return identifiers.keySet().stream().filter(id -> id.namespace().equals(namespace)).count();
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    Store currentStore = store;
    synchronized (currentStore) {
      if (!currentStore.namespaces.containsKey(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      currentStore.namespaces.computeIfPresent(
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
    Store currentStore = store;
    synchronized (currentStore) {
      if (!currentStore.namespaces.containsKey(namespace)) {
        throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
      }

      currentStore.namespaces.computeIfPresent(
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
    Map<String, String> properties = store.namespaces.get(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return ImmutableMap.copyOf(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return store.namespaces.keySet().stream()
        .filter(n -> !n.isEmpty())
        .map(n -> n.level(0))
        .distinct()
        .sorted()
        .map(Namespace::of)
        .collect(Collectors.toList());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    Store currentStore = store;
    int rootLength = namespace.levels().length;

    List<Namespace> descendants =
        childNamespacesOf(currentStore, namespace).collect(Collectors.toList());

    // If the namespace does not exist and the namespace is not a prefix of another namespace,
    // throw an exception.
    if (!currentStore.namespaces.containsKey(namespace) && descendants.isEmpty()) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return descendants.stream()
        // List only the child-namespaces roots.
        .map(n -> Namespace.of(Arrays.copyOf(n.levels(), rootLength + 1)))
        .distinct()
        .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
        .collect(Collectors.toList());
  }

  /**
   * Closes resources held by this catalog. When the instance owns a private store the in-memory
   * namespaces, tables, and views are cleared. When it shares a store (initialized with {@link
   * #SHARED_STORE_ID}) the store is left intact so other instances continue to observe it; use
   * {@link #clearSharedStore} to drop it explicitly.
   */
  @Override
  public void close() throws IOException {
    if (closeableGroup != null) {
      closeableGroup.close();
    }
    if (ownsStore) {
      store.clear();
    }
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    Store currentStore = store;
    if (!currentStore.namespaces.containsKey(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list views for namespace. Namespace does not exist: %s", namespace);
    }

    return currentStore.views.keySet().stream()
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
    Store currentStore = store;
    synchronized (currentStore) {
      return null != currentStore.views.remove(identifier);
    }
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }

    Store currentStore = store;
    synchronized (currentStore) {
      if (!currentStore.namespaces.containsKey(to.namespace())) {
        throw new NoSuchNamespaceException(
            "Cannot rename %s to %s. Namespace does not exist: %s", from, to, to.namespace());
      }

      String fromViewLocation = currentStore.views.get(from);
      if (null == fromViewLocation) {
        throw new NoSuchViewException("Cannot rename %s to %s. View does not exist", from, to);
      }

      if (currentStore.tables.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. Table already exists", from, to);
      }

      if (currentStore.views.containsKey(to)) {
        throw new AlreadyExistsException("Cannot rename %s to %s. View already exists", from, to);
      }

      currentStore.views.put(to, fromViewLocation);
      currentStore.views.remove(from);
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
      String latestLocation = store.tables.get(tableIdentifier);
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

      Store currentStore = store;
      synchronized (currentStore) {
        if (null == base && !currentStore.namespaces.containsKey(tableIdentifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create table %s. Namespace does not exist: %s",
              tableIdentifier, tableIdentifier.namespace());
        }

        if (currentStore.views.containsKey(tableIdentifier)) {
          throw new AlreadyExistsException(
              "View with same name already exists: %s", tableIdentifier);
        }

        currentStore.tables.compute(
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
      String latestLocation = store.views.get(identifier);
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

      Store currentStore = store;
      synchronized (currentStore) {
        if (null == base && !currentStore.namespaces.containsKey(identifier.namespace())) {
          throw new NoSuchNamespaceException(
              "Cannot create view %s. Namespace does not exist: %s",
              identifier, identifier.namespace());
        }

        if (currentStore.tables.containsKey(identifier)) {
          throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
        }

        currentStore.views.compute(
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

  private static final class Store {
    private final ConcurrentMap<Namespace, Map<String, String>> namespaces =
        Maps.newConcurrentMap();
    private final ConcurrentMap<TableIdentifier, String> tables = Maps.newConcurrentMap();
    private final ConcurrentMap<TableIdentifier, String> views = Maps.newConcurrentMap();

    void clear() {
      namespaces.clear();
      tables.clear();
      views.clear();
    }
  }
}
