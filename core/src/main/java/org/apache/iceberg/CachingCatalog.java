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
package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that wraps an Iceberg Catalog to cache tables.
 *
 * <p>See {@link CatalogProperties#CACHE_EXPIRATION_INTERVAL_MS} for more details regarding special
 * values for {@code expirationIntervalMillis}.
 */
public class CachingCatalog implements Catalog {
  private static final Logger LOG = LoggerFactory.getLogger(CachingCatalog.class);
  private static final MetadataTableType[] METADATA_TABLE_TYPE_VALUES = MetadataTableType.values();

  public static Catalog wrap(Catalog catalog) {
    return wrap(catalog, CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_OFF);
  }

  public static Catalog wrap(Catalog catalog, long expirationIntervalMillis) {
    return wrap(catalog, true, expirationIntervalMillis);
  }

  public static Catalog wrap(
      Catalog catalog, boolean caseSensitive, long expirationIntervalMillis) {
    return new CachingCatalog(catalog, caseSensitive, expirationIntervalMillis);
  }

  private final Catalog catalog;
  private final boolean caseSensitive;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final long expirationIntervalMillis;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final Cache<TableIdentifier, Table> tableCache;

  private final Set<String> invalidReplacementTableTypeNames = ConcurrentHashMap.newKeySet();

  private CachingCatalog(Catalog catalog, boolean caseSensitive, long expirationIntervalMillis) {
    this(catalog, caseSensitive, expirationIntervalMillis, Ticker.systemTicker());
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected CachingCatalog(
      Catalog catalog, boolean caseSensitive, long expirationIntervalMillis, Ticker ticker) {
    Preconditions.checkArgument(
        expirationIntervalMillis != 0,
        "When %s is set to 0, the catalog cache should be disabled. This indicates a bug.",
        CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS);
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
    this.expirationIntervalMillis = expirationIntervalMillis;
    this.tableCache = createTableCache(ticker);
  }

  /**
   * RemovalListener class for removing metadata tables when their associated data table is expired
   * via cache expiration.
   */
  class MetadataTableInvalidatingRemovalListener
      implements RemovalListener<TableIdentifier, Table> {
    @Override
    public void onRemoval(TableIdentifier tableIdentifier, Table table, RemovalCause cause) {
      LOG.debug("Evicted {} from the table cache ({})", tableIdentifier, cause);
      if (RemovalCause.EXPIRED.equals(cause)) {
        if (!MetadataTableUtils.hasMetadataTableName(tableIdentifier)) {
          tableCache.invalidateAll(metadataTableIdentifiers(tableIdentifier));
        }
      }
    }
  }

  private Cache<TableIdentifier, Table> createTableCache(Ticker ticker) {
    Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().softValues();

    if (expirationIntervalMillis > 0) {
      return cacheBuilder
          .removalListener(new MetadataTableInvalidatingRemovalListener())
          .executor(Runnable::run) // Makes the callbacks to removal listener synchronous
          .expireAfterAccess(Duration.ofMillis(expirationIntervalMillis))
          .ticker(ticker)
          .build();
    }

    return cacheBuilder.build();
  }

  private TableIdentifier canonicalizeIdentifier(TableIdentifier tableIdentifier) {
    if (caseSensitive) {
      return tableIdentifier;
    } else {
      return tableIdentifier.toLowerCase();
    }
  }

  @Override
  public String name() {
    return catalog.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return catalog.listTables(namespace);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    TableIdentifier canonicalized = canonicalizeIdentifier(ident);
    Table cached = tableCache.getIfPresent(canonicalized);
    if (cached != null) {
      return cached;
    }

    Table table;
    try {
      table = tableCache.get(canonicalized, this::loadTableForCache);
    } catch (UncacheableTableException e) {
      return e.table();
    }

    if (table instanceof BaseMetadataTable) {
      // Cache underlying table
      TableIdentifier originTableIdentifier =
          TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable;
      try {
        originTable = tableCache.get(originTableIdentifier, this::loadTableForCache);
      } catch (UncacheableTableException e) {
        tableCache.invalidate(canonicalized);
        return table;
      }

      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());

        Table metadataTable =
            MetadataTableUtils.createMetadataTableInstance(
                ops, catalog.name(), originTableIdentifier, canonicalized, type);
        return publishMetadataTable(
            canonicalized, table, originTableIdentifier, originTable, metadataTable);
      }
    }

    return table;
  }

  private Table loadTableForCache(TableIdentifier identifier) {
    Table loaded = catalog.loadTable(identifier);
    if (loaded instanceof BaseMetadataTable) {
      return loaded;
    }

    return requireCacheable(prepareTable(identifier, loaded));
  }

  private Table publishMetadataTable(
      TableIdentifier identifier,
      Table initiallyLoaded,
      TableIdentifier originIdentifier,
      Table originTable,
      Table metadataTable) {
    Table published =
        tableCache
            .asMap()
            .compute(
                identifier,
                (ignored, current) -> {
                  // If this entry was invalidated or replaced after its initial load, do not
                  // resurrect or overwrite it.
                  if (current != initiallyLoaded) {
                    return current;
                  }

                  // Do not replace this quiet lookup with getIfPresent. Recording an access can
                  // schedule cache maintenance while this mapping callback is running.
                  Table currentOrigin = tableCache.policy().getIfPresentQuietly(originIdentifier);
                  if (currentOrigin != originTable) {
                    // Invalidation removes the origin entry before its metadata entries. If the
                    // origin changed while this metadata table was being prepared, remove the
                    // initially loaded entry.
                    return null;
                  }

                  return metadataTable;
                });
    return published != null ? published : metadataTable;
  }

  /**
   * Installs commit-tracking operations on a table handed out by this cache, so that a commit
   * performed through the table removes a different table cached during the write (and invalidates
   * its metadata tables). Otherwise a table that was reloaded during an in-flight write keeps
   * serving the pre-commit snapshot until the entry expires. See
   * https://github.com/apache/iceberg/issues/17338.
   *
   * <p>The concrete table type is preserved. A plain {@link BaseTable} is re-created directly. A
   * subclass opts in by implementing {@link SupportsOperationsReplacement} (for example {@code
   * RESTTable}, which keeps its server-side scan planning). Other table implementations retain the
   * existing caching behavior without commit tracking. Metadata tables share the safely prepared
   * origin table's operations and are invalidated together with it.
   */
  private PreparedTable prepareTable(TableIdentifier identifier, Table table) {
    TableOperations operations;
    Table wrapped;
    if (table instanceof SupportsOperationsReplacement replaceable) {
      operations = replaceable.operations();
      CacheInvalidatingTableOperations replacementOperations =
          new CacheInvalidatingTableOperations(operations, identifier);
      try {
        wrapped = replaceable.withOperations(replacementOperations);
      } catch (RuntimeException e) {
        warnInvalidReplacement(identifier, table, "withOperations threw an exception", e);
        return PreparedTable.uncacheable(table);
      }

      if (wrapped != null
          && wrapped != table
          && wrapped.getClass() == table.getClass()
          && ((SupportsOperationsReplacement) wrapped).operations() == replacementOperations
          && replaceable.operations() == operations) {
        replacementOperations.track(wrapped);
        return PreparedTable.cacheable(wrapped);
      }

      warnInvalidReplacement(
          identifier,
          table,
          "withOperations must return an independent copy of the same concrete type without "
              + "modifying the original table",
          null);
      return PreparedTable.uncacheable(table);
    } else if (table.getClass() == BaseTable.class) {
      BaseTable baseTable = (BaseTable) table;
      operations = baseTable.operations();
      CacheInvalidatingTableOperations replacementOperations =
          new CacheInvalidatingTableOperations(operations, identifier);
      wrapped = new BaseTable(replacementOperations, baseTable.name(), baseTable.reporter());
      replacementOperations.track(wrapped);
      return PreparedTable.cacheable(wrapped);
    }

    return PreparedTable.cacheable(table);
  }

  private Table requireCacheable(PreparedTable prepared) {
    if (!prepared.cacheable()) {
      throw new UncacheableTableException(prepared.table());
    }

    return prepared.table();
  }

  private void warnInvalidReplacement(
      TableIdentifier identifier, Table table, String reason, RuntimeException failure) {
    String tableTypeName = table.getClass().getName();
    if (invalidReplacementTableTypeNames.add(tableTypeName)) {
      if (failure != null) {
        LOG.warn(
            "Table {} of type {} cannot use CachingCatalog commit tracking because {}. "
                + "This instance will not be cached",
            identifier,
            tableTypeName,
            reason,
            failure);
      } else {
        LOG.warn(
            "Table {} of type {} cannot use CachingCatalog commit tracking because {}. "
                + "This instance will not be cached",
            identifier,
            tableTypeName,
            reason);
      }
    }
  }

  /**
   * A {@link TableOperations} wrapper that reconciles the cached table entry after a commit. It
   * preserves the committing table when it is still cached, but removes an entry that was loaded
   * concurrently during the write. This ensures that a subsequent {@link
   * #loadTable(TableIdentifier)} observes the commit without unnecessarily changing the table
   * identity used by clients such as Spark's relation cache. Reconciliation can wait for an
   * in-progress load of the same table, and retaining the committing entry refreshes its cache
   * expiration age. See <a href="https://github.com/apache/iceberg/issues/17338">#17338</a>.
   */
  private class CacheInvalidatingTableOperations implements TableOperations {
    private final TableOperations delegate;
    private final TableIdentifier identifier;
    private volatile Table trackedTable;

    private CacheInvalidatingTableOperations(TableOperations delegate, TableIdentifier identifier) {
      this.delegate = delegate;
      this.identifier = identifier;
    }

    private void track(Table table) {
      Preconditions.checkState(trackedTable == null, "Cannot replace the tracked table");
      this.trackedTable = Preconditions.checkNotNull(table, "Invalid table: null");
    }

    @Override
    public TableMetadata current() {
      return delegate.current();
    }

    @Override
    public TableMetadata refresh() {
      return delegate.refresh();
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      try {
        delegate.commit(base, metadata);
      } catch (CommitStateUnknownException e) {
        // The commit may have succeeded, so the cached table may be stale. Invalidate before
        // rethrowing so that a subsequent load re-reads the table metadata.
        try {
          invalidateLocalCache(identifier);
        } catch (RuntimeException invalidationFailure) {
          e.addSuppressed(invalidationFailure);
        }

        try {
          catalog.invalidateTable(identifier);
        } catch (RuntimeException invalidationFailure) {
          e.addSuppressed(invalidationFailure);
        }

        throw e;
      }

      try {
        reconcileLocalCacheAfterCommit(identifier, trackedTable);
      } catch (RuntimeException e) {
        // The commit is durable. Do not turn an invalidation failure into an apparent commit
        // failure, which could cause callers to clean up files that are now referenced by the
        // table.
        LOG.warn(
            "Failed to reconcile CachingCatalog entry {} after a successful commit; "
                + "the entry may remain stale until it is evicted or explicitly invalidated",
            identifier,
            e);
      }
    }

    @Override
    public FileIO io() {
      return delegate.io();
    }

    @Override
    public EncryptionManager encryption() {
      return delegate.encryption();
    }

    @Override
    public String metadataFileLocation(String fileName) {
      return delegate.metadataFileLocation(fileName);
    }

    @Override
    public LocationProvider locationProvider() {
      return delegate.locationProvider();
    }

    @Override
    public TableOperations temp(TableMetadata uncommittedMetadata) {
      // Temporary operations are not used to commit, so they do not need commit tracking.
      return delegate.temp(uncommittedMetadata);
    }

    @Override
    public long newSnapshotId() {
      return delegate.newSnapshotId();
    }

    @Override
    public boolean requireStrictCleanup() {
      return delegate.requireStrictCleanup();
    }
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    invalidateTable(ident);
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    catalog.renameTable(from, to);
    invalidateTable(from);
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    catalog.invalidateTable(ident);
    invalidateLocalCache(canonicalizeIdentifier(ident));
  }

  private void invalidateLocalCache(TableIdentifier canonicalized) {
    tableCache.invalidate(canonicalized);
    tableCache.invalidateAll(metadataTableIdentifiers(canonicalized));
  }

  private void reconcileLocalCacheAfterCommit(
      TableIdentifier canonicalized, Table committingTable) {
    // This same-key remapping is ordered with loads of the table. Keep the committing instance if
    // it is still cached; otherwise remove the table loaded while the commit was in flight. The
    // callback intentionally performs no other cache operation (see #3791).
    Table reconciled =
        tableCache
            .asMap()
            .compute(
                canonicalized,
                (ignored, cachedTable) -> cachedTable == committingTable ? cachedTable : null);

    if (reconciled != committingTable) {
      // Keep metadata invalidation outside the remapping callback to avoid recursive cache updates.
      // If compute removes an expired origin entry, Caffeine also dispatches its removal listener
      // after the underlying map remapping returns, so that listener's cascade is not nested here.
      tableCache.invalidateAll(metadataTableIdentifiers(canonicalized));
    }
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    Table table =
        prepareTable(
                canonicalizeIdentifier(identifier),
                catalog.registerTable(identifier, metadataFileLocation))
            .table();
    invalidateTable(identifier);
    return table;
  }

  @Override
  public Table registerTable(
      TableIdentifier identifier, String metadataFileLocation, boolean overwrite) {
    Table table =
        prepareTable(
                canonicalizeIdentifier(identifier),
                catalog.registerTable(identifier, metadataFileLocation, overwrite))
            .table();
    invalidateTable(identifier);
    return table;
  }

  private Iterable<TableIdentifier> metadataTableIdentifiers(TableIdentifier ident) {
    ImmutableList.Builder<TableIdentifier> builder = ImmutableList.builder();

    for (MetadataTableType type : METADATA_TABLE_TYPE_VALUES) {
      // metadata table resolution is case insensitive right now
      builder.add(TableIdentifier.parse(ident + "." + type.name()));
      builder.add(TableIdentifier.parse(ident + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    return builder.build();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new CachingTableBuilder(identifier, schema);
  }

  private class CachingTableBuilder implements TableBuilder {
    private final TableIdentifier ident;
    private final TableBuilder innerBuilder;

    private CachingTableBuilder(TableIdentifier identifier, Schema schema) {
      this.innerBuilder = catalog.buildTable(identifier, schema);
      this.ident = identifier;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec spec) {
      innerBuilder.withPartitionSpec(spec);
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder sortOrder) {
      innerBuilder.withSortOrder(sortOrder);
      return this;
    }

    @Override
    public TableBuilder withLocation(String location) {
      innerBuilder.withLocation(location);
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      innerBuilder.withProperties(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      innerBuilder.withProperty(key, value);
      return this;
    }

    @Override
    public Table create() {
      AtomicBoolean created = new AtomicBoolean(false);
      Table table = createThroughCache(canonicalizeIdentifier(ident), created);

      if (!created.get()) {
        throw new AlreadyExistsException("Table already exists: %s", ident);
      }

      return table;
    }

    private Table createThroughCache(TableIdentifier identifier, AtomicBoolean created) {
      try {
        return tableCache.get(
            identifier,
            ignored -> {
              created.set(true);
              return requireCacheable(prepareTable(identifier, innerBuilder.create()));
            });
      } catch (UncacheableTableException e) {
        return e.table();
      }
    }

    @Override
    public Transaction createTransaction() {
      // create a new transaction without altering the cache. the table doesn't exist until the
      // transaction is
      // committed. if the table is created before the transaction commits, any cached version is
      // correct and the
      // transaction create will fail. if the transaction commits before another create, then the
      // cache will be empty.
      return innerBuilder.createTransaction();
    }

    @Override
    public Transaction replaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the
      // transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is
      // present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.replaceTransaction(), () -> invalidateTable(ident));
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the
      // transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is
      // present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.createOrReplaceTransaction(), () -> invalidateTable(ident));
    }
  }

  private static class PreparedTable {
    private final Table table;
    private final boolean cacheable;

    private static PreparedTable cacheable(Table table) {
      return new PreparedTable(table, true);
    }

    private static PreparedTable uncacheable(Table table) {
      return new PreparedTable(table, false);
    }

    private PreparedTable(Table table, boolean cacheable) {
      this.table = Preconditions.checkNotNull(table, "Prepared table cannot be null");
      this.cacheable = cacheable;
    }

    private Table table() {
      return table;
    }

    private boolean cacheable() {
      return cacheable;
    }
  }

  private static class UncacheableTableException extends RuntimeException {
    private final Table table;

    private UncacheableTableException(Table table) {
      super(null, null, false, false);
      this.table = table;
    }

    private Table table() {
      return table;
    }
  }
}
