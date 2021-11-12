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
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingCatalog implements Catalog {

  private static final Logger LOG = LoggerFactory.getLogger(CachingCatalog.class);
  private static final RemovalListener<TableIdentifier, Table> keyLoggingRemovalListener =
      (key, value, cause) -> LOG.info("Expired {} from the TableCache", key);

  public static Catalog wrap(Catalog catalog) {
    return wrap(catalog, false, 0);
  }

  public static Catalog wrap(Catalog catalog, boolean expirationEnabled, long expirationIntervalMilllis) {
    return wrap(catalog, true, expirationEnabled, expirationIntervalMilllis);
  }

  public static Catalog wrap(Catalog catalog, boolean caseSensitive, boolean expirationEnabled,
      long expirationIntervalMillis) {
    return new CachingCatalog(catalog, caseSensitive, expirationEnabled, expirationIntervalMillis);
  }

  public static Catalog wrap(Catalog catalog, boolean expirationEnabled, long expirationIntervalMillis, Ticker ticker) {
    return new CachingCatalog(catalog, true, expirationEnabled, expirationIntervalMillis, ticker);
  }

  private final Catalog catalog;
  private final boolean caseSensitive;
  private final boolean expirationEnabled;
  private final long expirationIntervalMillis;
  private Cache<TableIdentifier, Table> tableCache;

  private CachingCatalog(Catalog catalog, boolean caseSensitive, boolean isExpirationEnabled,
      long expirationIntervalInMillis) {
    this(catalog, caseSensitive, isExpirationEnabled, expirationIntervalInMillis, Ticker.systemTicker());
  }

  private CachingCatalog(Catalog catalog, boolean caseSensitive, boolean isExpirationEnabled,
      long expirationIntervalMillis, Ticker ticker) {
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
    this.expirationEnabled = isExpirationEnabled;
    this.expirationIntervalMillis = expirationIntervalMillis;

    this.tableCache = createTableCache(ticker);
  }

  /**
   * Return the age of an entry in the cache.
   * <p>
   * This method is only visiable for testing the cache expiration policy, as cache invalidation is handled
   * by the catalog and not the cache itself.
   * <p>
   * Returns the age of the cache entry corresponding to the identifier,  or {@code Optional.empty} if the table
   * is not present in the cache or if no expireAfterAccess policy is present in this CachingCatalog.
   */
  // VisibleForTesting
  public Optional<Duration> cachedEntryAge(TableIdentifier identifier) {
    // TODO - This is wrong. Should use `ageOf`.
    return tableCache.policy()
        .expireAfterAccess()
        .flatMap(tableExpiration -> tableExpiration.ageOf(identifier));
  }

  // Returns the cached Table entry corresponding to the given identifier iff
  // it's in the cache. Grabs the table in a way that doesn't count as an access
  // and thus won't affect the cache's ttl's (if enabled).
  // VisibleForTesting
  public Optional<Table> tableFromCacheQuietly(TableIdentifier identifier) {
    // Ensure async cleanup actions have happened.
    tableCache.cleanUp();
    return Optional.ofNullable(tableCache.policy().getIfPresentQuietly(identifier));
  }

  // TODO - Make this package private and add a utility helper method to access it for tests.
  // VisibleForTesting
  public Cache<TableIdentifier, Table> cache() {
    return tableCache;
  }

  // Visiblefortesting
  public Optional<Duration> getTimeToTTL(TableIdentifier identifier) {
    return tableCache
        .policy()
        .expireAfterAccess()  // Currently assumed expireAfterAAccess, which is what we set at cache level.
        .flatMap(tableExpiration -> tableExpiration.ageOf(identifier)) // Get the time table has been cached.
        .map(age -> Duration.ofMillis(expirationIntervalMillis).minus(age));
  }

  private Cache<TableIdentifier, Table> createTableCache(Ticker ticker) {
    Caffeine<TableIdentifier, Table> cacheBuilder = Caffeine
        .newBuilder()
        .softValues()
        .removalListener(keyLoggingRemovalListener)
        .writer(new CacheWriter<TableIdentifier, Table>() {
          @Override
          // TODO - Consider expiring and syncing any metadata tables that have a different snapshotId
          //        upon write.
          public void write(TableIdentifier tableIdentifier, Table table) {
            LOG.info("Table {} was written to the catalog at snapshot id {}", tableIdentifier,
                table.currentSnapshot() == null ? null : table.currentSnapshot().snapshotId());
          }

          @Override
          // On expiration, remove any associated metadata tables. If a metadata table is expired,
          public void delete(TableIdentifier tableIdentifier, Table table, RemovalCause cause) {
            // On expiration, remove any associated metadata tables so that subsequent catalog loads won't
            // return stale metadata tables w.r.t. the underlying data tables they would return.
            //
            // TODO - Should we put metadata tables abck into the catalog if their associated table is still
            //        cached to keep tables and metadata tables on the same snapshot?
            if (expirationEnabled && !MetadataTableUtils.hasMetadataTableName(tableIdentifier)) {
              onTableExpiration(tableIdentifier);
            }
          }
        });

    if (expirationEnabled) {
      // Expire after write isn't going to work as we need to invalidate after writes.
      return cacheBuilder.expireAfterAccess(Duration.ofMillis(expirationIntervalMillis)).ticker(ticker).build();
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
      // TODO - If this is a metadata table consider, checking that this is in sync
      //        with the version that is presently cached. Could be confusing to user's though.
      return cached;
    }

    if (MetadataTableUtils.hasMetadataTableName(canonicalized)) {
      TableIdentifier originTableIdentifier = TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, catalog::loadTable);

      // share TableOperations instance of origin table for all metadata tables, so that metadata table instances are
      // also refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());

        Table metadataTable = MetadataTableUtils.createMetadataTableInstance(
            ops, catalog.name(), originTableIdentifier,
            canonicalized, type);
        tableCache.put(canonicalized, metadataTable);
        return metadataTable;
      }
    }

    return tableCache.get(canonicalized, catalog::loadTable);
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    invalidate(canonicalizeIdentifier(ident));
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    catalog.renameTable(from, to);
    invalidate(canonicalizeIdentifier(from));
  }

  private void invalidate(TableIdentifier ident) {
    tableCache.invalidate(ident);
    tableCache.invalidateAll(metadataTableIdentifiers(ident));
  }

  private void onTableExpiration(TableIdentifier ident) {
    // Don't need to canonicalize as we're using the form that was written.
    // Only invalidate metadata tables to avoid infinite recursion from cache invalidate
    // to the CacheWriter callback on table expiration.
    if (!MetadataTableUtils.hasMetadataTableName(ident)) {
      tableCache.invalidateAll(metadataTableIdentifiers(ident));
    }
  }

  private Iterable<TableIdentifier> metadataTableIdentifiers(TableIdentifier ident) {
    ImmutableList.Builder<TableIdentifier> builder = ImmutableList.builder();

    for (MetadataTableType type : MetadataTableType.values()) {
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
      Table table = tableCache.get(canonicalizeIdentifier(ident), identifier -> {
        created.set(true);
        return innerBuilder.create();
      });

      if (!created.get()) {
        throw new AlreadyExistsException("Table already exists: %s", ident);
      }

      return table;
    }

    @Override
    public Transaction createTransaction() {
      // create a new transaction without altering the cache. the table doesn't exist until the transaction is
      // committed. if the table is created before the transaction commits, any cached version is correct and the
      // transaction create will fail. if the transaction commits before another create, then the cache will be empty.
      return innerBuilder.createTransaction();
    }

    @Override
    public Transaction replaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.replaceTransaction(),
          () -> invalidate(canonicalizeIdentifier(ident)));
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.createOrReplaceTransaction(),
          () -> invalidate(canonicalizeIdentifier(ident)));
    }
  }
}
