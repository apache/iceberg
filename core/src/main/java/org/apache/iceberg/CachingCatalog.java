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
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CachingCatalog implements Catalog {

  private static final Logger LOG = LoggerFactory.getLogger(CachingCatalog.class);

  // TODO - Make a Builder.
  public static CachingCatalog.Builder wrap(Catalog catalog) {
    return new Builder(catalog);
  }

  // TODO - Probably just extend this class as ExpiringCachingCatalog.
  public static Catalog wrap(Catalog catalog, boolean expirationEnabled, long expirationIntervalMilllis) {
    return wrap(catalog, true, expirationEnabled, expirationIntervalMilllis);
  }

  public static Catalog wrap(Catalog catalog, boolean caseSensitive, boolean expirationEnabled,
      long expirationIntervalMillis) {
    return new CachingCatalog(catalog, caseSensitive, expirationEnabled, expirationIntervalMillis);
  }

  // TODO - Add these as config values and then try to have a background thread refreshing for the streaming case.
  //        If Java9+, use System in built ones. See the Caffeine wiki:
  //        https://github.com/ben-manes/caffeine/wiki/Removal
  // private final Cache<TableIdentifier, Table> tableCache = createTableCache(expirationEnabled,
  //     expirationIntervalMillis);
  // private final Cache<TableIdentifier, Table> tableCache = Caffeine
  //     .newBuilder()
  //     .softValues()
  //     // TODO - Implement removalListener (check if there's another Listener... I think so)
  //     //        to remove metadata tables as well on expiration / refresh.
  //     //        Possibly reimplement the cache so that metadata tables are loaded and removed atomically so that
  //     //        we can depend on cache expiration provided by Caffine?
  //     // .removalListener((RemovalListener<TableIdentifier, Table>) (key, value, cause) -> {
  //     //   if (MetadataTableUtils.hasMetadataTableName()) {
  //     //
  //     //   }
  //     // })
  //     .build();

  private final Catalog catalog;
  private final boolean caseSensitive;
  private final boolean expirationEnabled;
  private final long expirationIntervalMillis;
  private final boolean recordCacheStats;
  private Cache<TableIdentifier, Table> tableCache;

  private CachingCatalog(Catalog catalog, boolean caseSensitive, boolean expirationEnabled,
      long expirationIntervalMillis) {
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
    this.expirationEnabled = expirationEnabled;
    this.expirationIntervalMillis = expirationIntervalMillis;
    this.recordCacheStats = false;

    this.tableCache = createTableCache(expirationEnabled, expirationIntervalMillis);
  }

  // The tableCache should only be passed in during tests, so that a ticket can be used
  // to control for cache entry expiration.
  @VisibleForTesting
  public CachingCatalog(Catalog catalog, boolean caseSensitive, boolean expirationEnabled,
      long expirationIntervalMillis, Cache<TableIdentifier, Table> tableCache, boolean recordCacheStats) {
    this.catalog = catalog;
    this.caseSensitive = caseSensitive;
    this.expirationEnabled = expirationEnabled;
    this.expirationIntervalMillis = expirationIntervalMillis;
    this.tableCache = tableCache;
    this.recordCacheStats = recordCacheStats;
  }

  private Cache<TableIdentifier, Table> createTableCache(boolean expirationEnabled,
      long expirationIntervalMillis) {
    return createTableCache(expirationEnabled, expirationIntervalMillis, null, false);
  }

  @VisibleForTesting
  public static Cache<TableIdentifier, Table> createTableCache(boolean expirationEnabled, long expirationMillis,
      Ticker ticker, boolean recordTableStats) {
    // TODO - move this somewhere better - this is just for getting tests to work.
    Preconditions.checkArgument(!expirationEnabled || expirationMillis > 0L,
        "The cache expiration time must be greater than zero if cache expiration is enabled");
    Caffeine<Object, Object> cacheBuilder = Caffeine
      .newBuilder()
      .softValues();

    if (expirationEnabled) {
      // TODO - Update this to be a write expiration so that it matches current semantics.
      LOG.info("Instantiating CachingCatalog with a cache expiration interval of {} milliseconds", expirationMillis);
      cacheBuilder = cacheBuilder.expireAfterAccess(Duration.ofMillis(expirationMillis));
    }

    if (recordTableStats) {
      LOG.info("Instantiating CachingCatalog an internal cache that records statistics on hits and misses");
      cacheBuilder = cacheBuilder.recordStats();
    }

    if (ticker != null) {
      LOG.info(
        "Received a non-null Ticker when instantiating the CachingCatalog's tableCache. This should only happen " +
            "during tests. If you see this log outside of tests, there is potentially a bug.");

      cacheBuilder = cacheBuilder.ticker(ticker);
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

  private Iterable<TableIdentifier> metadataTableIdentifiers(TableIdentifier ident) {
    ImmutableList.Builder<TableIdentifier> builder = ImmutableList.builder();

    for (MetadataTableType type : MetadataTableType.values()) {
      // metadata table resolution is case insensitive right now
      builder.add(TableIdentifier.parse(ident + "." + type.name()));
      builder.add(TableIdentifier.parse(ident + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    return builder.build();
  }

  public static class Builder {
    private final Catalog catalog;
    private boolean caseSensitive;
    // TODO - Use CatalogProperties.
    private boolean expirationEnabled = false;
    // TODO - Change name from INTERVAL_MILLIS to just MILLIS - INTERVAL_MILLIS implies that it happens on a schedule.
    private long expirationMillis;

    private Builder(Catalog catalog) {
      this.catalog = catalog;
    }

    public Builder caseSensitive(boolean caseSensitive) {
      this.caseSensitive = caseSensitive;
      return this;
    }

    public Builder expirationEnabled(boolean expirationEnabled) {
      this.expirationEnabled = expirationEnabled;
      return this;
    }

    public Builder withExpirationMillis(long expirationMillis) {
      this.expirationMillis = expirationMillis;
      return this;
    }

    public CachingCatalog build() {
      return new CachingCatalog(catalog, caseSensitive, expirationEnabled, expirationMillis);
    }
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
