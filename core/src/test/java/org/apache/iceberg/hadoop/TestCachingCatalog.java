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

package org.apache.iceberg.hadoop;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.FakeTicker;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestCachingCatalog extends HadoopTableTestBase {

  @Test
  public void testInvalidateMetadataTablesIfBaseTableIsModified() throws Exception {
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog());
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    table.newAppend().appendFile(FILE_A).commit();

    Snapshot oldSnapshot = table.currentSnapshot();

    TableIdentifier filesMetaTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "files");
    Table filesMetaTable = catalog.loadTable(filesMetaTableIdent);

    TableIdentifier manifestsMetaTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "manifests");
    Table manifestsMetaTable = catalog.loadTable(manifestsMetaTableIdent);

    table.newAppend().appendFile(FILE_B).commit();

    Table filesMetaTable2 = catalog.loadTable(filesMetaTableIdent);
    Table manifestsMetaTable2 = catalog.loadTable(manifestsMetaTableIdent);

    // metadata tables are cached
    Assert.assertEquals(filesMetaTable2, filesMetaTable);
    Assert.assertEquals(manifestsMetaTable2, manifestsMetaTable);

    // the current snapshot of origin table is updated after committing
    Assert.assertNotEquals(table.currentSnapshot(), oldSnapshot);

    // underlying table operation in metadata tables are shared with the origin table
    Assert.assertEquals(filesMetaTable2.currentSnapshot(), table.currentSnapshot());
    Assert.assertEquals(manifestsMetaTable2.currentSnapshot(), table.currentSnapshot());
  }

  @Test
  public void testInvalidateMetadataTablesIfBaseTableIsDropped() throws IOException {
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog());

    // create the original table
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    table.newAppend().appendFile(FILE_A).commit();

    // remember the original snapshot
    Snapshot oldSnapshot = table.currentSnapshot();

    // populate the cache with metadata tables
    for (MetadataTableType type : MetadataTableType.values()) {
      catalog.loadTable(TableIdentifier.parse(tableIdent + "." + type.name()));
      catalog.loadTable(TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    // drop the original table
    catalog.dropTable(tableIdent);

    // create a new table with the same name
    table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    table.newAppend().appendFile(FILE_B).commit();

    // remember the new snapshot
    Snapshot newSnapshot = table.currentSnapshot();

    Assert.assertNotEquals("Snapshots must be different", oldSnapshot, newSnapshot);

    // validate metadata tables were correctly invalidated
    for (MetadataTableType type : MetadataTableType.values()) {
      TableIdentifier metadataIdent1 = TableIdentifier.parse(tableIdent + "." + type.name());
      Table metadataTable1 = catalog.loadTable(metadataIdent1);
      Assert.assertEquals("Snapshot must be new", newSnapshot, metadataTable1.currentSnapshot());

      TableIdentifier metadataIdent2 = TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT));
      Table metadataTable2 = catalog.loadTable(metadataIdent2);
      Assert.assertEquals("Snapshot must be new", newSnapshot, metadataTable2.currentSnapshot());
    }
  }

  @Test
  public void testTableName() throws Exception {
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog());
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    Table table = catalog.loadTable(tableIdent);
    Assert.assertEquals("Name must match", "hadoop.db.ns1.ns2.tbl", table.name());

    TableIdentifier snapshotsTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    Assert.assertEquals("Name must match", "hadoop.db.ns1.ns2.tbl.snapshots", snapshotsTable.name());
  }

  // TODO - Currently the "new" behavior is to restart the cache timer any time the table is touched at all.
  //        This won't help users who continue to try to read a table somebody else wrote to.
  //        Should we consider expiring on write instead or using a more fine-grained `Expiry`
  @Test
  public void testTableExpirationAfterNotAccessed() throws IOException {
    boolean caseSensitive = true;
    boolean expirationEnabled = true;
    long expirationMinutes = Duration.ofMinutes(5).toMillis();

    // Create CachingCatalog with a controllable ticker for testing cache expiry.
    FakeTicker ticker = new FakeTicker();
    Cache<TableIdentifier, Table> tableCache = CachingCatalog.createTableCache(
        expirationEnabled, expirationMinutes, ticker, true /* recordCacheStats */);
    Catalog catalog = new CachingCatalog(hadoopCatalog(), caseSensitive, expirationEnabled,
        expirationMinutes, tableCache);

    // Create the table and populate the catalog.
    String tblName = "tbl";
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, tblName);
    Table tblAtCreate = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Ensure that the table is now in the cache
    Assert.assertEquals("Table should remain in the cache after insertion before the expiration period",
        1, tableCache.asMap().size());
    checkStats(tableCache, 0L /* cache hits */, 1L /* cache misses */, 1 /* total cache load attempts */);

    // Check that the table is still cached if the clock hasn't passed the expiration time since last access.
    Duration fiveSecondsToTableTTL = Duration.ofMinutes(expirationMinutes).minusSeconds(5);
    ticker.advance(fiveSecondsToTableTTL.toNanos());
    Assert.assertEquals(
        "Table should remain in the cache after insertion before the expiration period if it has not been refreshed",
        1, tableCache.asMap().size());
    // Stats should not have changed as we haven't interacted with the table cache
    checkStats(tableCache, 0L /* cache hits */, 1L /* cache misses */, 1 /* total cache load attempts */);

    // Check that th table is still cached just until the expiration time (as we haven't accessed it in a way that
    // would invalidate and re-cache.
    ticker.advance(Duration.ofSeconds(4).toNanos());
    Assert.assertEquals("CachingCatalog should keep tables in the cache until the expiration if not accessed",
        1, tableCache.asMap().size());

    // Now ensure that the table is no longer in the cache
    ticker.advance(1, TimeUnit.MILLISECONDS);
    Assert.assertNull(
        "Cache should expire tables at the expiration interval if they're not accessed",
        tableCache.getIfPresent(tableIdent));

    // Force clean-up. Cache won't return stale results per our spec but might still have data in it depending on impl.
    tableCache.cleanUp();
    Assert.assertEquals("Cache should be empty after all entries have expired", 0, tableCache.asMap().size());

    // Get the table again, which should put a new entry representing the same table identifier back in the cache.
    // Ensure that the returned Table's reference the same thing, but are not the same object.
    Table tblAfterCacheMiss = catalog.loadTable(tableIdent);
    Assert.assertNotSame(
        "CachingCatalog should return a new instance after expiration",
        tblAtCreate,
        tblAfterCacheMiss);
    Assert.assertEquals("CachingCatalog should return functionally equivalent tables on load after expiration",
        tblAtCreate.name(), tblAfterCacheMiss.name());
    Assertions.assertThat(tblAfterCacheMiss).isNotEqualTo(tblAtCreate);

    // Ensure that the cache is now re-populated from the table load
    Assert.assertEquals("Reloading the expired table should repopulate its cache entry",
        1, tableCache.asMap().size());
  }

  @Test
  public void testCacheExpirationRemovesMetadataTables() throws IOException {
    boolean caseSensitive = true;
    boolean expirationEnabled = true;
    boolean recordCacheStats = true;
    long expirationMinutes = Duration.ofMinutes(5).toMillis();

    // Create CachingCatalog with a controllable ticker for testing cache expiry.
    FakeTicker ticker = new FakeTicker();
    long nanosAtTickerCreation = ticker.read();
    Cache<TableIdentifier, Table> tableCache = CachingCatalog.createTableCache(
        expirationEnabled, expirationMinutes, ticker, recordCacheStats);
    Catalog catalog = new CachingCatalog(hadoopCatalog(), caseSensitive, expirationEnabled,
        expirationMinutes, tableCache);

    // Create the table and populate the catalog.
    String tblName = "tbl";
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    table.newAppend().appendFile(FILE_A).commit();

    // Ensure that the table is now in the cache
    Assert.assertEquals("Table should remain in the cache after insertion before the expiration period",
        1, tableCache.asMap().size());
    checkStats(tableCache, 0L /* cache hits */, 1L /* cache misses */, 1 /* total cache load attempts */);

    // remember the original snapshot
    Snapshot oldSnapshot = table.currentSnapshot();

    // populate the cache with metadata tables
    int metadataTablesLoaded = 0;
    for (MetadataTableType type : MetadataTableType.values()) {
      metadataTablesLoaded += 1;
      catalog.loadTable(TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    // Ensure that the metadata tables are now in the cache as well.
    int expectedCacheTableCount = 1 + MetadataTableType.values().length;
    Assert.assertEquals("Table should remain in the cache after insertion before the expiration period",
        expectedCacheTableCount, tableCache.asMap().size());

    // All the metadata tables should result in a cache hit as we pre-populate the metadata tables
    //   when we load a table that has them and the original table identifier iss not already in the cache.
    //
    // We have called loadTable explicitly one time for each metadata table, but since we pre-populate the metadata
    //   tables, the stats track that as only one load operation.
    //
    // However, given that we have called loadTable 1 time for the table itself + 1 time for each metadata table,
    //   the number of cache requests should reflect 1 for each attempt.
    checkStats(tableCache, metadataTablesLoaded /* cache hits */, expectedCacheTableCount /* cache misses */,
        expectedCacheTableCount - metadataTablesLoaded /* total cache load attempts */);

    tableCache.cleanUp();

    Assert.assertEquals("Before the ticker is advanced, the cache should not drop tables during maintenance",
        expectedCacheTableCount, tableCache.asMap().size());

    Assert.assertEquals("The ticker should have the same time until its advanced",
        nanosAtTickerCreation, ticker.read());

    // Advance the ticker such that the tables are dropped.
    ticker.advance(Duration.ofMinutes(expirationMinutes + 1).toNanos());

    Assert.assertEquals(
        "After advancing the ticker, it should reflect the changed time",
        nanosAtTickerCreation + Duration.ofMinutes(expirationMinutes + 1).toNanos(),
        ticker.read());

    // Ensure that the table's are no longer being returned by the cache now that the expiration period has
    // occurred.
    Assert.assertNull(
        "The cache should not return tables that have been expired",
        tableCache.getIfPresent(tableIdent));
    for (MetadataTableType type : MetadataTableType.values()) {
      Assert.assertNull(
          "Metadatatables should expire when the primary table has expired",
          tableCache.getIfPresent(TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT))));
    }

    tableCache.cleanUp();
    Assert.assertEquals("All of the metadata tables should be dropped after the table is dropped from the cache",
        0, tableCache.asMap().size());

    // drop the original table
    catalog.dropTable(tableIdent);

    // create a new table with the same name
    table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key3", "value3"));

    table.newAppend().appendFile(FILE_B).commit();

    // remember the new snapshot
    Snapshot newSnapshot = table.currentSnapshot();

    Assert.assertNotEquals("Snapshots must be different", oldSnapshot, newSnapshot);

    // validate metadata tables were correctly invalidated
    for (MetadataTableType type : MetadataTableType.values()) {
      TableIdentifier metadataIdent = TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT));
      Table metadataTable = catalog.loadTable(metadataIdent);
      Assert.assertEquals("Snapshot must be new", newSnapshot, metadataTable.currentSnapshot());
    }
  }

  // TODO - Because of the way things are implemented, asserting on stats might not be the best idea.
  private void checkStats(Cache<?, ?> cache, long hitCount, long missCount, long loadCount) {
    CacheStats stats = cache.stats();
    Assert.assertEquals("CachingCatalog should have the correct number of hits",
        hitCount, stats.hitCount());
    Assert.assertEquals("CachingCatalog should have the correct number of misses",
        missCount, stats.missCount());
    Assert.assertEquals("CachingCatalog should accurately reflect the number of times we've tried to load",
        loadCount, stats.loadCount());
  }
}
