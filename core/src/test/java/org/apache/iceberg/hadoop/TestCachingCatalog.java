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
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestCachingCatalog extends HadoopTableTestBase {

  private static final Duration EXPIRATION_TTL = Duration.ofMinutes(5);
  private static final Duration HALF_OF_EXPIRATION = EXPIRATION_TTL.dividedBy(2);

  private FakeTicker ticker;

  @Before
  public void beforeEach() {
    this.ticker = new FakeTicker();
  }

  @After
  public void afterEach() {
    this.ticker = null;
  }

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

  @Test
  public void testTableExpiresAfterInterval() throws IOException {
    boolean expirationEnabled = true;
    CachingCatalog catalog = (CachingCatalog) CachingCatalog.wrap(hadoopCatalog(), expirationEnabled,
        EXPIRATION_TTL.toMillis(), ticker);
    Cache<TableIdentifier, Table> cache = catalog.cache();

    // Create the table and populate the catalog.
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Ensure table is in the cache.
    assertTableIsCached("Upon creation, the table should be cached", catalog, tableIdent);

    // Ensure that the table has its full duration remaining
    Assert.assertEquals("Table should not expire for the given Duration",
        Optional.of(EXPIRATION_TTL),
        catalog.getTimeToTTL(tableIdent)); // Get the time remaining until it would be evicted if

    // Alternate means of ensuring table is present.
    Assertions.assertThat(catalog.getIfPresentQuietly(tableIdent)).isNotNull();

    // Move time forward by half of the duration interval
    ticker.advance(HALF_OF_EXPIRATION);
    assertTableIsCached("Before the table's expiration interval has passed, it shold remain cached",
        catalog, tableIdent);

    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);

    // Move the ticker past the duration interval and then ensure that the catalog is serving a new object for tha
    // table (even if the table that is loaded is functionally equivalent).
    ticker.advance(HALF_OF_EXPIRATION.plus(Duration.ofSeconds(10)));
    Assert.assertFalse("Table should be expired as it hasn't been written to for the duration of the cache interval",
        catalog.getIfPresentQuietly(tableIdent).isPresent());
    Table tblAfterCacheMiss = catalog.loadTable(tableIdent);
    Assert.assertNotSame(
        "CachingCatalog should return a new instance after expiration",
        table,
        tblAfterCacheMiss);
  }

  @Test
  public void testCatalogExpirationTtlRefreshesAfterAccessViaCatalog() throws IOException {
    boolean expirationEnabled = true;
    CachingCatalog catalog = (CachingCatalog) CachingCatalog.wrap(hadoopCatalog(), expirationEnabled,
        EXPIRATION_TTL.toMillis(), ticker);
    Cache<TableIdentifier, Table> cache = catalog.cache();

    // Create the table and populate the catalog.
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Ensure table is in the cache.
    assertTableIsCached("On creation, the caching catalog should cache tables", catalog, tableIdent);

    // Check the age - Should be zero.
    Assert.assertEquals("The catalog's cached entry should have an age of zero on initialization",
        Optional.of(Duration.ZERO), catalog.cachedEntryAge(tableIdent));

    // Move time forward without accessing the table from the cache.
    ticker.advance(HALF_OF_EXPIRATION);
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);
    assertCatalogEntryTimeToTTL(catalog, tableIdent, HALF_OF_EXPIRATION);

    // Move time forward a bit more
    Duration oneMinute = Duration.ofMinutes(1L);
    ticker.advance(oneMinute);
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION.plus(oneMinute));
    assertCatalogEntryTimeToTTL(catalog, tableIdent, EXPIRATION_TTL.minus(HALF_OF_EXPIRATION).minus(oneMinute));

    // Access the table via the catalog, which should refresh the TTL
    Table table = catalog.loadTable(tableIdent);
    assertCatalogEntryHasAge(catalog, tableIdent, Duration.ZERO);
    assertCatalogEntryTimeToTTL(catalog, tableIdent, EXPIRATION_TTL);

    // Move forward, then access the table via the table object to ensure that does not advance TTL, only
    // access via the catalog does.
    ticker.advance(HALF_OF_EXPIRATION);
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);
    assertCatalogEntryTimeToTTL(catalog, tableIdent, HALF_OF_EXPIRATION);
    table.newAppend().appendFile(FILE_A).commit();  // This mutates the table object, but doesn't refresh the cache.
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);
    assertCatalogEntryTimeToTTL(catalog, tableIdent, HALF_OF_EXPIRATION);
  }

  @Test
  public void testCacheExpirationEagerlyRemovesMetadataTables() throws IOException {
    boolean expirationEnabled = true;
    CachingCatalog catalog = (CachingCatalog) CachingCatalog.wrap(hadoopCatalog(), expirationEnabled,
        EXPIRATION_TTL.toMillis(), ticker);
    Cache<TableIdentifier, Table> cache = catalog.cache();

    // Create the table and populate the catalog.
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));
    assertTableIsCached("Upon creation, the caching catalog should cache tables", catalog, tableIdent);

    // Write to the table.
    table.newAppend().appendFile(FILE_A).commit();
    assertTableIsCached("Caching catalog should not expire a table after update if its expiration has not passed",
        catalog, tableIdent);

    // Advance time by half of the expiration interval.
    // We'll then load the metadata tables and ensure that they are expired when the main table is expired,
    // even if their own age is not up to the expiration interval.
    //
    // We want to ensure that all cached metadata tables are expired together.
    ticker.advance(HALF_OF_EXPIRATION);
    assertTableIsCached("The caching catalog should not expire tables before their ttl",
        catalog, tableIdent);
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);

    // Load the metadata tables at time EXPIRATION_TTL / 2.
    medtadataTables(tableIdent).forEach(catalog::loadTable);
    assertMetadataTablesAreCached(cache, tableIdent);

    // Accessing the metadata tables will also refresh the main table.
    assertCatalogEntryTimeToTTL(catalog, tableIdent, EXPIRATION_TTL);
    // Sanity check - the cached metadata tables should have an age of ZERO.
    medtadataTables(tableIdent).forEach(metadataTbl -> assertCatalogEntryHasAge(catalog, metadataTbl, Duration.ZERO));

    // Move time forward now that metadata tables are loaded and then access the data table again, to reset its value.
    ticker.advance(HALF_OF_EXPIRATION);
    medtadataTables(tableIdent).forEach(catalog::loadTable);
    medtadataTables(tableIdent).forEach(metadataTbl ->
        assertCatalogEntryHasAge(catalog, metadataTbl, Duration.ZERO));

    // Sanity check. Because the table is still in the cache, its ttl isn't updated on metadata table load
    assertCatalogEntryHasAge(catalog, tableIdent, HALF_OF_EXPIRATION);

    // Move time forward the other half of the expiration interval
    ticker.advance(HALF_OF_EXPIRATION);
    assertCatalogHasExpiredTable(catalog, tableIdent);
    assertCatalogHasExpiredMetadataTables(catalog, tableIdent);
  }

  private void assertTableIsCached(String assertionMessage, CachingCatalog catalog, TableIdentifier identifier) {
    catalog.cache().cleanUp();
    Assert.assertTrue(assertionMessage, catalog.getIfPresentQuietly(identifier).isPresent());
  }

  private List<TableIdentifier> medtadataTables(TableIdentifier tableIdent) {
    return Arrays.stream(MetadataTableType.values())
        .map(type -> TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList());
  }

  private void assertMetadataTablesAreCached(Cache<TableIdentifier, Table> cache, TableIdentifier tableIdentifier) {
    medtadataTables(tableIdentifier)
        .forEach(metadataTable ->
          Assert.assertNotNull(String.format("Metadata table %s should be in the cache", metadataTable.name()),
              cache.policy().getIfPresentQuietly(metadataTable)));
  }

  private void assertCatalogHasExpiredTable(CachingCatalog catalog, TableIdentifier tableIdent) {
    Assert.assertFalse("The catalog should not serve table's that are past their TTL",
        catalog.getIfPresentQuietly(tableIdent).isPresent());
  }

  private void assertCatalogHasExpiredMetadataTables(CachingCatalog catalog, TableIdentifier tableIdent) {
    // Sanity check tha the table itself is expired
    Assert.assertFalse("The table should not be served by the CachingCatalog's cache",
        catalog.getIfPresentQuietly(tableIdent).isPresent());
    medtadataTables(tableIdent)
        .forEach(metadataTable ->
            Assert.assertFalse("The CachingCatalog should not return metadata tables for a TTL'd table",
                catalog.getIfPresentQuietly(metadataTable).isPresent()));
  }

  private void assertCatalogEntryHasAge(CachingCatalog catalog, TableIdentifier identifier, Duration expectedAge) {
    Assert.assertEquals("The table entry should have the expected age in the catalog cache",
        Optional.of(expectedAge), catalog.cachedEntryAge(identifier));
  }

  private void assertCatalogEntryTimeToTTL(CachingCatalog catalog, TableIdentifier ident, Duration expectedTimeLeft) {
    Assert.assertEquals("The catalog should TTL the given table after the specified duration",
        Optional.of(expectedTimeLeft), catalog.getTimeToTTL(ident));
  }
}
