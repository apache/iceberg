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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.benmanes.caffeine.cache.Cache;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SupportsOperationsReplacement;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestableCachingCatalog;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.FakeTicker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestCachingCatalog extends HadoopTableTestBase {

  private static final Duration EXPIRATION_TTL = Duration.ofMinutes(5);
  private static final Duration HALF_OF_EXPIRATION = EXPIRATION_TTL.dividedBy(2);

  private FakeTicker ticker;

  @BeforeEach
  public void beforeEach() {
    this.ticker = new FakeTicker();
  }

  @AfterEach
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

    TableIdentifier manifestsMetaTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "tbl", "manifests");
    Table manifestsMetaTable = catalog.loadTable(manifestsMetaTableIdent);

    table.newAppend().appendFile(FILE_B).commit();

    Table filesMetaTable2 = catalog.loadTable(filesMetaTableIdent);
    Table manifestsMetaTable2 = catalog.loadTable(manifestsMetaTableIdent);

    // committing through the origin table invalidates its metadata tables, so they are reloaded
    assertThat(filesMetaTable2).isNotEqualTo(filesMetaTable);
    assertThat(manifestsMetaTable2).isNotEqualTo(manifestsMetaTable);

    // the current snapshot of origin table is updated after committing
    assertThat(table.currentSnapshot()).isNotEqualTo(oldSnapshot);

    // reloaded metadata tables observe the origin table's new snapshot
    assertThat(filesMetaTable2.currentSnapshot()).isEqualTo(table.currentSnapshot());
    assertThat(manifestsMetaTable2.currentSnapshot()).isEqualTo(table.currentSnapshot());
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
      catalog.loadTable(
          TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    // drop the original table
    catalog.dropTable(tableIdent);

    // create a new table with the same name
    table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    table.newAppend().appendFile(FILE_B).commit();

    // remember the new snapshot
    Snapshot newSnapshot = table.currentSnapshot();

    assertThat(newSnapshot).as("Snapshots must be different").isNotEqualTo(oldSnapshot);

    // validate metadata tables were correctly invalidated
    for (MetadataTableType type : MetadataTableType.values()) {
      TableIdentifier metadataIdent1 = TableIdentifier.parse(tableIdent + "." + type.name());
      Table metadataTable1 = catalog.loadTable(metadataIdent1);
      assertThat(metadataTable1.currentSnapshot())
          .as("Snapshot must be new")
          .isEqualTo(newSnapshot);

      TableIdentifier metadataIdent2 =
          TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT));
      Table metadataTable2 = catalog.loadTable(metadataIdent2);
      assertThat(metadataTable2.currentSnapshot())
          .as("Snapshot must be new")
          .isEqualTo(newSnapshot);
    }
  }

  @Test
  public void testTableName() throws Exception {
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog());
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    Table table = catalog.loadTable(tableIdent);
    assertThat(table.name()).as("Name must match").isEqualTo("hadoop.db.ns1.ns2.tbl");

    TableIdentifier snapshotsTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    assertThat(snapshotsTable.name())
        .as("Name must match")
        .isEqualTo("hadoop.db.ns1.ns2.tbl.snapshots");
  }

  @Test
  public void testNonExistingTable() throws Exception {
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog());

    TableIdentifier tableIdent = TableIdentifier.of("otherDB", "otherTbl");

    assertThatThrownBy(() -> catalog.loadTable(tableIdent))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: otherDB.otherTbl");
  }

  @Test
  public void testTableWithMetadataTableName() throws Exception {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "partitions");
    TableIdentifier metaTableIdent =
        TableIdentifier.of("db", "ns1", "ns2", "partitions", "partitions");

    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));
    catalog.cache().invalidateAll();

    Table table = catalog.loadTable(tableIdent);
    assertThat(table.name()).isEqualTo("hadoop.db.ns1.ns2.partitions");
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.cache().asMap()).doesNotContainKey(metaTableIdent);

    catalog.cache().invalidateAll();
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);

    Table metaTable = catalog.loadTable(metaTableIdent);
    assertThat(metaTable).isInstanceOf(BaseMetadataTable.class);
    assertThat(metaTable.name()).isEqualTo("hadoop.db.ns1.ns2.partitions.partitions");
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.cache().asMap()).containsKey(metaTableIdent);
  }

  @Test
  public void testTableExpiresAfterInterval() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);

    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Ensure table is cached with full ttl remaining upon creation
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.remainingAgeFor(tableIdent)).isPresent().get().isEqualTo(EXPIRATION_TTL);

    ticker.advance(HALF_OF_EXPIRATION);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(HALF_OF_EXPIRATION);

    ticker.advance(HALF_OF_EXPIRATION.plus(Duration.ofSeconds(10)));
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);
    assertThat(catalog.loadTable(tableIdent))
        .as("CachingCatalog should return a new instance after expiration")
        .isNotSameAs(table);
  }

  @Test
  public void testCatalogExpirationTtlRefreshesAfterAccessViaCatalog() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");

    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(Duration.ZERO);

    ticker.advance(HALF_OF_EXPIRATION);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).isPresent().get().isEqualTo(HALF_OF_EXPIRATION);
    assertThat(catalog.remainingAgeFor(tableIdent)).isPresent().get().isEqualTo(HALF_OF_EXPIRATION);

    Duration oneMinute = Duration.ofMinutes(1L);
    ticker.advance(oneMinute);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent))
        .isPresent()
        .get()
        .isEqualTo(HALF_OF_EXPIRATION.plus(oneMinute));
    assertThat(catalog.remainingAgeFor(tableIdent))
        .get()
        .isEqualTo(HALF_OF_EXPIRATION.minus(oneMinute));

    // Access the table via the catalog, which should refresh the TTL
    Table table = catalog.loadTable(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(Duration.ZERO);
    assertThat(catalog.remainingAgeFor(tableIdent)).get().isEqualTo(EXPIRATION_TTL);

    ticker.advance(HALF_OF_EXPIRATION);
    assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);
    assertThat(catalog.remainingAgeFor(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);

    // Check that accessing the table object directly does not affect the cache TTL
    table.refresh();
    assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);
    assertThat(catalog.remainingAgeFor(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);

    // Committing through a table obtained from the catalog invalidates its cache entry so that a
    // subsequent load observes the commit (https://github.com/apache/iceberg/issues/10493).
    table.newAppend().appendFile(FILE_A).commit();
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);
  }

  @Test
  public void testLoadTableObservesCommitAfterConcurrentReloadDuringWrite() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Start from a clean cache so the writer is loaded fresh from the underlying catalog.
    catalog.cache().invalidateAll();

    // A writer loads the table and establishes an initial snapshot.
    Table writer = catalog.loadTable(tableIdent);
    writer.newAppend().appendFile(FILE_A).commit();
    Snapshot committedBeforeWrite = writer.currentSnapshot();
    assertThat(committedBeforeWrite).isNotNull();

    // The writer begins a long-running write that will outlive the cache TTL.
    AppendFiles pendingWrite = writer.newAppend().appendFile(FILE_B);

    // The cache entry expires while the write is still in progress.
    ticker.advance(EXPIRATION_TTL.plus(Duration.ofSeconds(1)));
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);

    // A concurrent loader (e.g. an OpenLineage listener thread) reloads the table before the
    // write commits, re-caching the pre-commit table with a fresh TTL.
    Table concurrentlyReloaded = catalog.loadTable(tableIdent);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(concurrentlyReloaded.currentSnapshot()).isEqualTo(committedBeforeWrite);

    // The writer commits the in-flight write through its own table reference.
    pendingWrite.commit();
    Snapshot committedAfterWrite = writer.currentSnapshot();
    assertThat(committedAfterWrite).isNotEqualTo(committedBeforeWrite);

    // A subsequent load must observe the committed snapshot, not the stale table that was
    // re-cached during the write (https://github.com/apache/iceberg/issues/10493).
    assertThat(catalog.loadTable(tableIdent).currentSnapshot())
        .as("loadTable after commit must not return a snapshot re-cached during the write")
        .isEqualTo(committedAfterWrite);
  }

  @Test
  public void testCreatedTableObservesCommitAfterConcurrentReloadDuringWrite() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");

    // The writer keeps the table reference returned by createTable.
    Table writer = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));
    writer.newAppend().appendFile(FILE_A).commit();
    Snapshot committedBeforeWrite = writer.currentSnapshot();
    assertThat(committedBeforeWrite).isNotNull();

    // The writer begins a long-running write that will outlive the cache TTL.
    AppendFiles pendingWrite = writer.newAppend().appendFile(FILE_B);

    // The cache entry is not present while the write is in progress.
    ticker.advance(EXPIRATION_TTL.plus(Duration.ofSeconds(1)));
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);

    // A concurrent loader reloads the table before the write commits, re-caching the pre-commit
    // table with a fresh TTL.
    Table concurrentlyReloaded = catalog.loadTable(tableIdent);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(concurrentlyReloaded.currentSnapshot()).isEqualTo(committedBeforeWrite);

    // The writer commits the in-flight write through the created-table reference.
    pendingWrite.commit();
    Snapshot committedAfterWrite = writer.currentSnapshot();
    assertThat(committedAfterWrite).isNotEqualTo(committedBeforeWrite);

    // A subsequent load must observe the committed snapshot, not the stale re-cached table.
    assertThat(catalog.loadTable(tableIdent).currentSnapshot())
        .as("loadTable after a commit through a created table must not return a stale snapshot")
        .isEqualTo(committedAfterWrite);
  }

  @Test
  public void testCachingCatalogPreservesTableSubtypeAndInvalidatesOnCommit() throws IOException {
    // A delegate catalog that returns a BaseTable subclass (like RESTTable) from loadTable.
    HadoopCatalog subtypeReturningCatalog =
        new HadoopCatalog() {
          @Override
          public Table loadTable(TableIdentifier ident) {
            Table loaded = super.loadTable(ident);
            return new CustomBaseTable(((HasTableOperations) loaded).operations(), loaded.name());
          }
        };
    subtypeReturningCatalog.setConf(new Configuration());
    subtypeReturningCatalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, tempDir.getAbsolutePath()));

    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(subtypeReturningCatalog, EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Load fresh from the delegate so the cache wraps the delegate's subtype.
    catalog.cache().invalidateAll();
    Table loaded = catalog.loadTable(tableIdent);

    // The cache preserves the delegate's concrete table type via SupportsOperationsReplacement,
    // rather than downgrading it to a plain BaseTable (which would discard specialized behavior).
    assertThat(loaded).isInstanceOf(CustomBaseTable.class);

    // A commit through the subtype still invalidates the cache entry.
    loaded.newAppend().appendFile(FILE_A).commit();
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);
  }

  @Test
  public void testCachingCatalogLeavesUnknownSubclassUnchanged() throws IOException {
    // A delegate that returns a BaseTable subclass which does NOT opt in to operations replacement.
    HadoopCatalog subtypeReturningCatalog =
        new HadoopCatalog() {
          @Override
          public Table loadTable(TableIdentifier ident) {
            Table loaded = super.loadTable(ident);
            return new NonReplaceableBaseTable(
                ((HasTableOperations) loaded).operations(), loaded.name());
          }
        };
    subtypeReturningCatalog.setConf(new Configuration());
    subtypeReturningCatalog.initialize(
        "hadoop", ImmutableMap.of(CatalogProperties.WAREHOUSE_LOCATION, tempDir.getAbsolutePath()));

    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(subtypeReturningCatalog, EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));

    // Load fresh from the delegate so the cache processes the subtype.
    catalog.cache().invalidateAll();
    Table loaded = catalog.loadTable(tableIdent);

    // A subclass that does not opt in must be returned unchanged, not downgraded to a plain
    // BaseTable (which would discard its overridden behavior).
    assertThat(loaded).isInstanceOf(NonReplaceableBaseTable.class);
  }

  @Test
  public void testCacheExpirationEagerlyRemovesMetadataTables() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    Table table = catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);

    table.newAppend().appendFile(FILE_A).commit();

    // The commit invalidates the cached entry; reload so the data table is cached again.
    catalog.loadTable(tableIdent);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(Duration.ZERO);

    ticker.advance(HALF_OF_EXPIRATION);
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    assertThat(catalog.ageOf(tableIdent)).get().isEqualTo(HALF_OF_EXPIRATION);

    // Load the metadata tables for the first time. Their age should be zero as they're new entries.
    Arrays.stream(metadataTables(tableIdent)).forEach(catalog::loadTable);
    assertThat(catalog.cache().asMap()).containsKeys(metadataTables(tableIdent));
    assertThat(Arrays.stream(metadataTables(tableIdent)).map(catalog::ageOf))
        .isNotEmpty()
        .allMatch(age -> age.isPresent() && age.get().equals(Duration.ZERO));

    assertThat(catalog.remainingAgeFor(tableIdent))
        .as("Loading a non-cached metadata table should refresh the main table's age")
        .contains(EXPIRATION_TTL);

    // Move time forward and access already cached metadata tables.
    ticker.advance(HALF_OF_EXPIRATION);
    Arrays.stream(metadataTables(tableIdent)).forEach(catalog::loadTable);
    assertThat(Arrays.stream(metadataTables(tableIdent)).map(catalog::ageOf))
        .isNotEmpty()
        .allMatch(age -> age.isPresent() && age.get().equals(Duration.ZERO));

    assertThat(catalog.remainingAgeFor(tableIdent))
        .as("Accessing a cached metadata table should not affect the main table's age")
        .contains(HALF_OF_EXPIRATION);

    // Move time forward so the data table drops.
    ticker.advance(HALF_OF_EXPIRATION);
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);

    Arrays.stream(metadataTables(tableIdent))
        .forEach(
            metadataTable ->
                assertThat(catalog.cache().asMap())
                    .as(
                        "When a data table expires, its metadata tables should expire regardless of age")
                    .doesNotContainKeys(metadataTable));
  }

  @Test
  public void testDeadlock() throws IOException, InterruptedException {
    HadoopCatalog underlyingCatalog = hadoopCatalog();
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(underlyingCatalog, Duration.ofSeconds(1), ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    int numThreads = 20;
    List<TableIdentifier> createdTables = Lists.newArrayList();
    for (int i = 0; i < numThreads; i++) {
      TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl" + i);
      catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));
      createdTables.add(tableIdent);
    }

    Cache<TableIdentifier, Table> cache = catalog.cache();
    AtomicInteger cacheGetCount = new AtomicInteger(0);
    AtomicInteger cacheCleanupCount = new AtomicInteger(0);
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    for (int i = 0; i < numThreads; i++) {
      if (i % 2 == 0) {
        String table = "tbl" + i;
        executor.submit(
            () -> {
              ticker.advance(Duration.ofSeconds(2));
              cache.get(TableIdentifier.of(namespace, table), underlyingCatalog::loadTable);
              cacheGetCount.incrementAndGet();
            });
      } else {
        executor.submit(
            () -> {
              ticker.advance(Duration.ofSeconds(2));
              cache.cleanUp();
              cacheCleanupCount.incrementAndGet();
            });
      }
    }
    executor.awaitTermination(2, TimeUnit.SECONDS);
    assertThat(cacheGetCount).hasValue(numThreads / 2);
    assertThat(cacheCleanupCount).hasValue(numThreads / 2);

    executor.shutdown();
    createdTables.forEach(table -> catalog.dropTable(table, true));
  }

  @Test
  public void testCachingCatalogRejectsExpirationIntervalOfZero() {
    assertThatThrownBy(() -> TestableCachingCatalog.wrap(hadoopCatalog(), Duration.ZERO, ticker))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "When cache.expiration-interval-ms is set to 0, the catalog cache should be disabled. This indicates a bug.");
  }

  @Test
  public void testCacheExpirationIsDisabledByANegativeValue() throws IOException {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(
            hadoopCatalog(),
            Duration.ofMillis(CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS_OFF),
            ticker);

    assertThat(catalog.isCacheExpirationEnabled())
        .as(
            "When a negative value is used as the expiration interval, the cache should not expire entries based on a TTL")
        .isFalse();
  }

  @Test
  public void testInvalidateTableForChainedCachingCatalogs() throws Exception {
    TestableCachingCatalog wrappedCatalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(wrappedCatalog, EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier tableIdent = TableIdentifier.of(namespace, "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));
    assertThat(catalog.cache().asMap()).containsKey(tableIdent);
    catalog.invalidateTable(tableIdent);
    assertThat(catalog.cache().asMap()).doesNotContainKey(tableIdent);
    assertThat(wrappedCatalog.cache().asMap()).doesNotContainKey(tableIdent);
  }

  @Test
  public void testRegisterTableWithOverwriteInvalidatesCache() throws Exception {
    TestableCachingCatalog catalog =
        TestableCachingCatalog.wrap(hadoopCatalog(), EXPIRATION_TTL, ticker);
    Namespace namespace = Namespace.of("db", "ns1", "ns2");
    TableIdentifier sourceIdent = TableIdentifier.of(namespace, "src");
    TableIdentifier targetIdent = TableIdentifier.of(namespace, "tgt");

    Table sourceTable =
        catalog.createTable(sourceIdent, SCHEMA, SPEC, ImmutableMap.of("key", "value"));
    String metadataLocation =
        ((HasTableOperations) sourceTable).operations().current().metadataFileLocation();

    Table registered = catalog.registerTable(targetIdent, metadataLocation, false);
    assertThat(registered).isNotNull();

    // load so the target table is cached
    catalog.loadTable(targetIdent);
    assertThat(catalog.cache().asMap()).containsKey(targetIdent);

    // drop and re-register with overwrite=false to verify cache invalidation
    catalog.dropTable(targetIdent);
    catalog.registerTable(targetIdent, metadataLocation, false);

    // cache should have been invalidated by registerTable
    assertThat(catalog.cache().asMap()).doesNotContainKey(targetIdent);
  }

  public static TableIdentifier[] metadataTables(TableIdentifier tableIdent) {
    return Arrays.stream(MetadataTableType.values())
        .map(type -> TableIdentifier.parse(tableIdent + "." + type.name().toLowerCase(Locale.ROOT)))
        .toArray(TableIdentifier[]::new);
  }

  /**
   * A {@link BaseTable} subclass that opts in to operations replacement, preserving its type when
   * re-created with new operations.
   */
  private static class CustomBaseTable extends BaseTable implements SupportsOperationsReplacement {
    private CustomBaseTable(TableOperations ops, String name) {
      super(ops, name);
    }

    @Override
    public Table withOperations(TableOperations newOps) {
      return new CustomBaseTable(newOps, name());
    }
  }

  /** A {@link BaseTable} subclass that does not opt in to operations replacement. */
  private static class NonReplaceableBaseTable extends BaseTable {
    private NonReplaceableBaseTable(TableOperations ops, String name) {
      super(ops, name);
    }
  }
}
