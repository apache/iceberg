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
import com.github.benmanes.caffeine.cache.Ticker;
import java.io.IOException;
import java.time.Duration;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;
import org.apache.iceberg.util.FakeTicker;


public class TestCachingCatalog extends HadoopTableTestBase {

  @Test
  public void testInvalidateMetadataTablesIfBaseTableIsModified() throws Exception {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();

    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog).build();
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
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();

    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog).build();

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
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();

    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog).build();
    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    Table table = catalog.loadTable(tableIdent);
    Assert.assertEquals("Name must match", "hadoop.db.ns1.ns2.tbl", table.name());

    TableIdentifier snapshotsTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "snapshots");
    Table snapshotsTable = catalog.loadTable(snapshotsTableIdent);
    Assert.assertEquals("Name must match", "hadoop.db.ns1.ns2.tbl.snapshots", snapshotsTable.name());
  }

  @Test
  public void testTableDropsAfterInaccessWhenConfigured() throws Exception {
    // TODO - Can I possibly assert Java9+ and use the dedicated System timer thread instead?
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();

    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    Catalog catalog = CachingCatalog.wrap(hadoopCatalog).build();

    TableIdentifier tableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl");
    catalog.createTable(tableIdent, SCHEMA, SPEC, ImmutableMap.of("key2", "value2"));

    Table table = catalog.loadTable(tableIdent);
    Assert.assertTrue(table.currentSnapshot() == null);  // Table is empty

    // Writing should update the Cache TTL
    table.newAppend().appendFile(FILE_A).commit();
    Snapshot oldSnapshot = table.currentSnapshot();

    // TODO - Assert on the state of the cache if possible. See testing library.
    TableIdentifier filesMetaTableIdent = TableIdentifier.of("db", "ns1", "ns2", "tbl", "files");
    Table filesMetaTable = catalog.loadTable(filesMetaTableIdent);
  }

  @Test
  public void testTableExpiration() {

    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    boolean caseSensitive = true;
    boolean expirationEnabled = true;
    long expirationMillis = Duration.ofMinutes(5).toMillis();

    // Create CachingCatalog with a controllable ticker for testing cache expiry.
    Ticker ticker = new FakeTicker();
    Cache<TableIdentifier, Table> tableCache = CachingCatalog.createTableCache(
        expirationEnabled, expirationMillis, ticker);
    HadopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    Catalog catalog = new CachingCatalog(hadoopCatalog, caseSensitive, expirationEnabled, expirationMillis, tableCache);

    long expirationMillis = Duration.ofSeconds(100).toMillis();
    Cache<TableIdentifier, Table> cache = CachingCatalog.createTableCache(true, expirationMillis, new FakeTicker());
  }

  @Test
  public void testMetadataTablesExpireOnCacheExpiration() {
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

  private Catalog createCachingCatalog(boolean expirationEnabled, long expirationMillis, Ticker ticker)
      throws IOException {
    Configuration conf = new Configuration();
    String warehousePath = temp.newFolder().getAbsolutePath();
    Cache<TableIdentifier, Table> tableCache = CachingCatalog.createTableCache(
        expirationEnabled, expirationMillis, ticker);
    HadoopCatalog hadoopCatalog = new HadoopCatalog(conf, warehousePath);
    return new CachingCatalog(hadoopCatalog, true, expirationEnabled, expirationMillis, tableCache);
  }
}
