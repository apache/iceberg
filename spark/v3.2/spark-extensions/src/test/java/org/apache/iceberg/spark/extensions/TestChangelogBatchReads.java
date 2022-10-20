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
package org.apache.iceberg.spark.extensions;

import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MANIFEST_MERGE_ENABLED;
import static org.apache.iceberg.TableProperties.MANIFEST_MIN_MERGE_COUNT;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

public class TestChangelogBatchReads extends SparkExtensionsTestBase {

  @Parameters(name = "formatVersion = {0}, catalogName = {1}, implementation = {2}, config = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        1,
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties()
      },
      {
        2,
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties()
      }
    };
  }

  private final int formatVersion;

  public TestChangelogBatchReads(
      int formatVersion, String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
    this.formatVersion = formatVersion;
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDataFilters() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion);

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO %s VALUES (3, 'c')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap3 = table.currentSnapshot();

    sql("DELETE FROM %s WHERE id = 3", tableName);

    table.refresh();

    Snapshot snap4 = table.currentSnapshot();

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(
            row(3, "c", "INSERT", 2, snap3.snapshotId()),
            row(3, "c", "DELETE", 3, snap4.snapshotId())),
        sql("SELECT * FROM %s.changes WHERE id = 3 ORDER BY _change_ordinal, id", tableName));
  }

  @Test
  public void testOverwrites() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion);

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap2 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);

    table.refresh();

    Snapshot snap3 = table.currentSnapshot();

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", "DELETE", 0, snap3.snapshotId()),
            row(-2, "b", "INSERT", 0, snap3.snapshotId())),
        changelogRecords(snap2, snap3));
  }

  @Test
  public void testMetadataDeletes() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion);

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap2 = table.currentSnapshot();

    sql("DELETE FROM %s WHERE data = 'a'", tableName);

    table.refresh();

    Snapshot snap3 = table.currentSnapshot();
    Assert.assertEquals("Operation must match", DataOperations.DELETE, snap3.operation());

    assertEquals(
        "Rows should match",
        ImmutableList.of(row(1, "a", "DELETE", 0, snap3.snapshotId())),
        changelogRecords(snap2, snap3));
  }

  @Test
  public void testExistingEntriesInNewDataManifestsAreIgnored() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d', "
            + " '%s' = '1', "
            + " '%s' = 'true' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion, MANIFEST_MIN_MERGE_COUNT, MANIFEST_MERGE_ENABLED);

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    table.refresh();

    Snapshot snap2 = table.currentSnapshot();
    Assert.assertEquals("Manifest number must match", 1, snap2.dataManifests(table.io()).size());

    assertEquals(
        "Rows should match",
        ImmutableList.of(row(2, "b", "INSERT", 0, snap2.snapshotId())),
        changelogRecords(snap1, snap2));
  }

  @Test
  public void testManifestRewritesAreIgnored() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion);

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Num snapshots must match", 3, Iterables.size(table.snapshots()));

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "INSERT"), row(2, "INSERT")),
        sql("SELECT id, _change_type FROM %s.changes ORDER BY id", tableName));
  }

  private List<Object[]> changelogRecords(Snapshot startSnapshot, Snapshot endSnapshot) {
    DataFrameReader reader = spark.read();

    if (startSnapshot != null) {
      reader = reader.option(SparkReadOptions.START_SNAPSHOT_ID, startSnapshot.snapshotId());
    }

    if (endSnapshot != null) {
      reader = reader.option(SparkReadOptions.END_SNAPSHOT_ID, endSnapshot.snapshotId());
    }

    return rowsToJava(collect(reader));
  }

  private List<Row> collect(DataFrameReader reader) {
    return reader
        .table(tableName + "." + SparkChangelogTable.TABLE_NAME)
        .orderBy("_change_ordinal", "_commit_snapshot_id", "_change_type", "id")
        .collectAsList();
  }
}
