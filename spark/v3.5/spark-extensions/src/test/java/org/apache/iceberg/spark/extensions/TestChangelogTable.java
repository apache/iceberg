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
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.source.SparkChangelogTable;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Row;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestChangelogTable extends ExtensionsTestBase {

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, formatVersion = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        SparkCatalogConfig.SPARK.properties(),
        1
      },
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        SparkCatalogConfig.HIVE.properties(),
        2
      }
    };
  }

  @Parameter(index = 3)
  private int formatVersion;

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testDataFilters() {
    createTableWithDefaultRows();

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

  @TestTemplate
  public void testOverwrites() {
    createTableWithDefaultRows();

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

  @TestTemplate
  public void testQueryWithTimeRange() {
    createTable();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();
    long rightAfterSnap1 = waitUntilAfter(snap1.timestampMillis());

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();
    long rightAfterSnap2 = waitUntilAfter(snap2.timestampMillis());

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();
    long rightAfterSnap3 = waitUntilAfter(snap3.timestampMillis());

    assertEquals(
        "Should have expected changed rows only from snapshot 3",
        ImmutableList.of(
            row(2, "b", "DELETE", 0, snap3.snapshotId()),
            row(-2, "b", "INSERT", 0, snap3.snapshotId())),
        changelogRecords(rightAfterSnap2, snap3.timestampMillis()));

    assertEquals(
        "Should have expected changed rows only from snapshot 3",
        ImmutableList.of(
            row(2, "b", "DELETE", 0, snap3.snapshotId()),
            row(-2, "b", "INSERT", 0, snap3.snapshotId())),
        changelogRecords(snap2.timestampMillis(), snap3.timestampMillis()));

    assertEquals(
        "Should have expected changed rows from snapshot 2 and 3",
        ImmutableList.of(
            row(2, "b", "INSERT", 0, snap2.snapshotId()),
            row(2, "b", "DELETE", 1, snap3.snapshotId()),
            row(-2, "b", "INSERT", 1, snap3.snapshotId())),
        changelogRecords(rightAfterSnap1, snap3.timestampMillis()));

    assertEquals(
        "Should have expected changed rows up to the current snapshot",
        ImmutableList.of(
            row(2, "b", "INSERT", 0, snap2.snapshotId()),
            row(2, "b", "DELETE", 1, snap3.snapshotId()),
            row(-2, "b", "INSERT", 1, snap3.snapshotId())),
        changelogRecords(rightAfterSnap1, null));

    assertEquals(
        "Should have empty changed rows if end time is before the first snapshot",
        ImmutableList.of(),
        changelogRecords(null, snap1.timestampMillis() - 1));

    assertEquals(
        "Should have empty changed rows if start time is after the current snapshot",
        ImmutableList.of(),
        changelogRecords(rightAfterSnap3, null));

    assertEquals(
        "Should have empty changed rows if end time is before the first snapshot",
        ImmutableList.of(),
        changelogRecords(null, snap1.timestampMillis() - 1));

    assertEquals(
        "Should have empty changed rows if there are no snapshots between start time and end time",
        ImmutableList.of(),
        changelogRecords(rightAfterSnap2, snap3.timestampMillis() - 1));
  }

  @TestTemplate
  public void testTimeRangeValidation() {
    createTableWithDefaultRows();

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap2 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();
    long rightAfterSnap3 = waitUntilAfter(snap3.timestampMillis());
    Assertions.assertThatThrownBy(
            () -> changelogRecords(snap3.timestampMillis(), snap2.timestampMillis()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot set start-timestamp to be greater than end-timestamp for changelogs");
  }

  @TestTemplate
  public void testMetadataDeletes() {
    createTableWithDefaultRows();

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap2 = table.currentSnapshot();

    sql("DELETE FROM %s WHERE data = 'a'", tableName);

    table.refresh();

    Snapshot snap3 = table.currentSnapshot();
    assertThat(snap3.operation()).as("Operation must match").isEqualTo(DataOperations.DELETE);

    assertEquals(
        "Rows should match",
        ImmutableList.of(row(1, "a", "DELETE", 0, snap3.snapshotId())),
        changelogRecords(snap2, snap3));
  }

  @TestTemplate
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
    assertThat(snap2.dataManifests(table.io())).as("Manifest number must match").hasSize(1);

    assertEquals(
        "Rows should match",
        ImmutableList.of(row(2, "b", "INSERT", 0, snap2.snapshotId())),
        changelogRecords(snap1, snap2));
  }

  @TestTemplate
  public void testManifestRewritesAreIgnored() {
    createTableWithDefaultRows();

    sql("CALL %s.system.rewrite_manifests('%s')", catalogName, tableIdent);

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.snapshots()).as("Num snapshots must match").hasSize(3);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1, "INSERT"), row(2, "INSERT")),
        sql("SELECT id, _change_type FROM %s.changes ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMetadataColumns() {
    createTableWithDefaultRows();
    List<Object[]> rows =
        sql(
            "SELECT id, _file, _pos, _deleted, _spec_id, _partition FROM %s.changes ORDER BY id",
            tableName);

    String file1 = rows.get(0)[1].toString();
    assertThat(file1).startsWith("file:/");
    String file2 = rows.get(1)[1].toString();

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, file1, 0L, false, 0, row("a")), row(2, file2, 0L, false, 0, row("b"))),
        rows);
  }

  @TestTemplate
  public void testQueryWithRollback() {
    createTable();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();
    long rightAfterSnap1 = waitUntilAfter(snap1.timestampMillis());

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();
    long rightAfterSnap2 = waitUntilAfter(snap2.timestampMillis());

    sql(
        "CALL %s.system.rollback_to_snapshot('%s', %d)",
        catalogName, tableIdent, snap1.snapshotId());
    table.refresh();
    assertThat(table.currentSnapshot()).isEqualTo(snap1);

    sql("INSERT OVERWRITE %s VALUES (-2, 'a')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();
    long rightAfterSnap3 = waitUntilAfter(snap3.timestampMillis());

    assertEquals(
        "Should have expected changed rows up to snapshot 3",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap1.snapshotId()),
            row(1, "a", "DELETE", 1, snap3.snapshotId()),
            row(-2, "a", "INSERT", 1, snap3.snapshotId())),
        changelogRecords(null, rightAfterSnap3));

    assertEquals(
        "Should have expected changed rows up to snapshot 2",
        ImmutableList.of(row(1, "a", "INSERT", 0, snap1.snapshotId())),
        changelogRecords(null, rightAfterSnap2));

    assertEquals(
        "Should have expected changed rows from snapshot 3 only since snapshot 2 is on a different branch.",
        ImmutableList.of(
            row(1, "a", "DELETE", 0, snap3.snapshotId()),
            row(-2, "a", "INSERT", 0, snap3.snapshotId())),
        changelogRecords(rightAfterSnap1, snap3.timestampMillis()));

    assertEquals(
        "Should have expected changed rows from snapshot 3",
        ImmutableList.of(
            row(1, "a", "DELETE", 0, snap3.snapshotId()),
            row(-2, "a", "INSERT", 0, snap3.snapshotId())),
        changelogRecords(rightAfterSnap2, null));

    sql(
        "CALL %s.system.set_current_snapshot('%s', %d)",
        catalogName, tableIdent, snap2.snapshotId());
    table.refresh();
    assertThat(table.currentSnapshot()).isEqualTo(snap2);
    assertEquals(
        "Should have expected changed rows from snapshot 2 only since snapshot 3 is on a different branch.",
        ImmutableList.of(row(2, "b", "INSERT", 0, snap2.snapshotId())),
        changelogRecords(rightAfterSnap1, null));
  }

  private void createTableWithDefaultRows() {
    createTable();
    insertDefaultRows();
  }

  private void createTable() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (data) "
            + "TBLPROPERTIES ( "
            + " '%s' = '%d' "
            + ")",
        tableName, FORMAT_VERSION, formatVersion);
  }

  private void insertDefaultRows() {
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
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

  private List<Object[]> changelogRecords(Long startTimestamp, Long endTimeStamp) {
    DataFrameReader reader = spark.read();

    if (startTimestamp != null) {
      reader = reader.option(SparkReadOptions.START_TIMESTAMP, startTimestamp);
    }

    if (endTimeStamp != null) {
      reader = reader.option(SparkReadOptions.END_TIMESTAMP, endTimeStamp);
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
