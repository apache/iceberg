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

import java.util.List;
import java.util.Map;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkReadOptions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestCreateChangelogViewProcedure extends SparkExtensionsTestBase {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  public TestCreateChangelogViewProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  public void createTableWith2Columns() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='%d')", tableName, 1);
    sql("ALTER TABLE %s ADD PARTITION FIELD data", tableName);
  }

  private void createTableWith3Columns() {
    sql("CREATE TABLE %s (id INT, data STRING, age INT) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='%d')", tableName, 1);
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);
  }

  private void createTableWithIdentifierField() {
    sql("CREATE TABLE %s (id INT NOT NULL, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='%d')", tableName, 1);
    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id", tableName);
  }

  @Test
  public void testCustomizedViewName() {
    createTableWith2Columns();
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);

    table.refresh();

    Snapshot snap2 = table.currentSnapshot();

    sql(
        "CALL %s.system.create_changelog_view("
            + "table => '%s',"
            + "options => map('%s','%s','%s','%s'),"
            + "changelog_view => '%s')",
        catalogName,
        tableName,
        SparkReadOptions.START_SNAPSHOT_ID,
        snap1.snapshotId(),
        SparkReadOptions.END_SNAPSHOT_ID,
        snap2.snapshotId(),
        "cdc_view");

    long rowCount = sql("select * from %s", "cdc_view").stream().count();
    Assert.assertEquals(2, rowCount);
  }

  @Test
  public void testNoSnapshotIdInput() {
    createTableWith2Columns();
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(" + "table => '%s')",
            catalogName, tableName, "cdc_view");

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()),
            row(2, "b", INSERT, 1, snap1.snapshotId()),
            row(-2, "b", INSERT, 2, snap2.snapshotId()),
            row(2, "b", DELETE, 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", viewName));
  }

  @Test
  public void testTimestampsBasedQuery() {
    createTableWith2Columns();
    long beginning = System.currentTimeMillis();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();
    long afterFirstInsert = waitUntilAfter(snap0.timestampMillis());

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    long afterInsertOverwrite = waitUntilAfter(snap2.timestampMillis());
    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('%s', '%s','%s', '%s'))",
            catalogName,
            tableName,
            SparkReadOptions.START_TIMESTAMP,
            beginning,
            SparkReadOptions.END_TIMESTAMP,
            afterInsertOverwrite);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()),
            row(2, "b", INSERT, 1, snap1.snapshotId()),
            row(-2, "b", INSERT, 2, snap2.snapshotId()),
            row(2, "b", DELETE, 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));

    // query the timestamps starting from the second insert
    returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('%s', '%s', '%s', '%s'))",
            catalogName,
            tableName,
            SparkReadOptions.START_TIMESTAMP,
            afterFirstInsert,
            SparkReadOptions.END_TIMESTAMP,
            afterInsertOverwrite);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(-2, "b", INSERT, 1, snap2.snapshotId()),
            row(2, "b", DELETE, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @Test
  public void testWithCarryovers() {
    createTableWith2Columns();
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b'), (2, 'b'), (2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view("
                + "remove_carryovers => false,"
                + "table => '%s')",
            catalogName, tableName, "cdc_view");

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()),
            row(2, "b", INSERT, 1, snap1.snapshotId()),
            row(-2, "b", INSERT, 2, snap2.snapshotId()),
            row(2, "b", DELETE, 2, snap2.snapshotId()),
            row(2, "b", INSERT, 2, snap2.snapshotId()),
            row(2, "b", INSERT, 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, _change_type", viewName));
  }

  @Test
  public void testUpdate() {
    createTableWith2Columns();
    sql("ALTER TABLE %s DROP PARTITION FIELD data", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (3, 'c'), (2, 'd')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', identifier_columns => array('id'))",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap1.snapshotId()),
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(2, "b", UPDATE_BEFORE, 1, snap2.snapshotId()),
            row(2, "d", UPDATE_AFTER, 1, snap2.snapshotId()),
            row(3, "c", INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testUpdateWithIdentifierField() {
    createTableWithIdentifierField();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (3, 'c'), (2, 'd')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', compute_updates => true)",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(2, "b", UPDATE_BEFORE, 1, snap2.snapshotId()),
            row(2, "d", UPDATE_AFTER, 1, snap2.snapshotId()),
            row(3, "c", INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testUpdateWithFilter() {
    createTableWith2Columns();
    sql("ALTER TABLE %s DROP PARTITION FIELD data", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (3, 'c'), (2, 'd')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', identifier_columns => array('id'))",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap1.snapshotId()),
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(2, "b", UPDATE_BEFORE, 1, snap2.snapshotId()),
            row(2, "d", UPDATE_AFTER, 1, snap2.snapshotId())),
        // the predicate on partition columns will filter out the insert of (3, 'c') at the planning
        // phase
        sql("select * from %s where id != 3 order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testUpdateWithMultipleIdentifierColumns() {
    createTableWith3Columns();

    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view("
                + "identifier_columns => array('id','age'),"
                + "table => '%s')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, UPDATE_BEFORE, 1, snap2.snapshotId()),
            row(2, "d", 11, UPDATE_AFTER, 1, snap2.snapshotId()),
            row(2, "e", 12, INSERT, 1, snap2.snapshotId()),
            row(3, "c", 13, INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testRemoveCarryOvers() {
    createTableWith3Columns();

    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11), (2, 'e', 12)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // carry-over row (2, 'e', 12)
    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view("
                + "identifier_columns => array('id','age'), "
                + "table => '%s')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    // the carry-over rows (2, 'e', 12, 'DELETE', 1), (2, 'e', 12, 'INSERT', 1) are removed
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, INSERT, 0, snap1.snapshotId()),
            row(2, "e", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, UPDATE_BEFORE, 1, snap2.snapshotId()),
            row(2, "d", 11, UPDATE_AFTER, 1, snap2.snapshotId()),
            row(3, "c", 13, INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testRemoveCarryOversWithoutUpdatedRows() {
    createTableWith3Columns();

    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11), (2, 'e', 12)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // carry-over row (2, 'e', 12)
    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql("CALL %s.system.create_changelog_view(table => '%s')", catalogName, tableName);

    String viewName = (String) returns.get(0)[0];

    // the carry-over rows (2, 'e', 12, 'DELETE', 1), (2, 'e', 12, 'INSERT', 1) are removed, even
    // though update-row is not computed
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, INSERT, 0, snap1.snapshotId()),
            row(2, "e", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, DELETE, 1, snap2.snapshotId()),
            row(2, "d", 11, INSERT, 1, snap2.snapshotId()),
            row(3, "c", 13, INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testNotRemoveCarryOvers() {
    createTableWith3Columns();

    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11), (2, 'e', 12)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // carry-over row (2, 'e', 12)
    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view("
                + "remove_carryovers => false,"
                + "table => '%s')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, INSERT, 0, snap1.snapshotId()),
            row(2, "e", 12, INSERT, 0, snap1.snapshotId()),
            row(2, "b", 11, DELETE, 1, snap2.snapshotId()),
            row(2, "d", 11, INSERT, 1, snap2.snapshotId()),
            // the following two rows are carry-over rows
            row(2, "e", 12, DELETE, 1, snap2.snapshotId()),
            row(2, "e", 12, INSERT, 1, snap2.snapshotId()),
            row(3, "c", 13, INSERT, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data, _change_type", viewName));
  }
}
