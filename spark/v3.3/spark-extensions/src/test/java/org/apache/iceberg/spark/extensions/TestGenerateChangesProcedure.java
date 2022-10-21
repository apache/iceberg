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

import static org.apache.iceberg.ChangelogOperation.UPDATE_POSTIMAGE;
import static org.apache.iceberg.ChangelogOperation.UPDATE_PREIMAGE;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestGenerateChangesProcedure extends SparkExtensionsTestBase {
  public TestGenerateChangesProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Test
  public void testCustomizedViewName() {
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);

    table.refresh();

    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.generate_changes("
                + "table => '%s',"
                + "start_snapshot_id_exclusive => %s,"
                + "end_snapshot_id_inclusive => %s,"
                + "table_change_view => '%s')",
            catalogName, tableName, snap1.snapshotId(), snap2.snapshotId(), "cdc_view");

    long rowCount = sql("select * from %s", "cdc_view").stream().count();
    Assert.assertEquals(2, rowCount);
  }

  @Test
  public void testNoSnapshotIdInput() {
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
            "CALL %s.system.generate_changes(" + "table => '%s')",
            catalogName, tableName, "cdc_view");

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap0.snapshotId()),
            row(2, "b", "INSERT", 1, snap1.snapshotId()),
            row(-2, "b", "INSERT", 2, snap2.snapshotId()),
            row(2, "b", "DELETE", 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", viewName));
  }

  @Test
  public void testTimestamps() {
    String beginning = LocalDateTime.now().toString();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();
    String afterFirstInsert = LocalDateTime.now().toString();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    String afterInsertOverwrite = LocalDateTime.now().toString();
    List<Object[]> returns =
        sql(
            "CALL %s.system.generate_changes(table => '%s', "
                + "start_timestamp => TIMESTAMP '%s', "
                + "end_timestamp => TIMESTAMP '%s')",
            catalogName, tableName, beginning, afterInsertOverwrite);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap0.snapshotId()),
            row(2, "b", "INSERT", 1, snap1.snapshotId()),
            row(-2, "b", "INSERT", 2, snap2.snapshotId()),
            row(2, "b", "DELETE", 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));

    // query the timestamps starting from the second insert
    returns =
        sql(
            "CALL %s.system.generate_changes(table => '%s', "
                + "start_timestamp => TIMESTAMP '%s', "
                + "end_timestamp => TIMESTAMP '%s')",
            catalogName, tableName, afterFirstInsert, afterInsertOverwrite);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", "INSERT", 0, snap1.snapshotId()),
            row(-2, "b", "INSERT", 1, snap2.snapshotId()),
            row(2, "b", "DELETE", 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));

    // query without start timestamp

  }

  @Test
  public void testWithCarryOvers() {
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
            "CALL %s.system.generate_changes(" + "table => '%s')",
            catalogName, tableName, "cdc_view");

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap0.snapshotId()),
            row(2, "b", "INSERT", 1, snap1.snapshotId()),
            row(-2, "b", "INSERT", 2, snap2.snapshotId()),
            row(2, "b", "DELETE", 2, snap2.snapshotId()),
            row(2, "b", "INSERT", 2, snap2.snapshotId()),
            row(2, "b", "INSERT", 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, _change_type", viewName));
  }

  @Test
  public void testUpdate() {
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
            "CALL %s.system.generate_changes(table => '%s', identifier_columns => 'id')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap1.snapshotId()),
            row(2, "b", "INSERT", 0, snap1.snapshotId()),
            row(2, "b", UPDATE_PREIMAGE.name(), 1, snap2.snapshotId()),
            row(2, "d", UPDATE_POSTIMAGE.name(), 1, snap2.snapshotId()),
            row(3, "c", "INSERT", 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testUpdateWithFilter() {
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
            "CALL %s.system.generate_changes(table => '%s', identifier_columns => 'id')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", "INSERT", 0, snap1.snapshotId()),
            row(2, "b", "INSERT", 0, snap1.snapshotId()),
            row(2, "b", UPDATE_PREIMAGE.name(), 1, snap2.snapshotId()),
            row(2, "d", UPDATE_POSTIMAGE.name(), 1, snap2.snapshotId())),
        // the predicate on partition columns will filter out the insert of (3, 'c') at the planning
        // phase
        sql("select * from %s where id != 3 order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testUpdateWithMultipleIdentifierColumns() {
    removeTables();
    createTableWith3Columns();

    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.generate_changes("
                + "identifier_columns => 'id,age',"
                + "table => '%s')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, "INSERT", 0, snap1.snapshotId()),
            row(2, "b", 11, "INSERT", 0, snap1.snapshotId()),
            row(2, "b", 11, UPDATE_PREIMAGE.name(), 1, snap2.snapshotId()),
            row(2, "d", 11, UPDATE_POSTIMAGE.name(), 1, snap2.snapshotId()),
            row(2, "e", 12, "INSERT", 1, snap2.snapshotId()),
            row(3, "c", 13, "INSERT", 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Test
  public void testRemoveCarryOvers() {
    removeTables();
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
            "CALL %s.system.generate_changes("
                + "identifier_columns => 'id,age',"
                + "table => '%s')",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, "INSERT", 0, snap1.snapshotId()),
            row(2, "b", 11, "INSERT", 0, snap1.snapshotId()),
            row(2, "e", 12, "INSERT", 0, snap1.snapshotId()),
            row(2, "b", 11, UPDATE_PREIMAGE.name(), 1, snap2.snapshotId()),
            row(2, "d", 11, UPDATE_POSTIMAGE.name(), 1, snap2.snapshotId()),
            row(3, "c", 13, "INSERT", 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id, data", viewName));
  }

  @Before
  public void setupTable() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='%d')", tableName, 1);
    sql("ALTER TABLE %s ADD PARTITION FIELD data", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  private void createTableWith3Columns() {
    sql("CREATE TABLE %s (id INT, data STRING, age INT) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('format-version'='%d')", tableName, 1);
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);
  }
}
