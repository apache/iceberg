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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCreateChangelogViewProcedure extends ExtensionsTestBase {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  public void createTableWithTwoColumns() {
    sql("CREATE TABLE %s (id INT, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD data", tableName);
  }

  private void createTableWithThreeColumns() {
    sql("CREATE TABLE %s (id INT, data STRING, age INT) USING iceberg", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD id", tableName);
  }

  private void createTableWithIdentifierField() {
    sql("CREATE TABLE %s (id INT NOT NULL, data STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s SET IDENTIFIER FIELDS id", tableName);
  }

  @TestTemplate
  public void testCustomizedViewName() {
    createTableWithTwoColumns();
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
    assertThat(rowCount).isEqualTo(2);
  }

  @TestTemplate
  public void testNonStandardColumnNames() {
    sql("CREATE TABLE %s (`the id` INT, `the.data` STRING) USING iceberg", tableName);
    sql("ALTER TABLE %s ADD PARTITION FIELD `the.data`", tableName);

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

    var df = spark.sql("select * from cdc_view");
    var fieldNames =
        Arrays.stream(df.schema().fields()).map(StructField::name).collect(Collectors.toList());
    assertThat(fieldNames)
        .containsExactly(
            "the id", "the.data", "_change_type", "_change_ordinal", "_commit_snapshot_id");

    assertThat(df.collectAsList()).hasSize(2);
  }

  @TestTemplate
  public void testNoSnapshotIdInput() {
    createTableWithTwoColumns();
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

  @TestTemplate
  public void testOnlyStartSnapshotIdInput() {
    createTableWithTwoColumns();
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
            "CALL %s.system.create_changelog_view(table => '%s'," + "options => map('%s', '%s'))",
            catalogName, tableName, SparkReadOptions.START_SNAPSHOT_ID, snap0.snapshotId());

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(-2, "b", INSERT, 1, snap2.snapshotId()),
            row(2, "b", DELETE, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testOnlyEndTimestampIdInput() {
    createTableWithTwoColumns();
    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s'," + "options => map('%s', '%s'))",
            catalogName, tableName, SparkReadOptions.END_SNAPSHOT_ID, snap1.snapshotId());

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()), row(2, "b", INSERT, 1, snap1.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testTimestampsBasedQuery() {
    createTableWithTwoColumns();
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

  @TestTemplate
  public void testOnlyStartTimestampInput() {
    createTableWithTwoColumns();
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

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', " + "options => map('%s', '%s'))",
            catalogName, tableName, SparkReadOptions.START_TIMESTAMP, beginning);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()),
            row(2, "b", INSERT, 1, snap1.snapshotId()),
            row(-2, "b", INSERT, 2, snap2.snapshotId()),
            row(2, "b", DELETE, 2, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));

    returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', " + "options => map('%s', '%s'))",
            catalogName, tableName, SparkReadOptions.START_TIMESTAMP, afterFirstInsert);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(-2, "b", INSERT, 1, snap2.snapshotId()),
            row(2, "b", DELETE, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testOnlyEndTimestampInput() {
    createTableWithTwoColumns();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();
    long afterSecondInsert = waitUntilAfter(snap1.timestampMillis());

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', " + "options => map('%s', '%s'))",
            catalogName, tableName, SparkReadOptions.END_TIMESTAMP, afterSecondInsert);

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", INSERT, 0, snap0.snapshotId()), row(2, "b", INSERT, 1, snap1.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testStartTimeStampEndSnapshotId() {
    createTableWithTwoColumns();

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

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('%s', '%s', '%s', '%s'))",
            catalogName,
            tableName,
            SparkReadOptions.START_TIMESTAMP,
            afterFirstInsert,
            SparkReadOptions.END_SNAPSHOT_ID,
            snap2.snapshotId());

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", INSERT, 0, snap1.snapshotId()),
            row(-2, "b", INSERT, 1, snap2.snapshotId()),
            row(2, "b", DELETE, 1, snap2.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testStartSnapshotIdEndTimestamp() {
    createTableWithTwoColumns();

    sql("INSERT INTO %s VALUES (1, 'a')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap0 = table.currentSnapshot();

    sql("INSERT INTO %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap1 = table.currentSnapshot();
    long afterSecondInsert = waitUntilAfter(snap1.timestampMillis());

    sql("INSERT OVERWRITE %s VALUES (-2, 'b')", tableName);
    table.refresh();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('%s', '%s', '%s', '%s'))",
            catalogName,
            tableName,
            SparkReadOptions.START_SNAPSHOT_ID,
            snap0.snapshotId(),
            SparkReadOptions.END_TIMESTAMP,
            afterSecondInsert);

    assertEquals(
        "Rows should match",
        ImmutableList.of(row(2, "b", INSERT, 0, snap1.snapshotId())),
        sql("select * from %s order by _change_ordinal, id", returns.get(0)[0]));
  }

  @TestTemplate
  public void testUpdate() {
    createTableWithTwoColumns();
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

  @TestTemplate
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

  @TestTemplate
  public void testUpdateWithFilter() {
    createTableWithTwoColumns();
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

  @TestTemplate
  public void testUpdateWithMultipleIdentifierColumns() {
    createTableWithThreeColumns();

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

  @TestTemplate
  public void testRemoveCarryOvers() {
    createTableWithThreeColumns();

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

  @TestTemplate
  public void testRemoveCarryOversWithoutUpdatedRows() {
    createTableWithThreeColumns();

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

  @TestTemplate
  public void testNetChangesWithRemoveCarryOvers() {
    // partitioned by id
    createTableWithThreeColumns();

    // insert rows: (1, 'a', 12) (2, 'b', 11) (2, 'e', 12)
    sql("INSERT INTO %s VALUES (1, 'a', 12), (2, 'b', 11), (2, 'e', 12)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // delete rows: (2, 'b', 11) (2, 'e', 12)
    // insert rows: (3, 'c', 13) (2, 'd', 11) (2, 'e', 12)
    sql("INSERT OVERWRITE %s VALUES (3, 'c', 13), (2, 'd', 11), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    // delete rows: (2, 'd', 11) (2, 'e', 12) (3, 'c', 13)
    // insert rows: (3, 'c', 15) (2, 'e', 12)
    sql("INSERT OVERWRITE %s VALUES (3, 'c', 15), (2, 'e', 12)", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();

    // test with all snapshots
    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', net_changes => true)",
            catalogName, tableName);

    String viewName = (String) returns.get(0)[0];

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 12, INSERT, 0, snap1.snapshotId()),
            row(3, "c", 15, INSERT, 2, snap3.snapshotId()),
            row(2, "e", 12, INSERT, 2, snap3.snapshotId())),
        sql("select * from %s order by _change_ordinal, data", viewName));

    // test with snap2 and snap3
    sql(
        "CALL %s.system.create_changelog_view(table => '%s', "
            + "options => map('start-snapshot-id','%s'), "
            + "net_changes => true)",
        catalogName, tableName, snap1.snapshotId());

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(2, "b", 11, DELETE, 0, snap2.snapshotId()),
            row(3, "c", 15, INSERT, 1, snap3.snapshotId())),
        sql("select * from %s order by _change_ordinal, data", viewName));
  }

  @TestTemplate
  public void testNetChangesComputeUpdatesSingleSnapshot() {
    createTableWithIdentifierField();

    // Snapshot 1: INSERT (1, 'a'), (2, 'b')
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: UPDATE (1, 'a') to (1, 'a2'), keep (2, 'b'), INSERT (3, 'c')
    sql("INSERT OVERWRITE %s VALUES (1, 'a2'), (2, 'b'), (3, 'c')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id','%s'), "
                + "identifier_columns => array('id'), "
                + "net_changes => true)",
            catalogName, tableName, snap1.snapshotId());

    String viewName = (String) returns.get(0)[0];

    // Expected: Net result from start-snapshot-id (snap1) onwards, showing changes in snap2
    // Row 1: (1, 'a') in snap1 -> (1, 'a2') in snap2 = UPDATE_BEFORE + UPDATE_AFTER
    // Row 2: (2, 'b') in snap1 -> (2, 'b') in snap2 = carryover, filtered out
    // Row 3: (3, 'c') inserted in snap2 = INSERT
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", UPDATE_BEFORE, 0, snap2.snapshotId()),
            row(1, "a2", UPDATE_AFTER, 0, snap2.snapshotId()),
            row(3, "c", INSERT, 0, snap2.snapshotId())),
        sql("select * from %s order by id, _change_ordinal", viewName));
  }

  @TestTemplate
  public void testNetChangesComputeUpdatesMultipleSnapshots() {
    createTableWithIdentifierField();

    // Snapshot 1: INSERT (1, 'a'), (2, 'b')
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (4, 'd')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: UPDATE (1, 'a') to (1, 'a2'), DELETE (2, 'b'), INSERT (3, 'c')
    sql("INSERT OVERWRITE %s VALUES (1, 'a2'), (2, 'b'), (3, 'c'), (4, 'd')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: DELETE (4, 'd')
    sql("INSERT OVERWRITE %s VALUES (1, 'a3'), (2, 'b3'), (3, 'c3')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();

    // Snapshot 4:
    sql("INSERT OVERWRITE %s VALUES (1, 'a4'), (2, 'b4'), (3, 'c4')", tableName);

    // Snapshot 5: UPDATE (1, 'a2') to (1, 'a3'), INSERT (2, 'b2')
    sql("INSERT OVERWRITE %s VALUES (1, 'a5'), (2, 'b2'), (3, 'c')", tableName);
    table.refresh();
    Snapshot snap5 = table.currentSnapshot();

    // Test net changes from start-snapshot-id (snap1) onwards
    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id','%s'), "
                + "identifier_columns => array('id'), "
                + "net_changes => true)",
            catalogName, tableName, snap1.snapshotId());

    String viewName = (String) returns.get(0)[0];

    // Expected: Net result from start-snapshot-id (snap1) onwards across snap2-snap5
    // Row 1: (1, 'a') in snap1 -> (1, 'a5') in snap5 = UPDATE_BEFORE + UPDATE_AFTER
    // Row 2: (2, 'b') in snap1 -> (2, 'b2') in snap5 = UPDATE_BEFORE + UPDATE_AFTER
    // Row 3: (3, 'c') inserted in snap2, carried over to snap5 = INSERT
    // Row 4: (4, 'd') inserted in snap1, deleted in snap3
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", UPDATE_BEFORE, 0, snap2.snapshotId()),
            row(1, "a5", UPDATE_AFTER, 3, snap5.snapshotId()),
            row(2, "b", UPDATE_BEFORE, 1, snap3.snapshotId()),
            row(2, "b2", UPDATE_AFTER, 3, snap5.snapshotId()),
            row(3, "c", INSERT, 3, snap5.snapshotId()),
            row(4, "d", DELETE, 1, snap3.snapshotId())),
        sql("select * from %s order by id, _change_ordinal", viewName));
  }

  @TestTemplate
  public void testNetChangesComputeUpdatesInsertThenDelete() {
    createTableWithIdentifierField();

    // Snapshot 1: INSERT (1, 'a'), (2, 'b')
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: DELETE (1, 'a'), INSERT (3, 'c')
    sql("INSERT OVERWRITE %s VALUES (2, 'b'), (3, 'c')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: DELETE (3, 'c')
    sql("INSERT OVERWRITE %s VALUES (2, 'b')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id','%s'), "
                + "identifier_columns => array('id'), "
                + "net_changes => true)",
            catalogName, tableName, snap1.snapshotId());

    String viewName = (String) returns.get(0)[0];

    // Expected: Net result from start-snapshot-id (snap1) onwards
    // Row 1: (1, 'a') in snap1 deleted in snap2 = DELETE
    // Row 2: (2, 'b') in snap1 carried over to snap3 = INSERT (no net change)
    // Row 3: (3, 'c') inserted in snap2, deleted in snap3 = no-op (filtered)
    assertEquals(
        "Rows should match",
        ImmutableList.of(row(1, "a", DELETE, 0, snap2.snapshotId())),
        sql("select * from %s order by id, _change_ordinal", viewName));
  }

  @TestTemplate
  public void testNetChangesComputeUpdatesMultipleUpdates() {
    createTableWithIdentifierField();
    // Tests different record ranges for netting updates

    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (1, 'a2'), (2, 'b'), (3, 'c')", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (1, 'a3'), (2, 'b2'), (3, 'c2')", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (1, 'a4'), (2, 'b3')", tableName);
    table.refresh();
    Snapshot snap4 = table.currentSnapshot();

    sql("INSERT OVERWRITE %s VALUES (1, 'a4'), (2, 'b4'), (3, 'c4')", tableName);
    table.refresh();
    Snapshot snap5 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id','%s'), "
                + "identifier_columns => array('id'), "
                + "net_changes => true)",
            catalogName, tableName, snap1.snapshotId());

    String viewName = (String) returns.get(0)[0];

    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", UPDATE_BEFORE, 0, snap2.snapshotId()),
            row(1, "a4", UPDATE_AFTER, 2, snap4.snapshotId()),
            row(2, "b", UPDATE_BEFORE, 1, snap3.snapshotId()),
            row(2, "b4", UPDATE_AFTER, 3, snap5.snapshotId()),
            row(3, "c4", INSERT, 3, snap5.snapshotId())),
        sql("select * from %s order by id, _change_ordinal", viewName));
  }

  @TestTemplate
  public void testNetChangesComputeUpdatesMultipleIdentifiers() {
    createTableWithThreeColumns();

    // Snapshot 1: INSERT multiple rows
    sql("INSERT INTO %s VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)", tableName);
    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot snap1 = table.currentSnapshot();

    // Snapshot 2: Complex changes - update some, delete some, insert new
    sql("INSERT OVERWRITE %s VALUES (1, 'a_new', 11), (3, 'c', 29), (4, 'c', 40)", tableName);
    table.refresh();
    Snapshot snap2 = table.currentSnapshot();

    // Snapshot 3: More changes
    sql("INSERT OVERWRITE %s VALUES (1, 'a_new', 12), (3, 'c', 30)", tableName);
    table.refresh();
    Snapshot snap3 = table.currentSnapshot();

    List<Object[]> returns =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id','%s'), "
                + "identifier_columns => array('id', 'data'), "
                + "net_changes => true)",
            catalogName, tableName, snap1.snapshotId());

    String viewName = (String) returns.get(0)[0];

    // Expected: Net result from start-snapshot-id (snap1) onwards across snap2-snap3
    // Row 1: (1, 'a', 10) in snap1 -> (1, 'a_new', 11) in snap3 = UPDATE_BEFORE + UPDATE_AFTER
    // Row 2: (2, 'b', 20) in snap1 deleted in snap2 = DELETE
    // Row 3: (3, 'c', 30) in snap1 carried to snap2, deleted in snap3 = DELETE
    // Row 4: (4, 'd', 40) inserted in snap2, deleted in snap3 = no-op (filtered)
    // Row 5: (5, 'e', 50) inserted in snap3 = INSERT
    assertEquals(
        "Rows should match",
        ImmutableList.of(
            row(1, "a", 10, DELETE, 0, snap2.snapshotId()),
            row(1, "a_new", 12, INSERT, 1, snap3.snapshotId()),
            row(3, "c", 30, UPDATE_BEFORE, 0, snap2.snapshotId()),
            row(3, "c", 30, UPDATE_AFTER, 1, snap3.snapshotId()),
            row(4, "c", 40, INSERT, 0, snap2.snapshotId())),
        sql("select * from %s order by id, _change_ordinal", viewName));
  }

  @TestTemplate
  public void testUpdateWithInComparableType() {
    sql(
        "CREATE TABLE %s (id INT NOT NULL, data MAP<STRING,STRING>, age INT) USING iceberg",
        tableName);

    assertThatThrownBy(
            () ->
                sql("CALL %s.system.create_changelog_view(table => '%s')", catalogName, tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Identifier field is required as table contains unorderable columns: [data]");
  }
}
