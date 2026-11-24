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
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.internal.SQLConf;
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

  private static final Map<String, String> CHANGELOG_GROUPING_CONF =
      Map.of(
          SparkSQLProperties.PRESERVE_DATA_GROUPING,
          "true",
          SQLConf.V2_BUCKETING_ENABLED().key(),
          "true",
          SQLConf.V2_BUCKETING_ALLOW_JOIN_KEYS_SUBSET_OF_PARTITION_KEYS().key(),
          "true",
          SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(),
          "false",
          SQLConf.PARTITION_OVERWRITE_MODE().key(),
          "static");

  private static final String UPDATE_OPTIONS =
      "compute_updates => true, identifier_columns => array('id')";

  private static final List<Row> UPDATE_ROWS =
      List.of(
          RowFactory.create(1, "a", UPDATE_BEFORE, 0),
          RowFactory.create(1, "c", UPDATE_AFTER, 0),
          RowFactory.create(1, "c", UPDATE_BEFORE, 1),
          RowFactory.create(1, "d", UPDATE_AFTER, 1));

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

  /** The initial and updated values must encode 0x01 and 0x02 respectively. */
  private void assertBinaryNetChanges(String initial, String updated, String projection) {
    sql("INSERT INTO %s VALUES (1, %s), (2, %s)", tableName, initial, initial);
    sql("INSERT OVERWRITE %s VALUES (1, %s), (2, %s)", tableName, updated, initial);

    List<Object[]> result =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', net_changes => true)",
            catalogName, tableName);
    String viewName = (String) result.get(0)[0];
    assertEquals(
        "Net changes should contain only the final rows",
        ImmutableList.of(row(1, "02", INSERT), row(2, "01", INSERT)),
        sql("SELECT id, %s, _change_type FROM %s ORDER BY id", projection, viewName));
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
  public void testNetChangesWithBinaryValues() {
    sql("CREATE TABLE %s (id INT, data BINARY) USING iceberg", tableName);
    assertBinaryNetChanges("X'01'", "X'02'", "hex(data)");
  }

  @TestTemplate
  public void testNetChangesWithNestedBinaryValues() {
    sql("CREATE TABLE %s (id INT, data ARRAY<ARRAY<BINARY>>) USING iceberg", tableName);
    assertBinaryNetChanges("array(array(X'01'))", "array(array(X'02'))", "hex(data[0][0])");
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
  public void testNetChangesWithComputeUpdates() {
    createTableWithTwoColumns();
    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.create_changelog_view(table => '%s', identifier_columns => array('id'), net_changes => true)",
                    catalogName, tableName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Not support net changes with update images");
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

  @TestTemplate
  void createChangelogView_bucketedUpdatesAvoidShuffle() {
    withSQLConf(
        CHANGELOG_GROUPING_CONF,
        () -> {
          long startSnapshotId = createChangelogHistory("bucket(8, id)");
          assertChangelog(startSnapshotId, UPDATE_OPTIONS, UPDATE_ROWS, 0);
        });
  }

  @TestTemplate
  void createChangelogView_bucketedCarryoversAvoidShuffle() {
    withSQLConf(
        CHANGELOG_GROUPING_CONF,
        () -> {
          long startSnapshotId = createChangelogHistory("bucket(8, id)");
          assertChangelog(
              startSnapshotId,
              "compute_updates => false",
              List.of(
                  RowFactory.create(1, "a", DELETE, 0),
                  RowFactory.create(1, "c", INSERT, 0),
                  RowFactory.create(1, "c", DELETE, 1),
                  RowFactory.create(1, "d", INSERT, 1)),
              0);
        });
  }

  @TestTemplate
  void createChangelogView_bucketedNetChangesAvoidShuffle() {
    withSQLConf(
        CHANGELOG_GROUPING_CONF,
        () -> {
          long startSnapshotId = createChangelogHistory("bucket(8, id)");
          assertChangelog(
              startSnapshotId,
              "net_changes => true",
              List.of(RowFactory.create(1, "a", DELETE, 0), RowFactory.create(1, "d", INSERT, 1)),
              0);
        });
  }

  @TestTemplate
  void createChangelogView_disabledGroupingRetainsShuffle() {
    Map<String, String> conf = Maps.newHashMap(CHANGELOG_GROUPING_CONF);
    conf.put(SparkSQLProperties.PRESERVE_DATA_GROUPING, "false");
    withSQLConf(
        conf,
        () -> {
          long startSnapshotId = createChangelogHistory("bucket(8, id)");
          assertChangelog(startSnapshotId, UPDATE_OPTIONS, UPDATE_ROWS, 1);
        });
  }

  @TestTemplate
  void createChangelogView_mutablePartitionColumnRetainsShuffle() {
    withSQLConf(
        CHANGELOG_GROUPING_CONF,
        () -> {
          long startSnapshotId = createChangelogHistory("bucket(8, id), data");
          assertChangelog(startSnapshotId, UPDATE_OPTIONS, UPDATE_ROWS, 1);
        });
  }

  @TestTemplate
  void createChangelogView_incompatibleBucketsRetainShuffle() {
    withSQLConf(
        CHANGELOG_GROUPING_CONF,
        () -> {
          long startSnapshotId = createInitialChangelogSnapshot("bucket(8, id)");
          sql(
              "ALTER TABLE %s REPLACE PARTITION FIELD bucket(8, id) WITH bucket(16, id)",
              tableName);
          sql("INSERT OVERWRITE %s VALUES (1, 'c'), (2, 'b')", tableName);
          sql("INSERT OVERWRITE %s VALUES (1, 'd'), (2, 'b')", tableName);
          assertChangelog(startSnapshotId, UPDATE_OPTIONS, UPDATE_ROWS, 1);
        });
  }

  private long createChangelogHistory(String partitioning) {
    long startSnapshotId = createInitialChangelogSnapshot(partitioning);
    sql("INSERT OVERWRITE %s VALUES (1, 'c'), (2, 'b')", tableName);
    sql("INSERT OVERWRITE %s VALUES (1, 'd'), (2, 'b')", tableName);
    return startSnapshotId;
  }

  private long createInitialChangelogSnapshot(String partitioning) {
    // Matching rows must cross input partitions to exercise grouping before comparison.
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg PARTITIONED BY (%s) "
            + "TBLPROPERTIES ('%s'='16777216', '%s'='16777216')",
        tableName, partitioning, TableProperties.SPLIT_SIZE, TableProperties.SPLIT_OPEN_FILE_COST);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b')", tableName);
    return validationCatalog.loadTable(tableIdent).currentSnapshot().snapshotId();
  }

  private void assertChangelog(
      long startSnapshotId, String options, List<Row> expectedRows, int expectedShuffles) {
    List<Object[]> result =
        sql(
            "CALL %s.system.create_changelog_view(table => '%s', "
                + "options => map('start-snapshot-id', '%s'), %s)",
            catalogName, tableName, startSnapshotId, options);
    String viewName = (String) result.get(0)[0];
    Dataset<Row> changes =
        spark.sql("SELECT id, data, _change_type, _change_ordinal FROM " + viewName);

    assertThat(changes.collectAsList()).containsExactlyInAnyOrderElementsOf(expectedRows);
    String plan = changes.queryExecution().executedPlan().toString();
    assertThat(StringUtils.countMatches(plan, "Exchange"))
        .as("Number of changelog shuffles in %s", plan)
        .isEqualTo(expectedShuffles);
  }
}
