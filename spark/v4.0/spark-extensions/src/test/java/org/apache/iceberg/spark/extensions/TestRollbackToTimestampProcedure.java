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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestRollbackToTimestampProcedure extends ExtensionsTestBase {

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testRollbackToTimestampUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String firstSnapshotTimestamp = LocalDateTime.now().toString();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output =
        sql(
            "CALL %s.system.rollback_to_timestamp('%s',TIMESTAMP '%s')",
            catalogName, tableIdent, firstSnapshotTimestamp);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRollbackToTimestampUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String firstSnapshotTimestamp = LocalDateTime.now().toString();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output =
        sql(
            "CALL %s.system.rollback_to_timestamp(timestamp => TIMESTAMP '%s', table => '%s')",
            catalogName, firstSnapshotTimestamp, tableIdent);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRollbackToTimestampRefreshesRelationCache() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String firstSnapshotTimestamp = LocalDateTime.now().toString();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals(
        "View should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM tmp"));

    List<Object[]> output =
        sql(
            "CALL %s.system.rollback_to_timestamp(table => '%s', timestamp => TIMESTAMP '%s')",
            catalogName, tableIdent, firstSnapshotTimestamp);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "View cache must be invalidated", ImmutableList.of(row(1L, "a")), sql("SELECT * FROM tmp"));

    sql("UNCACHE TABLE tmp");
  }

  @TestTemplate
  public void testRollbackToTimestampWithQuotedIdentifiers() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String firstSnapshotTimestamp = LocalDateTime.now().toString();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    StringBuilder quotedNamespaceBuilder = new StringBuilder();
    for (String level : tableIdent.namespace().levels()) {
      quotedNamespaceBuilder.append("`");
      quotedNamespaceBuilder.append(level);
      quotedNamespaceBuilder.append("`");
    }
    String quotedNamespace = quotedNamespaceBuilder.toString();

    List<Object[]> output =
        sql(
            "CALL %s.system.rollback_to_timestamp('%s', TIMESTAMP '%s')",
            catalogName, quotedNamespace + ".`" + tableIdent.name() + "`", firstSnapshotTimestamp);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRollbackToTimestampWithoutExplicitCatalog() {
    assumeThat(catalogName).as("Working only with the session catalog").isEqualTo("spark_catalog");

    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String firstSnapshotTimestamp = LocalDateTime.now().toString();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    // use camel case intentionally to test case sensitivity
    List<Object[]> output =
        sql(
            "CALL SyStEm.rOLlBaCk_to_TiMeStaMp('%s', TIMESTAMP '%s')",
            tableIdent, firstSnapshotTimestamp);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Rollback must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testRollbackToTimestampBeforeOrEqualToOldestSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    Timestamp beforeFirstSnapshot =
        Timestamp.from(Instant.ofEpochMilli(firstSnapshot.timestampMillis() - 1));
    Timestamp exactFirstSnapshot =
        Timestamp.from(Instant.ofEpochMilli(firstSnapshot.timestampMillis()));

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(timestamp => TIMESTAMP '%s', table => '%s')",
                    catalogName, beforeFirstSnapshot, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot roll back, no valid snapshot older than: %s",
            beforeFirstSnapshot.toInstant().toEpochMilli());

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(timestamp => TIMESTAMP '%s', table => '%s')",
                    catalogName, exactFirstSnapshot, tableIdent))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot roll back, no valid snapshot older than: %s",
            exactFirstSnapshot.toInstant().toEpochMilli());
  }

  @TestTemplate
  public void testInvalidRollbackToTimestampCases() {
    String timestamp = "TIMESTAMP '2007-12-03T10:15:30'";

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(namespace => 'n1', 't', %s)",
                    catalogName, timestamp))
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[UNEXPECTED_POSITIONAL_ARGUMENT] Cannot invoke routine `rollback_to_timestamp` because it contains positional argument(s) following the named argument assigned to `namespace`; please rearrange them so the positional arguments come first and then retry the query again. SQLSTATE: 4274K");

    assertThatThrownBy(
            () -> sql("CALL %s.custom.rollback_to_timestamp('n', 't', %s)", catalogName, timestamp))
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[FAILED_TO_LOAD_ROUTINE] Failed to load routine `%s`.`custom`.`rollback_to_timestamp`. SQLSTATE: 38000",
            catalogName);

    assertThatThrownBy(() -> sql("CALL %s.system.rollback_to_timestamp('t')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `rollback_to_timestamp` because the parameter named `timestamp` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 0 or by name) and retry the query again. SQLSTATE: 4274K");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(timestamp => %s)",
                    catalogName, timestamp))
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `rollback_to_timestamp` because the parameter named `table` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 0 or by name) and retry the query again. SQLSTATE: 4274K");

    assertThatThrownBy(() -> sql("CALL %s.system.rollback_to_timestamp(table => 't')", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage(
            "[REQUIRED_PARAMETER_NOT_FOUND] Cannot invoke routine `rollback_to_timestamp` because the parameter named `timestamp` is required, but the routine call did not supply a value. Please update the routine call to supply an argument value (either positionally at index 1 or by name) and retry the query again. SQLSTATE: 4274K");

    assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp('n', 't', %s, 1L)",
                    catalogName, timestamp))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `rollback_to_timestamp` requires 2 parameters but the actual number is 4.");

    assertThatThrownBy(() -> sql("CALL %s.system.rollback_to_timestamp('t', 2.2)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "[DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE] Cannot resolve CALL due to data type mismatch: The second parameter requires the \"TIMESTAMP\" type, however \"2.2\" has the type \"DECIMAL(2,1)\". SQLSTATE: 42K09");
  }
}
