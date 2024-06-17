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

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

public class TestRollbackToTimestampProcedure extends SparkExtensionsTestBase {

  public TestRollbackToTimestampProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
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

  @Test
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

  @Test
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

  @Test
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

  @Test
  public void testRollbackToTimestampWithoutExplicitCatalog() {
    Assume.assumeTrue("Working only with the session catalog", "spark_catalog".equals(catalogName));

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

  @Test
  public void testInvalidRollbackToTimestampCases() {
    String timestamp = "TIMESTAMP '2007-12-03T10:15:30'";

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(namespace => 'n1', 't', %s)",
                    catalogName, timestamp))
        .as("Should not allow mixed args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Named and positional arguments cannot be mixed");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.custom.rollback_to_timestamp('n', 't', %s)", catalogName, timestamp))
        .as("Should not resolve procedures in arbitrary namespaces")
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessageContaining("not found");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rollback_to_timestamp('t')", catalogName))
        .as("Should reject calls without all required args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp(timestamp => %s)",
                    catalogName, timestamp))
        .as("Should reject calls without all required args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rollback_to_timestamp(table => 't')", catalogName))
        .as("Should reject calls without all required args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Missing required parameters");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.rollback_to_timestamp('n', 't', %s, 1L)",
                    catalogName, timestamp))
        .as("Should reject calls with extra args")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Too many arguments");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.rollback_to_timestamp('t', 2.2)", catalogName))
        .as("Should reject calls with invalid arg types")
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining("Wrong arg type for timestamp: cannot cast");
  }
}
