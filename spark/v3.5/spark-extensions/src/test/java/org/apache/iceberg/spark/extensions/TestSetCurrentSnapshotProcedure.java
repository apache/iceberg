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

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assume;
import org.junit.Test;

public class TestSetCurrentSnapshotProcedure extends SparkExtensionsTestBase {

  public TestSetCurrentSnapshotProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSetCurrentSnapshotUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output =
        sql(
            "CALL %s.system.set_current_snapshot('%s', %dL)",
            catalogName, tableIdent, firstSnapshot.snapshotId());

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Set must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSetCurrentSnapshotUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output =
        sql(
            "CALL %s.system.set_current_snapshot(snapshot_id => %dL, table => '%s')",
            catalogName, firstSnapshot.snapshotId(), tableIdent);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Set must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSetCurrentSnapshotWap() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output =
        sql(
            "CALL %s.system.set_current_snapshot(table => '%s', snapshot_id => %dL)",
            catalogName, tableIdent, wapSnapshot.snapshotId());

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(null, wapSnapshot.snapshotId())),
        output);

    assertEquals(
        "Current snapshot must be set correctly",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void tesSetCurrentSnapshotWithoutExplicitCatalog() {
    Assume.assumeTrue("Working only with the session catalog", "spark_catalog".equals(catalogName));

    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    // use camel case intentionally to test case sensitivity
    List<Object[]> output =
        sql("CALL SyStEm.sEt_cuRrEnT_sNaPsHot('%s', %dL)", tableIdent, firstSnapshot.snapshotId());

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Set must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSetCurrentSnapshotToInvalidSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot('%s', -1L)", catalogName, tableIdent))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot roll back to unknown snapshot id: -1");
  }

  @Test
  public void testInvalidRollbackToSnapshotCases() {
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.set_current_snapshot(namespace => 'n1', table => 't', 1L)",
                    catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Named and positional arguments cannot be mixed");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.custom.set_current_snapshot('n', 't', 1L)", catalogName))
        .isInstanceOf(NoSuchProcedureException.class)
        .hasMessage("Procedure custom.set_current_snapshot not found");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot('t')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Either snapshot_id or ref must be provided, not both");

    Assertions.assertThatThrownBy(() -> sql("CALL %s.system.set_current_snapshot(1L)", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse identifier for arg table: 1");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot(snapshot_id => 1L)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot(table => 't')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Either snapshot_id or ref must be provided, not both");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot('t', 2.2)", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Wrong arg type for snapshot_id: cannot cast DecimalType(2,1) to LongType");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.set_current_snapshot('', 1L)", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");

    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.set_current_snapshot(table => 't', snapshot_id => 1L, ref => 's1')",
                    catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Either snapshot_id or ref must be provided, not both");
  }

  @Test
  public void testSetCurrentSnapshotToRef() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();
    String ref = "s1";
    sql("ALTER TABLE %s CREATE TAG %s", tableName, ref);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();

    List<Object[]> output =
        sql(
            "CALL %s.system.set_current_snapshot(table => '%s', ref => '%s')",
            catalogName, tableIdent, ref);

    assertEquals(
        "Procedure output must match",
        ImmutableList.of(row(secondSnapshot.snapshotId(), firstSnapshot.snapshotId())),
        output);

    assertEquals(
        "Set must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    String notExistRef = "s2";
    Assertions.assertThatThrownBy(
            () ->
                sql(
                    "CALL %s.system.set_current_snapshot(table => '%s', ref => '%s')",
                    catalogName, tableIdent, notExistRef))
        .isInstanceOf(ValidationException.class)
        .hasMessage("Cannot find matching snapshot ID for ref " + notExistRef);
  }
}
