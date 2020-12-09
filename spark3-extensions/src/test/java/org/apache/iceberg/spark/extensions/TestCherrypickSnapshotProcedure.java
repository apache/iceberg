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
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Test;

import static org.apache.iceberg.TableProperties.WRITE_AUDIT_PUBLISH_ENABLED;

public class TestCherrypickSnapshotProcedure extends SparkExtensionsTestBase {

  public TestCherrypickSnapshotProcedure(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testCherrypickSnapshotUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output = sql(
        "CALL %s.system.cherrypick_snapshot('%s', %dL)",
        catalogName, tableIdent, wapSnapshot.snapshotId());

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    assertEquals("Procedure output must match",
        ImmutableList.of(row(wapSnapshot.snapshotId(), currentSnapshot.snapshotId())),
        output);

    assertEquals("Cherrypick must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testCherrypickSnapshotUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    List<Object[]> output = sql(
        "CALL %s.system.cherrypick_snapshot(snapshot_id => %dL, table => '%s')",
        catalogName, wapSnapshot.snapshotId(), tableIdent);

    table.refresh();

    Snapshot currentSnapshot = table.currentSnapshot();

    assertEquals("Procedure output must match",
        ImmutableList.of(row(wapSnapshot.snapshotId(), currentSnapshot.snapshotId())),
        output);

    assertEquals("Cherrypick must be successful",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testCherrypickSnapshotRefreshesRelationCache() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'true')", tableName, WRITE_AUDIT_PUBLISH_ENABLED);

    Dataset<Row> query = spark.sql("SELECT * FROM " + tableName + " WHERE id = 1");
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals("View should not produce rows", ImmutableList.of(), sql("SELECT * FROM tmp"));

    spark.conf().set("spark.wap.id", "1");

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should not see rows from staged snapshot",
        ImmutableList.of(),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot wapSnapshot = Iterables.getOnlyElement(table.snapshots());

    sql("CALL %s.system.cherrypick_snapshot('%s', %dL)",
        catalogName, tableIdent, wapSnapshot.snapshotId());

    assertEquals("Cherrypick snapshot should be visible",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM tmp"));

    sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testCherrypickInvalidSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    AssertHelpers.assertThrows("Should reject invalid snapshot id",
        ValidationException.class, "Cannot cherry pick unknown snapshot id",
        () -> sql("CALL %s.system.cherrypick_snapshot('%s', -1L)", catalogName, tableIdent));
  }

  @Test
  public void testInvalidCherrypickSnapshotCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.cherrypick_snapshot('n', table => 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.cherrypick_snapshot('n', 't', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.cherrypick_snapshot('t')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.cherrypick_snapshot('', 1L)", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type for snapshot_id: cannot cast",
        () -> sql("CALL %s.system.cherrypick_snapshot('t', 2.2)", catalogName));
  }
}
