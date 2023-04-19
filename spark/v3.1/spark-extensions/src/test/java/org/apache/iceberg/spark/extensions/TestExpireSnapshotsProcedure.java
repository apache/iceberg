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

import static org.apache.iceberg.TableProperties.GC_ENABLED;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class TestExpireSnapshotsProcedure extends SparkExtensionsTestBase {

  public TestExpireSnapshotsProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testExpireSnapshotsInEmptyTable() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    List<Object[]> output = sql("CALL %s.system.expire_snapshots('%s')", catalogName, tableIdent);
    assertEquals("Should not delete any files", ImmutableList.of(row(0L, 0L, 0L)), output);
  }

  @Test
  public void testExpireSnapshotsUsingPositionalArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Snapshot firstSnapshot = table.currentSnapshot();

    waitUntilAfter(firstSnapshot.timestampMillis());

    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    table.refresh();

    Snapshot secondSnapshot = table.currentSnapshot();
    Timestamp secondSnapshotTimestamp =
        Timestamp.from(Instant.ofEpochMilli(secondSnapshot.timestampMillis()));

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    // expire without retainLast param
    List<Object[]> output1 =
        sql(
            "CALL %s.system.expire_snapshots('%s', TIMESTAMP '%s')",
            catalogName, tableIdent, secondSnapshotTimestamp);
    assertEquals("Procedure output must match", ImmutableList.of(row(0L, 0L, 1L)), output1);

    table.refresh();

    Assert.assertEquals("Should expire one snapshot", 1, Iterables.size(table.snapshots()));

    sql("INSERT OVERWRITE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);
    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(3L, "c"), row(4L, "d")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    table.refresh();

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    Assert.assertEquals("Should be 3 snapshots", 3, Iterables.size(table.snapshots()));

    // expire with retainLast param
    List<Object[]> output =
        sql(
            "CALL %s.system.expire_snapshots('%s', TIMESTAMP '%s', 2)",
            catalogName, tableIdent, currentTimestamp);
    assertEquals("Procedure output must match", ImmutableList.of(row(2L, 2L, 1L)), output);
  }

  @Test
  public void testExpireSnapshotUsingNamedArgs() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    List<Object[]> output =
        sql(
            "CALL %s.system.expire_snapshots("
                + "older_than => TIMESTAMP '%s',"
                + "table => '%s',"
                + "retain_last => 1)",
            catalogName, currentTimestamp, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0L, 0L, 1L)), output);
  }

  @Test
  public void testExpireSnapshotsGCDisabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, GC_ENABLED);

    AssertHelpers.assertThrows(
        "Should reject call",
        ValidationException.class,
        "Cannot expire snapshots: GC is disabled",
        () -> sql("CALL %s.system.expire_snapshots('%s')", catalogName, tableIdent));
  }

  @Test
  public void testInvalidExpireSnapshotsCases() {
    AssertHelpers.assertThrows(
        "Should not allow mixed args",
        AnalysisException.class,
        "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.expire_snapshots('n', table => 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class,
        "not found",
        () -> sql("CALL %s.custom.expire_snapshots('n', 't')", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls without all required args",
        AnalysisException.class,
        "Missing required parameters",
        () -> sql("CALL %s.system.expire_snapshots()", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with invalid arg types",
        AnalysisException.class,
        "Wrong arg type",
        () -> sql("CALL %s.system.expire_snapshots('n', 2.2)", catalogName));

    AssertHelpers.assertThrows(
        "Should reject calls with empty table identifier",
        IllegalArgumentException.class,
        "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.expire_snapshots('')", catalogName));
  }

  @Test
  public void testResolvingTableInAnotherCatalog() throws IOException {
    String anotherCatalog = "another_" + catalogName;
    spark.conf().set("spark.sql.catalog." + anotherCatalog, SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog." + anotherCatalog + ".type", "hadoop");
    spark
        .conf()
        .set(
            "spark.sql.catalog." + anotherCatalog + ".warehouse",
            "file:" + temp.newFolder().toString());

    sql(
        "CREATE TABLE %s.%s (id bigint NOT NULL, data string) USING iceberg",
        anotherCatalog, tableIdent);

    AssertHelpers.assertThrows(
        "Should reject calls for a table in another catalog",
        IllegalArgumentException.class,
        "Cannot run procedure in catalog",
        () ->
            sql(
                "CALL %s.system.expire_snapshots('%s')",
                catalogName, anotherCatalog + "." + tableName));
  }

  @Test
  public void testConcurrentExpireSnapshots() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'd')", tableName);

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));
    List<Object[]> output =
        sql(
            "CALL %s.system.expire_snapshots("
                + "older_than => TIMESTAMP '%s',"
                + "table => '%s',"
                + "max_concurrent_deletes => %s,"
                + "retain_last => 1)",
            catalogName, currentTimestamp, tableIdent, 4);
    assertEquals(
        "Expiring snapshots concurrently should succeed",
        ImmutableList.of(row(0L, 0L, 3L)),
        output);
  }

  @Test
  public void testConcurrentExpireSnapshotsWithInvalidInput() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    AssertHelpers.assertThrows(
        "Should throw an error when max_concurrent_deletes = 0",
        IllegalArgumentException.class,
        "max_concurrent_deletes should have value > 0",
        () ->
            sql(
                "CALL %s.system.expire_snapshots(table => '%s', max_concurrent_deletes => %s)",
                catalogName, tableIdent, 0));

    AssertHelpers.assertThrows(
        "Should throw an error when max_concurrent_deletes < 0 ",
        IllegalArgumentException.class,
        "max_concurrent_deletes should have value > 0",
        () ->
            sql(
                "CALL %s.system.expire_snapshots(table => '%s', max_concurrent_deletes => %s)",
                catalogName, tableIdent, -1));
  }

  @Test
  public void testExpireSnapshotWithStreamResultsEnabled() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    List<Object[]> output =
        sql(
            "CALL %s.system.expire_snapshots("
                + "older_than => TIMESTAMP '%s',"
                + "table => '%s',"
                + "retain_last => 1, "
                + "stream_results => true)",
            catalogName, currentTimestamp, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0L, 0L, 1L)), output);
  }

  @Test
  public void testExpireSnapshotsWithSnapshotId() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    // Expiring the snapshot specified by snapshot_id should keep only a single snapshot.
    long firstSnapshotId = table.currentSnapshot().parentId();
    sql(
        "CALL %s.system.expire_snapshots(" + "table => '%s'," + "snapshot_ids => ARRAY(%d))",
        catalogName, tableIdent, firstSnapshotId);

    // There should only be one single snapshot left.
    table.refresh();
    Assert.assertEquals("Should be 1 snapshots", 1, Iterables.size(table.snapshots()));
    Assert.assertEquals(
        "Snapshot ID should not be present",
        0,
        Iterables.size(
            Iterables.filter(
                table.snapshots(), snapshot -> snapshot.snapshotId() == firstSnapshotId)));
  }

  @Test
  public void testExpireSnapshotShouldFailForCurrentSnapshot() {
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    AssertHelpers.assertThrows(
        "Should reject call",
        IllegalArgumentException.class,
        "Cannot expire",
        () ->
            sql(
                "CALL %s.system.expire_snapshots("
                    + "table => '%s',"
                    + "snapshot_ids => ARRAY(%d, %d))",
                catalogName,
                tableIdent,
                table.currentSnapshot().snapshotId(),
                table.currentSnapshot().parentId()));
  }

  @Test
  public void testExpireSnapshotsProcedureWorksWithSqlComments() {
    // Ensure that systems such as dbt, that add leading comments into the generated SQL commands,
    // will
    // work with Iceberg-specific DDL
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING iceberg", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'b')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    Assert.assertEquals("Should be 2 snapshots", 2, Iterables.size(table.snapshots()));

    waitUntilAfter(table.currentSnapshot().timestampMillis());

    Timestamp currentTimestamp = Timestamp.from(Instant.ofEpochMilli(System.currentTimeMillis()));

    String callStatement =
        "/* CALL statement is used to expire snapshots */\n"
            + "-- And we have single line comments as well \n"
            + "/* And comments that span *multiple* \n"
            + " lines */ CALL /* this is the actual CALL */ %s.system.expire_snapshots("
            + "   older_than => TIMESTAMP '%s',"
            + "   table => '%s',"
            + "   retain_last => 1)";
    List<Object[]> output = sql(callStatement, catalogName, currentTimestamp, tableIdent);
    assertEquals("Procedure output must match", ImmutableList.of(row(0L, 0L, 1L)), output);

    table.refresh();

    Assert.assertEquals("Should be 1 snapshot remaining", 1, Iterables.size(table.snapshots()));
  }
}
