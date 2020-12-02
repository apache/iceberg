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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.catalyst.analysis.NoSuchProcedureException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestCreateProcedures extends SparkExtensionsTestBase {
  private static final String sourceName = "spark_catalog.default.source";
  // Currently we can only Snapshot only out of the Spark Session Catalog

  public TestCreateProcedures(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %S", sourceName);
  }

  @Test
  public void testMigrate() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Object[] result = sql("CALL %s.system.migrate('%s')", catalogName, tableName).get(0);

    Assert.assertEquals("Should have migrated one file", 1L, result[0]);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMigrateWithOptions() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql("CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Object[] result = sql("CALL %s.system.migrate('%s', map('foo', 'bar'))", catalogName, tableName).get(0);

    Assert.assertEquals("Should have migrated one file", 1L, result[0]);

    Map<String, String> props = validationCatalog.loadTable(tableIdent).properties();
    Assert.assertEquals("Should have extra property set", "bar", props.get("foo"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSnapshot() throws IOException {
    String location = temp.newFolder().toString();
    sql("CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName,
        location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    Object[] result = sql("CALL %s.system.snapshot('%s', '%s')", catalogName, sourceName, tableName).get(0);

    Assert.assertEquals("Should have migrated one file", 1L, result[0]);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSnapshotWithOptions() throws IOException {
    String location = temp.newFolder().toString();
    sql("CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName,
        location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    Object[] result = sql(
        "CALL %s.system.snapshot( snapshot_source => '%s', table => '%s', table_options => map('foo','bar'))",
        catalogName, sourceName, tableName).get(0);

    Assert.assertEquals("Should have migrated one file", 1L, result[0]);

    Map<String, String> props = validationCatalog.loadTable(tableIdent).properties();
    Assert.assertEquals("Should have extra property set", "bar", props.get("foo"));

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testSnapshotWithAlternateLocation() throws IOException {
    Assume.assumeTrue("No Snapshoting with Alternate locations with Hadoop Catalogs", !catalogName.contains("hadoop"));
    String location = temp.newFolder().toString();
    String snapshotLocation = temp.newFolder().toString();
    sql("CREATE TABLE IF NOT EXISTS %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'", sourceName,
        location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", sourceName);
    Object[] result = sql(
        "CALL %s.system.snapshot( snapshot_source => '%s', table => '%s', table_location => '%s')",
        catalogName, sourceName, tableName, snapshotLocation).get(0);

    Assert.assertEquals("Should have migrated one file", 1L, result[0]);

    String storageLocation = validationCatalog.loadTable(tableIdent).location();
    Assert.assertEquals("Snapshot should be made at specified location", snapshotLocation, storageLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals("Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testInvalidSnapshotsCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.snapshot('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.snapshot('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.snapshot('foo')", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type",
        () -> sql("CALL %s.system.snapshot('n', 't', map('foo', 'bar'))", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.snapshot('', '')", catalogName));
  }

  @Test
  public void testInvalidMigrateCases() {
    AssertHelpers.assertThrows("Should not allow mixed args",
        AnalysisException.class, "Named and positional arguments cannot be mixed",
        () -> sql("CALL %s.system.migrate('n', table => 't')", catalogName));

    AssertHelpers.assertThrows("Should not resolve procedures in arbitrary namespaces",
        NoSuchProcedureException.class, "not found",
        () -> sql("CALL %s.custom.migrate('n', 't')", catalogName));

    AssertHelpers.assertThrows("Should reject calls without all required args",
        AnalysisException.class, "Missing required parameters",
        () -> sql("CALL %s.system.migrate()", catalogName));

    AssertHelpers.assertThrows("Should reject calls with invalid arg types",
        AnalysisException.class, "Wrong arg type",
        () -> sql("CALL %s.system.migrate(map('foo','bar'))", catalogName));

    AssertHelpers.assertThrows("Should reject calls with empty table identifier",
        IllegalArgumentException.class, "Cannot handle an empty identifier",
        () -> sql("CALL %s.system.migrate('')", catalogName));
  }

}
