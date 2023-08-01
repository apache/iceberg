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
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMigrateTableProcedure extends SparkExtensionsTestBase {

  public TestMigrateTableProcedure(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
  }

  @Test
  public void testMigrate() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);

    Assert.assertEquals("Should have added one file", 1L, result);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    String tableLocation = createdTable.location().replace("file:", "");
    Assert.assertEquals("Table should have original location", location, tableLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @Test
  public void testMigrateWithOptions() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('foo', 'bar'))", catalogName, tableName);

    Assert.assertEquals("Should have added one file", 1L, result);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    Map<String, String> props = createdTable.properties();
    Assert.assertEquals("Should have extra property set", "bar", props.get("foo"));

    String tableLocation = createdTable.location().replace("file:", "");
    Assert.assertEquals("Table should have original location", location, tableLocation);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @Test
  public void testMigrateWithDropBackup() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql(
            "CALL %s.system.migrate(table => '%s', drop_backup => true)", catalogName, tableName);
    Assert.assertEquals("Should have added one file", 1L, result);
    Assert.assertFalse(spark.catalog().tableExists(tableName + "_BACKUP_"));
  }

  @Test
  public void testMigrateWithInvalidMetricsConfig() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));

    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);

    Assertions.assertThatThrownBy(
            () -> {
              String props = "map('write.metadata.metrics.column.x', 'X')";
              sql("CALL %s.system.migrate('%s', %s)", catalogName, tableName, props);
            })
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith("Invalid metrics config");
  }

  @Test
  public void testMigrateWithConflictingProps() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));

    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('migrated', 'false'))", catalogName, tableName);
    Assert.assertEquals("Should have added one file", 1L, result);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    Assert.assertEquals("Should override user value", "true", table.properties().get("migrated"));
  }

  @Test
  public void testInvalidMigrateCases() {
    Assertions.assertThatThrownBy(() -> sql("CALL %s.system.migrate()", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessage("Missing required parameters: [table]");

    Assertions.assertThatThrownBy(
            () -> sql("CALL %s.system.migrate(map('foo','bar'))", catalogName))
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith("Wrong arg type for table");

    Assertions.assertThatThrownBy(() -> sql("CALL %s.system.migrate('')", catalogName))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot handle an empty identifier for argument table");
  }

  @Test
  public void testMigratePartitionWithSpecialCharacter() throws IOException {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string, dt date) USING parquet "
            + "PARTITIONED BY (data, dt) LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, '2023/05/30', date '2023-05-30')", tableName);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "2023/05/30", java.sql.Date.valueOf("2023-05-30"))),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMigrateEmptyPartitionedTable() throws Exception {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet PARTITIONED BY (id) LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    Assert.assertEquals(0L, result);
  }

  @Test
  public void testMigrateEmptyTable() throws Exception {
    Assume.assumeTrue(catalogName.equals("spark_catalog"));
    String location = temp.newFolder().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    Assert.assertEquals(0L, result);
  }
}
