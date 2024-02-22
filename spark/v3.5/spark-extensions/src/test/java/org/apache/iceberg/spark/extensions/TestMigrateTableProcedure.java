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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.spark.sql.AnalysisException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMigrateTableProcedure extends ExtensionsTestBase {
  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS %s_BACKUP_", tableName);
  }

  @TestTemplate
  public void testMigrate() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    String tableLocation = createdTable.location().replace("file:", "");
    assertThat(tableLocation).as("Table should have original location").isEqualTo(location);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @TestTemplate
  public void testMigrateWithOptions() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('foo', 'bar'))", catalogName, tableName);

    assertThat(result).as("Should have added one file").isEqualTo(1L);

    Table createdTable = validationCatalog.loadTable(tableIdent);

    Map<String, String> props = createdTable.properties();
    assertThat(props).containsEntry("foo", "bar");

    String tableLocation = createdTable.location().replace("file:", "");
    assertThat(tableLocation).as("Table should have original location").isEqualTo(location);

    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a"), row(1L, "a")),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DROP TABLE IF EXISTS %s", tableName + "_BACKUP_");
  }

  @TestTemplate
  public void testMigrateWithDropBackup() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql(
            "CALL %s.system.migrate(table => '%s', drop_backup => true)", catalogName, tableName);
    assertThat(result).as("Should have added one file").isEqualTo(1L);
    assertThat(spark.catalog().tableExists(tableName + "_BACKUP_")).isFalse();
  }

  @TestTemplate
  public void testMigrateWithBackupTableName() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    String backupTableName = "backup_table";
    Object result =
        scalarSql(
            "CALL %s.system.migrate(table => '%s', backup_table_name => '%s')",
            catalogName, tableName, backupTableName);

    assertThat(result).isEqualTo(1L);
    String dbName = tableName.split("\\.")[0];
    assertThat(spark.catalog().tableExists(dbName + "." + backupTableName)).isTrue();
  }

  @TestTemplate
  public void testMigrateWithInvalidMetricsConfig() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");

    String location = temp.toFile().toString();
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

  @TestTemplate
  public void testMigrateWithConflictingProps() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");

    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    sql("INSERT INTO TABLE %s VALUES (1, 'a')", tableName);

    Object result =
        scalarSql("CALL %s.system.migrate('%s', map('migrated', 'false'))", catalogName, tableName);
    assertThat(result).as("Should have added one file").isEqualTo(1L);

    assertEquals(
        "Should have expected rows",
        ImmutableList.of(row(1L, "a")),
        sql("SELECT * FROM %s", tableName));

    Table table = validationCatalog.loadTable(tableIdent);
    assertThat(table.properties()).containsEntry("migrated", "true");
  }

  @TestTemplate
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

  @TestTemplate
  public void testMigratePartitionWithSpecialCharacter() throws IOException {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
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

  @TestTemplate
  public void testMigrateEmptyPartitionedTable() throws Exception {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet PARTITIONED BY (id) LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    assertThat(result).isEqualTo(0L);
  }

  @TestTemplate
  public void testMigrateEmptyTable() throws Exception {
    assumeThat(catalogName).isEqualToIgnoringCase("spark_catalog");
    String location = temp.toFile().toString();
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, data string) USING parquet LOCATION '%s'",
        tableName, location);
    Object result = scalarSql("CALL %s.system.migrate('%s')", catalogName, tableName);
    assertThat(result).isEqualTo(0L);
  }
}
