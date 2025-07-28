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
package org.apache.iceberg.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.Properties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;
import org.sqlite.SQLiteDataSource;

public class TestJdbcUtil {

  private static final String CATALOG_NAME = "TEST";
  private static final String NAMESPACE1 = "namespace1";
  private static final Namespace TEST_NAMESPACE = Namespace.of(NAMESPACE1);
  private static final String OLD_LOCATION = "testLocation";
  private static final String NEW_LOCATION = "newLocation";

  private String createTempDatabase() throws Exception {
    java.nio.file.Path dbFile = Files.createTempFile("icebergSchemaUpdate", "db");
    return "jdbc:sqlite:" + dbFile.toAbsolutePath();
  }

  private JdbcClientPool setupConnectionPool(String jdbcUrl) {
    return new JdbcClientPool(jdbcUrl, Maps.newHashMap());
  }

  // Initializes the database with V0 schema and creates sample tables.
  private void setupV0SchemaWithTables(JdbcClientPool connections) throws Exception {
    connections.newClient().prepareStatement(JdbcUtil.V0_CREATE_CATALOG_SQL).executeUpdate();

    // inserting tables
    JdbcUtil.doCommitCreateTable(
        JdbcUtil.SchemaVersion.V0,
        connections,
        CATALOG_NAME,
        TEST_NAMESPACE,
        TableIdentifier.of(TEST_NAMESPACE, "table1"),
        OLD_LOCATION);
    JdbcUtil.doCommitCreateTable(
        JdbcUtil.SchemaVersion.V0,
        connections,
        CATALOG_NAME,
        TEST_NAMESPACE,
        TableIdentifier.of(TEST_NAMESPACE, "table2"),
        OLD_LOCATION);
  }

  private void upgradeSchemaV0ToV1(JdbcClientPool connections) throws Exception {
    connections.newClient().prepareStatement(JdbcUtil.V1_UPDATE_CATALOG_SQL).execute();
  }

  /** Verifies that the expected tables exist in the database with the correct metadata. */
  private void verifyTablesExist(JdbcClientPool connections, String... expectedTableNames)
      throws Exception {
    try (PreparedStatement statement =
        connections.newClient().prepareStatement(JdbcUtil.V0_LIST_TABLE_SQL)) {
      statement.setString(1, CATALOG_NAME);
      statement.setString(2, NAMESPACE1);
      ResultSet tables = statement.executeQuery();

      for (String expectedTableName : expectedTableNames) {
        assertThat(tables.next()).isTrue();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo(expectedTableName);
      }
    }
  }

  @Test
  public void testFilterAndRemovePrefix() {
    Map<String, String> input = Maps.newHashMap();
    input.put("warehouse", "/tmp/warehouse");
    input.put("user", "foo");
    input.put("jdbc.user", "bar");
    input.put("jdbc.pass", "secret");
    input.put("jdbc.jdbc.abcxyz", "abcxyz");

    Properties expected = new Properties();
    expected.put("user", "bar");
    expected.put("pass", "secret");
    expected.put("jdbc.abcxyz", "abcxyz");

    Properties actual = JdbcUtil.filterAndRemovePrefix(input, "jdbc.");

    assertThat(actual).isEqualTo(expected);
  }

  @Test
  public void testV0toV1SqlStatements() throws Exception {
    String jdbcUrl = createTempDatabase();
    SQLiteDataSource dataSource = new SQLiteDataSource();
    dataSource.setUrl(jdbcUrl);

    try (JdbcClientPool connections = setupConnectionPool(jdbcUrl)) {
      // create "old style" SQL schema
      setupV0SchemaWithTables(connections);

      // Verify tables exist
      verifyTablesExist(connections, "table1", "table2");

      // updating the schema from V0 to V1
      upgradeSchemaV0ToV1(connections);

      // trying to add a table on the updated schema
      JdbcUtil.doCommitCreateTable(
          JdbcUtil.SchemaVersion.V1,
          connections,
          CATALOG_NAME,
          TEST_NAMESPACE,
          TableIdentifier.of(TEST_NAMESPACE, "table3"),
          OLD_LOCATION);

      // testing the tables after migration and new table added
      try (PreparedStatement statement =
          connections.newClient().prepareStatement(JdbcUtil.V0_LIST_TABLE_SQL)) {
        statement.setString(1, CATALOG_NAME);
        statement.setString(2, NAMESPACE1);
        ResultSet tables = statement.executeQuery();
        tables.next();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo("table1");
        assertThat(tables.getString(JdbcUtil.RECORD_TYPE)).isNull();
        tables.next();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo("table2");
        assertThat(tables.getString(JdbcUtil.RECORD_TYPE)).isNull();
        tables.next();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo("table3");
        assertThat(tables.getString(JdbcUtil.RECORD_TYPE)).isEqualTo(JdbcUtil.TABLE_RECORD_TYPE);
      }

      // update a table (commit) created on V1 schema
      int updated =
          JdbcUtil.updateTable(
              JdbcUtil.SchemaVersion.V1,
              connections,
              CATALOG_NAME,
              TableIdentifier.of(TEST_NAMESPACE, "table3"),
              NEW_LOCATION,
              OLD_LOCATION);
      assertThat(updated).isEqualTo(1);

      // update a table (commit) migrated from V0 schema
      updated =
          JdbcUtil.updateTable(
              JdbcUtil.SchemaVersion.V1,
              connections,
              CATALOG_NAME,
              TableIdentifier.of(TEST_NAMESPACE, "table1"),
              NEW_LOCATION,
              OLD_LOCATION);
      assertThat(updated).isEqualTo(1);
    }
  }

  @Test
  public void testSetMetadataLocationTable() throws Exception {
    String jdbcUrl = createTempDatabase();

    try (JdbcClientPool connections = setupConnectionPool(jdbcUrl)) {
      // Test with V0 schema
      setupV0SchemaWithTables(connections);

      TableIdentifier table1 = TableIdentifier.of(TEST_NAMESPACE, "table1");
      TableIdentifier table2 = TableIdentifier.of(TEST_NAMESPACE, "table2");

      // Test setMetadataLocationTable with V0 schema
      int updated =
          JdbcUtil.setMetadataLocationTable(
              JdbcUtil.SchemaVersion.V0, connections, CATALOG_NAME, table1, NEW_LOCATION);
      assertThat(updated).isEqualTo(1);

      // Verify the metadata location was updated
      Map<String, String> tableData =
          JdbcUtil.loadTable(JdbcUtil.SchemaVersion.V0, connections, CATALOG_NAME, table1);
      assertThat(tableData.get("metadata_location")).isEqualTo(NEW_LOCATION);

      // Upgrade to V1 schema
      upgradeSchemaV0ToV1(connections);

      // Add a new table in V1 schema
      TableIdentifier table3 = TableIdentifier.of(TEST_NAMESPACE, "table3");
      JdbcUtil.doCommitCreateTable(
          JdbcUtil.SchemaVersion.V1,
          connections,
          CATALOG_NAME,
          TEST_NAMESPACE,
          table3,
          OLD_LOCATION);

      // Test setMetadataLocationTable with V1 schema
      String anotherNewLocation = "anotherNewLocation";
      updated =
          JdbcUtil.setMetadataLocationTable(
              JdbcUtil.SchemaVersion.V1, connections, CATALOG_NAME, table3, anotherNewLocation);
      assertThat(updated).isEqualTo(1);

      // Verify the metadata location was updated for V1 table
      tableData = JdbcUtil.loadTable(JdbcUtil.SchemaVersion.V1, connections, CATALOG_NAME, table3);
      assertThat(tableData.get("metadata_location")).isEqualTo(anotherNewLocation);

      // Test setMetadataLocationTable on a migrated table (table2) with V1 schema
      updated =
          JdbcUtil.setMetadataLocationTable(
              JdbcUtil.SchemaVersion.V1, connections, CATALOG_NAME, table2, anotherNewLocation);
      assertThat(updated).isEqualTo(1);

      // Verify the metadata location was updated for migrated table
      tableData = JdbcUtil.loadTable(JdbcUtil.SchemaVersion.V1, connections, CATALOG_NAME, table2);
      assertThat(tableData.get("metadata_location")).isEqualTo(anotherNewLocation);

      // Test set metadata location for non-existent table
      TableIdentifier nonExistentTable = TableIdentifier.of(TEST_NAMESPACE, "nonexistent");
      updated =
          JdbcUtil.setMetadataLocationTable(
              JdbcUtil.SchemaVersion.V1,
              connections,
              CATALOG_NAME,
              nonExistentTable,
              "someLocation");
      assertThat(updated).isEqualTo(0);
    }
  }

  @Test
  public void emptyNamespaceInIdentifier() {
    assertThat(JdbcUtil.stringToTableIdentifier("", "tblName"))
        .isEqualTo(TableIdentifier.of(Namespace.empty(), "tblName"));
  }
}
