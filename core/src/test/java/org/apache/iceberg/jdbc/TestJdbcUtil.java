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

    assertThat(expected).isEqualTo(actual);
  }

  @Test
  public void testV0toV1SqlStatements() throws Exception {
    java.nio.file.Path dbFile = Files.createTempFile("icebergSchemaUpdate", "db");
    String jdbcUrl = "jdbc:sqlite:" + dbFile.toAbsolutePath();

    SQLiteDataSource dataSource = new SQLiteDataSource();
    dataSource.setUrl(jdbcUrl);

    try (JdbcClientPool connections = new JdbcClientPool(jdbcUrl, Maps.newHashMap())) {
      // create "old style" SQL schema
      connections.newClient().prepareStatement(JdbcUtil.V0_CREATE_CATALOG_SQL).executeUpdate();

      // inserting tables
      JdbcUtil.doCommitCreateTable(
          JdbcUtil.SchemaVersion.V0,
          connections,
          "TEST",
          Namespace.of("namespace1"),
          TableIdentifier.of(Namespace.of("namespace1"), "table1"),
          "testLocation");
      JdbcUtil.doCommitCreateTable(
          JdbcUtil.SchemaVersion.V0,
          connections,
          "TEST",
          Namespace.of("namespace1"),
          TableIdentifier.of(Namespace.of("namespace1"), "table2"),
          "testLocation");

      try (PreparedStatement statement =
          connections.newClient().prepareStatement(JdbcUtil.V0_LIST_TABLE_SQL)) {
        statement.setString(1, "TEST");
        statement.setString(2, "namespace1");
        ResultSet tables = statement.executeQuery();
        tables.next();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo("table1");
        tables.next();
        assertThat(tables.getString(JdbcUtil.TABLE_NAME)).isEqualTo("table2");
      }

      // updating the schema from V0 to V1
      connections.newClient().prepareStatement(JdbcUtil.V1_UPDATE_CATALOG_SQL).execute();

      // trying to add a table on the updated schema
      JdbcUtil.doCommitCreateTable(
          JdbcUtil.SchemaVersion.V1,
          connections,
          "TEST",
          Namespace.of("namespace1"),
          TableIdentifier.of(Namespace.of("namespace1"), "table3"),
          "testLocation");

      // testing the tables after migration and new table added
      try (PreparedStatement statement =
          connections.newClient().prepareStatement(JdbcUtil.V0_LIST_TABLE_SQL)) {
        statement.setString(1, "TEST");
        statement.setString(2, "namespace1");
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
              "TEST",
              TableIdentifier.of(Namespace.of("namespace1"), "table3"),
              "newLocation",
              "testLocation");
      assertThat(updated).isEqualTo(1);

      // update a table (commit) migrated from V0 schema
      updated =
          JdbcUtil.updateTable(
              JdbcUtil.SchemaVersion.V1,
              connections,
              "TEST",
              TableIdentifier.of(Namespace.of("namespace1"), "table1"),
              "newLocation",
              "testLocation");
      assertThat(updated).isEqualTo(1);
    }
  }
}
