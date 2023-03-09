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
package org.apache.iceberg.snowflake;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.EnvironmentUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

@SuppressWarnings("VisibilityModifier")
class SnowTestBase {

  static SnowflakeCatalog snowflakeCatalog;

  static JdbcClientPool clientPool;

  static Map<String, String> catalogProperties;

  protected SnowTestBase() {}

  @BeforeAll
  public static void beforeAll() {
    snowflakeCatalog = new SnowflakeCatalog();
    Map<String, String> props = Maps.newHashMap();
    // add catalog properties
    props.put(CatalogProperties.URI, "env:" + "SNOW_URI");

    // add jdbc properties, env: prefix is added to annotate that these variables should be
    // resolved by EnvironmentUtil.
    props.put("jdbc." + CatalogProperties.URI, "env:" + "SNOW_URI");
    props.put("jdbc." + CatalogProperties.USER, "env:" + "SNOW_USER");
    props.put("jdbc.password", "env:" + "SNOW_PASSWORD");
    props.put("jdbc.database", "env:" + "SNOW_TEST_DB_NAME");

    catalogProperties = EnvironmentUtil.resolveAll(props);

    // Check required arguments for test
    Preconditions.checkNotNull(
        getURI(),
        "URI is required argument and should be resolved from environment variable SNOW_URI");
    // Ensure that environment variable resolution was attempted and the variable was resolved.
    Preconditions.checkArgument(
        !getURI().isEmpty() && !getURI().contains("env"),
        "URI is required argument and should be resolved from environment variable SNOW_URI");
    Preconditions.checkNotNull(
        getDatabase(),
        "Database is required argument, please set environment variable SNOW_TEST_DB_NAME");

    snowflakeCatalog.initialize("testCatalog", Maps.newHashMap(getCatalogProperties()));
    clientPool = new JdbcClientPool(getURI(), getCatalogProperties());
    try {
      createOrReplaceDatabase(getDatabase());
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static String getURI() {
    return catalogProperties.get(CatalogProperties.URI);
  }

  static String getUser() {
    return catalogProperties.get("jdbc." + CatalogProperties.USER);
  }

  static String getDatabase() {
    return catalogProperties.get("jdbc.database");
  }

  static Map<String, String> getCatalogProperties() {
    return catalogProperties;
  }

  @AfterAll
  public static void afterAll() {
    try {
      dropDatabaseIfExists(getDatabase());
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  static void createOrReplaceSchema(String schemaName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
          PreparedStatement statement =
              conn.prepareStatement("CREATE OR REPLACE SCHEMA IDENTIFIER(?)");
          statement.setString(1, schemaName);
          return statement.execute();
        });
  }

  static void dropSchemaIfExists(String schemaName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
          PreparedStatement statement =
              conn.prepareStatement("DROP SCHEMA IF EXISTS IDENTIFIER(?)");
          statement.setString(1, schemaName);
          return statement.execute();
        });
  }

  static void createOrReplaceDatabase(String dbName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
          PreparedStatement statement =
              conn.prepareStatement("CREATE OR REPLACE DATABASE IDENTIFIER(?)");
          statement.setString(1, dbName);
          return statement.execute();
        });
  }

  static void dropDatabaseIfExists(String dbName) throws SQLException, InterruptedException {
    clientPool.run(
        conn -> {
          PreparedStatement statement =
              conn.prepareStatement("DROP DATABASE IF EXISTS IDENTIFIER(?)");
          statement.setString(1, dbName);
          return statement.execute();
        });
  }
}
