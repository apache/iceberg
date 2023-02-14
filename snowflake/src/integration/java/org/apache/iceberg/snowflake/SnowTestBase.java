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
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.junit.BeforeClass;
import org.junit.jupiter.api.AfterAll;

@SuppressWarnings("VisibilityModifier")
class SnowTestBase {

  static SnowflakeCatalog snowflakeCatalog;

  static JdbcClientPool clientPool;

  protected SnowTestBase() {}

  @BeforeClass
  public static void beforeAll() {
    snowflakeCatalog = new SnowflakeCatalog();
    TestConfigurations configs = TestConfigurations.getInstance();
    // Check required arguments for test
    Preconditions.checkNotNull(
        configs.getURI(),
        "URI is required argument and should be resolved from environment variable SNOW_URI");
    // Ensure that environment variable resolution was attempted and the variable was resolved.
    Preconditions.checkArgument(
        !configs.getURI().isEmpty() && !configs.getURI().contains("env"),
        "URI is required argument and should be resolved from environment variable SNOW_URI");
    Preconditions.checkNotNull(
        configs.getDatabase(),
        "Database is required argument, please set environment variable SNOW_TEST_DB_NAME");

    snowflakeCatalog.initialize("testCatalog", configs.getProperties());
    clientPool = new JdbcClientPool(configs.getURI(), configs.getProperties());
    try {
      createOrReplaceDatabase(configs.getDatabase());
    } catch (SQLException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @AfterAll
  public void afterAll() {
    try {
      dropDatabaseIfExists(TestConfigurations.getInstance().getDatabase());
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
