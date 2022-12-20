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

import java.sql.SQLException;
import java.util.List;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * This implementation of SnowflakeClient builds on top of Snowflake's JDBC driver to interact with
 * Snowflake's Iceberg-aware resource model.
 */
class JdbcSnowflakeClient implements SnowflakeClient {
  public static final String EXPECTED_JDBC_IMPL = "net.snowflake.client.jdbc.SnowflakeDriver";

  private final JdbcClientPool connectionPool;
  private QueryRunner queryRunner;

  JdbcSnowflakeClient(JdbcClientPool conn) {
    Preconditions.checkArgument(null != conn, "JdbcClientPool must be non-null");
    connectionPool = conn;
    queryRunner = new QueryRunner(true);
  }

  @VisibleForTesting
  void setQueryRunner(QueryRunner queryRunner) {
    this.queryRunner = queryRunner;
  }

  @Override
  public List<SnowflakeIdentifier> listSchemas(SnowflakeIdentifier scope) {
    StringBuilder baseQuery = new StringBuilder("SHOW SCHEMAS");
    Object[] queryParams = null;
    switch (scope.type()) {
      case ROOT:
        // account-level listing
        baseQuery.append(" IN ACCOUNT");
        break;
      case DATABASE:
        // database-level listing
        baseQuery.append(" IN DATABASE IDENTIFIER(?)");
        queryParams = new Object[] {scope.toIdentifierString()};
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listSchemas: %s", scope));
    }

    final String finalQuery = baseQuery.toString();
    final Object[] finalQueryParams = queryParams;
    List<SnowflakeIdentifier> schemas;
    try {
      schemas =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      finalQuery,
                      SnowflakeIdentifier.SCHEMA_RESULT_SET_HANDLER,
                      finalQueryParams));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to list schemas for scope %s", scope);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing schemas");
    }
    return schemas;
  }

  @Override
  public List<SnowflakeIdentifier> listIcebergTables(SnowflakeIdentifier scope) {
    StringBuilder baseQuery = new StringBuilder("SHOW ICEBERG TABLES");
    Object[] queryParams = null;
    switch (scope.type()) {
      case ROOT:
        // account-level listing
        baseQuery.append(" IN ACCOUNT");
        break;
      case DATABASE:
        // database-level listing
        baseQuery.append(" IN DATABASE IDENTIFIER(?)");
        queryParams = new Object[] {scope.toIdentifierString()};
        break;
      case SCHEMA:
        // schema-level listing
        baseQuery.append(" IN SCHEMA IDENTIFIER(?)");
        queryParams = new Object[] {scope.toIdentifierString()};
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listIcebergTables: %s", scope));
    }

    final String finalQuery = baseQuery.toString();
    final Object[] finalQueryParams = queryParams;
    List<SnowflakeIdentifier> tables;
    try {
      tables =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      finalQuery,
                      SnowflakeIdentifier.TABLE_RESULT_SET_HANDLER,
                      finalQueryParams));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to list tables for scope %s", scope.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing tables");
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata loadTableMetadata(SnowflakeIdentifier tableIdentifier) {
    SnowflakeTableMetadata tableMeta;
    try {
      final String finalQuery = "SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA";
      tableMeta =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      finalQuery,
                      SnowflakeTableMetadata.createHandler(),
                      tableIdentifier.toIdentifierString()));
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to get table metadata for %s", tableIdentifier.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while getting table metadata");
    }
    return tableMeta;
  }

  @Override
  public void close() {
    connectionPool.close();
  }
}
