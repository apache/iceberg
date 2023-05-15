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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * This implementation of SnowflakeClient builds on top of Snowflake's JDBC driver to interact with
 * Snowflake's Iceberg-aware resource model.
 */
class JdbcSnowflakeClient implements SnowflakeClient {
  static final String EXPECTED_JDBC_IMPL = "net.snowflake.client.jdbc.SnowflakeDriver";

  @VisibleForTesting
  static final Set<Integer> DATABASE_NOT_FOUND_ERROR_CODES = ImmutableSet.of(2001, 2003, 2043);

  @VisibleForTesting
  static final Set<Integer> SCHEMA_NOT_FOUND_ERROR_CODES = ImmutableSet.of(2001, 2003, 2043);

  @VisibleForTesting
  static final Set<Integer> TABLE_NOT_FOUND_ERROR_CODES = ImmutableSet.of(2001, 2003, 2043);

  @FunctionalInterface
  interface ResultSetParser<T> {
    T parse(ResultSet rs) throws SQLException;
  }

  /**
   * This class wraps the basic boilerplate of setting up PreparedStatements and applying a
   * ResultSetParser to translate a ResultSet into parsed objects. Allows easily injecting
   * subclasses for debugging/testing purposes.
   */
  static class QueryHarness {
    public <T> T query(Connection conn, String sql, ResultSetParser<T> parser, String... args)
        throws SQLException {
      try (PreparedStatement statement = conn.prepareStatement(sql)) {
        if (args != null) {
          for (int i = 0; i < args.length; ++i) {
            statement.setString(i + 1, args[i]);
          }
        }

        try (ResultSet rs = statement.executeQuery()) {
          return parser.parse(rs);
        }
      }
    }
  }

  /**
   * Expects to handle ResultSets representing fully-qualified Snowflake Database identifiers,
   * containing "name" (representing databaseName).
   */
  public static final ResultSetParser<List<SnowflakeIdentifier>> DATABASE_RESULT_SET_HANDLER =
      rs -> {
        List<SnowflakeIdentifier> databases = Lists.newArrayList();
        while (rs.next()) {
          String databaseName = rs.getString("name");
          databases.add(SnowflakeIdentifier.ofDatabase(databaseName));
        }
        return databases;
      };

  /**
   * Expects to handle ResultSets representing fully-qualified Snowflake Schema identifiers,
   * containing "database_name" and "name" (representing schemaName).
   */
  public static final ResultSetParser<List<SnowflakeIdentifier>> SCHEMA_RESULT_SET_HANDLER =
      rs -> {
        List<SnowflakeIdentifier> schemas = Lists.newArrayList();
        while (rs.next()) {
          String databaseName = rs.getString("database_name");
          String schemaName = rs.getString("name");
          schemas.add(SnowflakeIdentifier.ofSchema(databaseName, schemaName));
        }
        return schemas;
      };

  /**
   * Expects to handle ResultSets representing fully-qualified Snowflake Table identifiers,
   * containing "database_name", "schema_name", and "name" (representing tableName).
   */
  public static final ResultSetParser<List<SnowflakeIdentifier>> TABLE_RESULT_SET_HANDLER =
      rs -> {
        List<SnowflakeIdentifier> tables = Lists.newArrayList();
        while (rs.next()) {
          String databaseName = rs.getString("database_name");
          String schemaName = rs.getString("schema_name");
          String tableName = rs.getString("name");
          tables.add(SnowflakeIdentifier.ofTable(databaseName, schemaName, tableName));
        }
        return tables;
      };

  /**
   * Expects to handle ResultSets representing a single record holding Snowflake Iceberg metadata.
   */
  public static final ResultSetParser<SnowflakeTableMetadata> TABLE_METADATA_RESULT_SET_HANDLER =
      rs -> {
        if (!rs.next()) {
          return null;
        }

        String rawJsonVal = rs.getString("METADATA");
        return SnowflakeTableMetadata.parseJson(rawJsonVal);
      };

  private final JdbcClientPool connectionPool;
  private QueryHarness queryHarness;

  JdbcSnowflakeClient(JdbcClientPool conn) {
    Preconditions.checkArgument(null != conn, "JdbcClientPool must be non-null");
    connectionPool = conn;
    queryHarness = new QueryHarness();
  }

  @VisibleForTesting
  void setQueryHarness(QueryHarness queryHarness) {
    this.queryHarness = queryHarness;
  }

  @Override
  public boolean databaseExists(SnowflakeIdentifier database) {
    Preconditions.checkArgument(
        database.type() == SnowflakeIdentifier.Type.DATABASE,
        "databaseExists requires a DATABASE identifier, got '%s'",
        database);

    final String finalQuery = "SHOW SCHEMAS IN DATABASE IDENTIFIER(?) LIMIT 1";

    List<SnowflakeIdentifier> schemas;
    try {
      schemas =
          connectionPool.run(
              conn ->
                  queryHarness.query(
                      conn, finalQuery, SCHEMA_RESULT_SET_HANDLER, database.databaseName()));
    } catch (SQLException e) {
      if (DATABASE_NOT_FOUND_ERROR_CODES.contains(e.getErrorCode())) {
        return false;
      }
      throw new UncheckedSQLException(e, "Failed to check if database '%s' exists", database);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(
          e, "Interrupted while checking if database '%s' exists", database);
    }

    return !schemas.isEmpty();
  }

  @Override
  public boolean schemaExists(SnowflakeIdentifier schema) {
    Preconditions.checkArgument(
        schema.type() == SnowflakeIdentifier.Type.SCHEMA,
        "schemaExists requires a SCHEMA identifier, got '%s'",
        schema);

    if (!databaseExists(SnowflakeIdentifier.ofDatabase(schema.databaseName()))) {
      return false;
    }

    final String finalQuery = "SHOW TABLES IN SCHEMA IDENTIFIER(?) LIMIT 1";

    List<SnowflakeIdentifier> tables;
    try {
      tables =
          connectionPool.run(
              conn ->
                  queryHarness.query(
                      conn, finalQuery, TABLE_RESULT_SET_HANDLER, schema.toIdentifierString()));
    } catch (SQLException e) {
      if (SCHEMA_NOT_FOUND_ERROR_CODES.contains(e.getErrorCode())) {
        return false;
      }
      throw new UncheckedSQLException(e, "Failed to check if schema '%s' exists", schema);
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(
          e, "Interrupted while checking if schema '%s' exists", schema);
    }

    return true;
  }

  @Override
  public List<SnowflakeIdentifier> listDatabases() {
    List<SnowflakeIdentifier> databases;
    try {
      databases =
          connectionPool.run(
              conn ->
                  queryHarness.query(
                      conn, "SHOW DATABASES IN ACCOUNT", DATABASE_RESULT_SET_HANDLER));
    } catch (SQLException e) {
      throw snowflakeExceptionToIcebergException(
          SnowflakeIdentifier.ofRoot(), e, "Failed to list databases");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing databases");
    }
    databases.forEach(
        db ->
            Preconditions.checkState(
                db.type() == SnowflakeIdentifier.Type.DATABASE,
                "Expected DATABASE, got identifier '%s'",
                db));
    return databases;
  }

  @Override
  public List<SnowflakeIdentifier> listSchemas(SnowflakeIdentifier scope) {
    StringBuilder baseQuery = new StringBuilder("SHOW SCHEMAS");
    String[] queryParams = null;
    switch (scope.type()) {
      case ROOT:
        // account-level listing
        baseQuery.append(" IN ACCOUNT");
        break;
      case DATABASE:
        // database-level listing
        baseQuery.append(" IN DATABASE IDENTIFIER(?)");
        queryParams = new String[] {scope.toIdentifierString()};
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listSchemas: %s", scope));
    }

    final String finalQuery = baseQuery.toString();
    final String[] finalQueryParams = queryParams;
    List<SnowflakeIdentifier> schemas;
    try {
      schemas =
          connectionPool.run(
              conn ->
                  queryHarness.query(
                      conn, finalQuery, SCHEMA_RESULT_SET_HANDLER, finalQueryParams));
    } catch (SQLException e) {
      throw snowflakeExceptionToIcebergException(
          scope, e, String.format("Failed to list schemas for scope '%s'", scope));
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(
          e, "Interrupted while listing schemas for scope '%s'", scope);
    }
    schemas.forEach(
        schema ->
            Preconditions.checkState(
                schema.type() == SnowflakeIdentifier.Type.SCHEMA,
                "Expected SCHEMA, got identifier '%s' for scope '%s'",
                schema,
                scope));
    return schemas;
  }

  @Override
  public List<SnowflakeIdentifier> listIcebergTables(SnowflakeIdentifier scope) {
    StringBuilder baseQuery = new StringBuilder("SHOW ICEBERG TABLES");
    String[] queryParams = null;
    switch (scope.type()) {
      case ROOT:
        // account-level listing
        baseQuery.append(" IN ACCOUNT");
        break;
      case DATABASE:
        // database-level listing
        baseQuery.append(" IN DATABASE IDENTIFIER(?)");
        queryParams = new String[] {scope.toIdentifierString()};
        break;
      case SCHEMA:
        // schema-level listing
        baseQuery.append(" IN SCHEMA IDENTIFIER(?)");
        queryParams = new String[] {scope.toIdentifierString()};
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unsupported scope type for listIcebergTables: %s", scope));
    }

    final String finalQuery = baseQuery.toString();
    final String[] finalQueryParams = queryParams;
    List<SnowflakeIdentifier> tables;
    try {
      tables =
          connectionPool.run(
              conn ->
                  queryHarness.query(conn, finalQuery, TABLE_RESULT_SET_HANDLER, finalQueryParams));
    } catch (SQLException e) {
      throw snowflakeExceptionToIcebergException(
          scope, e, String.format("Failed to list tables for scope '%s'", scope));
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(
          e, "Interrupted while listing tables for scope '%s'", scope);
    }
    tables.forEach(
        table ->
            Preconditions.checkState(
                table.type() == SnowflakeIdentifier.Type.TABLE,
                "Expected TABLE, got identifier '%s' for scope '%s'",
                table,
                scope));
    return tables;
  }

  @Override
  public SnowflakeTableMetadata loadTableMetadata(SnowflakeIdentifier tableIdentifier) {
    Preconditions.checkArgument(
        tableIdentifier.type() == SnowflakeIdentifier.Type.TABLE,
        "loadTableMetadata requires a TABLE identifier, got '%s'",
        tableIdentifier);
    SnowflakeTableMetadata tableMeta;
    try {
      final String finalQuery = "SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA";
      tableMeta =
          connectionPool.run(
              conn ->
                  queryHarness.query(
                      conn,
                      finalQuery,
                      TABLE_METADATA_RESULT_SET_HANDLER,
                      tableIdentifier.toIdentifierString()));
    } catch (SQLException e) {
      throw snowflakeExceptionToIcebergException(
          tableIdentifier,
          e,
          String.format("Failed to get table metadata for '%s'", tableIdentifier));
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(
          e, "Interrupted while getting table metadata for '%s'", tableIdentifier);
    }
    return tableMeta;
  }

  @Override
  public void close() {
    connectionPool.close();
  }

  private RuntimeException snowflakeExceptionToIcebergException(
      SnowflakeIdentifier identifier, SQLException ex, String defaultExceptionMessage) {
    // NoSuchNamespace exception for Database and Schema cases
    if ((identifier.type() == SnowflakeIdentifier.Type.DATABASE
            && DATABASE_NOT_FOUND_ERROR_CODES.contains(ex.getErrorCode()))
        || (identifier.type() == SnowflakeIdentifier.Type.SCHEMA
            && SCHEMA_NOT_FOUND_ERROR_CODES.contains(ex.getErrorCode()))) {
      return new NoSuchNamespaceException(
          ex,
          "Identifier not found: '%s'. Underlying exception: '%s'",
          identifier,
          ex.getMessage());
    }
    // NoSuchTable exception for Table cases
    else if (identifier.type() == SnowflakeIdentifier.Type.TABLE
        && TABLE_NOT_FOUND_ERROR_CODES.contains(ex.getErrorCode())) {
      return new NoSuchTableException(
          ex,
          "Identifier not found: '%s'. Underlying exception: '%s'",
          identifier,
          ex.getMessage());
    }
    // Unchecked SQL Exception in all other cases as fall back
    return new UncheckedSQLException(ex, "Exception Message: %s", defaultExceptionMessage);
  }
}
