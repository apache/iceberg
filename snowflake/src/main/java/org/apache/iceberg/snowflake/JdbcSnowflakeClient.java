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

  /**
   * For rare cases where PreparedStatements aren't supported for user-supplied identifiers intended
   * for use in special LIKE clauses, we can sanitize by "broadening" the identifier with
   * single-character wildcards and manually post-filter client-side.
   *
   * <p>Note: This sanitization approach intentionally "broadens" the scope of matching results;
   * callers must be able to handle this method returning an all-wildcard expression; i.e. the
   * caller must treat the usage of the LIKE clause as only an optional optimization, and should
   * post-filter for correctness as if the LIKE clause wasn't present in the query at all.
   */
  @VisibleForTesting
  String sanitizeIdentifierWithWildcardForLikeClause(String identifier) {
    // Restrict identifiers to the "Unquoted object identifiers" synax documented at
    // https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    //
    // Use a strict allowlist of characters, replace everything *not* matching the character set
    // with "_", which is used as a single-character wildcard in Snowflake.
    String sanitized = identifier.replaceAll("[^a-zA-Z0-9_$]", "_");
    if (sanitized.startsWith("$")) {
      sanitized = "_" + sanitized.substring(1);
    }
    return sanitized;
  }

  @Override
  public boolean databaseExists(SnowflakeIdentifier database) {
    Preconditions.checkArgument(
        database.type() == SnowflakeIdentifier.Type.DATABASE,
        "databaseExists requires a DATABASE identifier, got '%s'",
        database);

    // Due to current limitations in PreparedStatement parameters for the LIKE clause in
    // SHOW DATABASES queries, we'll use a fairly limited allowlist for identifier characters,
    // using wildcards for non-allowed characters, and post-filter for matching.
    final String finalQuery =
        String.format(
            "SHOW DATABASES LIKE '%s' IN ACCOUNT",
            sanitizeIdentifierWithWildcardForLikeClause(database.databaseName()));
    List<SnowflakeIdentifier> databases;
    try {
      databases =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      finalQuery,
                      SnowflakeIdentifier.DATABASE_RESULT_SET_HANDLER,
                      (Object[]) null));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to check if database exists");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while checking if database exists");
    }

    // Filter to handle the edge case of '_' appearing as a wildcard that can't be remapped the way
    // it can for predicates in SELECT statements.
    databases.removeIf(db -> !database.databaseName().equalsIgnoreCase(db.databaseName()));
    return !databases.isEmpty();
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

    // Due to current limitations in PreparedStatement parameters for the LIKE clause in
    // SHOW SCHEMAS queries, we'll use a fairly limited allowlist for identifier characters,
    // using wildcards for non-allowed characters, and post-filter for matching.
    final String finalQuery =
        String.format(
            "SHOW SCHEMAS LIKE '%s' IN DATABASE IDENTIFIER(?)",
            sanitizeIdentifierWithWildcardForLikeClause(schema.schemaName()));
    List<SnowflakeIdentifier> schemas;
    try {
      schemas =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      finalQuery,
                      SnowflakeIdentifier.SCHEMA_RESULT_SET_HANDLER,
                      new Object[] {schema.databaseName()}));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to check if schema exists");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while checking if schema exists");
    }

    // Filter to handle the edge case of '_' appearing as a wildcard that can't be remapped the way
    // it can for predicates in SELECT statements.
    schemas.removeIf(sc -> !schema.schemaName().equalsIgnoreCase(sc.schemaName()));
    return !schemas.isEmpty();
  }

  @Override
  public List<SnowflakeIdentifier> listDatabases() {
    List<SnowflakeIdentifier> databases;
    try {
      databases =
          connectionPool.run(
              conn ->
                  queryRunner.query(
                      conn,
                      "SHOW DATABASES IN ACCOUNT",
                      SnowflakeIdentifier.DATABASE_RESULT_SET_HANDLER,
                      (Object[]) null));
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to list databases");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing databases");
    }
    return databases;
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
