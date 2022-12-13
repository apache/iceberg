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
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcClientPool;
import org.apache.iceberg.jdbc.UncheckedInterruptedException;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.snowflake.entities.SnowflakeSchema;
import org.apache.iceberg.snowflake.entities.SnowflakeTable;
import org.apache.iceberg.snowflake.entities.SnowflakeTableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This implementation of SnowflakeClient builds on top of Snowflake's JDBC driver to interact with
 * Snowflake's Iceberg-aware resource model. Despite using JDBC libraries, the resource model is
 * derived from Snowflake's own first-class support for Iceberg tables as opposed to using an opaque
 * JDBC layer to store Iceberg metadata itself in an Iceberg-agnostic database.
 *
 * <p>This thus differs from the JdbcCatalog in that Snowflake's service provides the source of
 * truth of Iceberg metadata, rather than serving as a storage layer for a client-defined Iceberg
 * resource model.
 */
public class JdbcSnowflakeClient implements SnowflakeClient {
  public static final String EXPECTED_JDBC_IMPL = "net.snowflake.client.jdbc.SnowflakeDriver";

  private static final Logger LOG = LoggerFactory.getLogger(JdbcSnowflakeClient.class);
  private final JdbcClientPool connectionPool;

  JdbcSnowflakeClient(JdbcClientPool conn) {
    connectionPool = conn;
  }

  @Override
  public List<SnowflakeSchema> listSchemas(Namespace namespace) {
    StringBuilder baseQuery = new StringBuilder("SHOW SCHEMAS");
    Object[] queryParams = null;
    if (namespace == null || namespace.isEmpty()) {
      // for empty or null namespace search for all schemas at account level where the user
      // has access to list.
      baseQuery.append(" IN ACCOUNT");
    } else {
      // otherwise restrict listing of schema within the database.
      baseQuery.append(" IN DATABASE IDENTIFIER(?)");
      queryParams = new Object[] {namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)};
    }

    final String finalQuery = baseQuery.toString();
    final Object[] finalQueryParams = queryParams;
    QueryRunner run = new QueryRunner(true);
    List<SnowflakeSchema> schemas;
    try {
      schemas =
          connectionPool.run(
              conn ->
                  run.query(conn, finalQuery, SnowflakeSchema.createHandler(), finalQueryParams));
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e,
          "Failed to list schemas for namespace %s",
          namespace != null ? namespace.toString() : "");
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing schemas");
    }
    return schemas;
  }

  @Override
  public List<SnowflakeTable> listIcebergTables(Namespace namespace) {
    StringBuilder baseQuery = new StringBuilder("SHOW ICEBERG TABLES");
    Object[] queryParams = null;
    if (namespace.length() == SnowflakeResources.MAX_NAMESPACE_DEPTH) {
      // For two level namespace, search for iceberg tables within the given schema.
      baseQuery.append(" IN SCHEMA IDENTIFIER(?)");
      queryParams =
          new Object[] {
            String.format(
                "%s.%s",
                namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1),
                namespace.level(SnowflakeResources.NAMESPACE_SCHEMA_LEVEL - 1))
          };
    } else if (namespace.length() == SnowflakeResources.NAMESPACE_DB_LEVEL) {
      // For one level namespace, search for iceberg tables within the given database.
      baseQuery.append(" IN DATABASE IDENTIFIER(?)");
      queryParams = new Object[] {namespace.level(SnowflakeResources.NAMESPACE_DB_LEVEL - 1)};
    } else {
      // For empty or db level namespace, search at account level.
      baseQuery.append(" IN ACCOUNT");
    }

    final String finalQuery = baseQuery.toString();
    final Object[] finalQueryParams = queryParams;
    QueryRunner run = new QueryRunner(true);
    List<SnowflakeTable> tables;
    try {
      tables =
          connectionPool.run(
              conn ->
                  run.query(conn, finalQuery, SnowflakeTable.createHandler(), finalQueryParams));
    } catch (SQLException e) {
      throw new UncheckedSQLException(
          e, "Failed to list tables for namespace %s", namespace.toString());
    } catch (InterruptedException e) {
      throw new UncheckedInterruptedException(e, "Interrupted while listing tables");
    }
    return tables;
  }

  @Override
  public SnowflakeTableMetadata getTableMetadata(TableIdentifier tableIdentifier) {
    QueryRunner run = new QueryRunner(true);

    SnowflakeTableMetadata tableMeta;
    try {
      final String finalQuery = "SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(?) AS METADATA";
      tableMeta =
          connectionPool.run(
              conn ->
                  run.query(
                      conn,
                      finalQuery,
                      SnowflakeTableMetadata.createHandler(),
                      tableIdentifier.toString()));
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
