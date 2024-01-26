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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

final class JdbcUtil {
  // property to control strict-mode (aka check if namespace exists when creating a table)
  static final String STRICT_MODE_PROPERTY = JdbcCatalog.PROPERTY_PREFIX + "strict-mode";

  // Catalog Table
  static final String CATALOG_TABLE_NAME = "iceberg_tables";
  static final String CATALOG_NAME = "catalog_name";
  static final String TABLE_NAMESPACE = "table_namespace";
  static final String TABLE_NAME = "table_name";

  static final String DO_COMMIT_SQL =
      "UPDATE "
          + CATALOG_TABLE_NAME
          + " SET "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? , "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " = ? "
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ?";
  static final String CREATE_CATALOG_TABLE =
      "CREATE TABLE "
          + CATALOG_TABLE_NAME
          + "("
          + CATALOG_NAME
          + " VARCHAR(255) NOT NULL,"
          + TABLE_NAMESPACE
          + " VARCHAR(255) NOT NULL,"
          + TABLE_NAME
          + " VARCHAR(255) NOT NULL,"
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " VARCHAR(1000),"
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " VARCHAR(1000),"
          + "PRIMARY KEY ("
          + CATALOG_NAME
          + ", "
          + TABLE_NAMESPACE
          + ", "
          + TABLE_NAME
          + ")"
          + ")";
  static final String GET_TABLE_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? ";
  static final String LIST_TABLES_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ?";
  static final String RENAME_TABLE_SQL =
      "UPDATE "
          + CATALOG_TABLE_NAME
          + " SET "
          + TABLE_NAMESPACE
          + " = ? , "
          + TABLE_NAME
          + " = ? "
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? ";
  static final String DROP_TABLE_SQL =
      "DELETE FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? ";
  static final String GET_NAMESPACE_SQL =
      "SELECT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ( "
          + TABLE_NAMESPACE
          + " = ? OR "
          + TABLE_NAMESPACE
          + " LIKE ? ESCAPE '\\' "
          + " ) "
          + " LIMIT 1";
  static final String LIST_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " LIKE ?";
  static final String LIST_ALL_TABLE_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";
  static final String DO_COMMIT_CREATE_TABLE_SQL =
      "INSERT INTO "
          + CATALOG_TABLE_NAME
          + " ("
          + CATALOG_NAME
          + ", "
          + TABLE_NAMESPACE
          + ", "
          + TABLE_NAME
          + ", "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + ", "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + ") "
          + " VALUES (?,?,?,?,null)";

  // Catalog Namespace Properties
  static final String NAMESPACE_PROPERTIES_TABLE_NAME = "iceberg_namespace_properties";
  static final String NAMESPACE_NAME = "namespace";
  static final String NAMESPACE_PROPERTY_KEY = "property_key";
  static final String NAMESPACE_PROPERTY_VALUE = "property_value";

  static final String CREATE_NAMESPACE_PROPERTIES_TABLE =
      "CREATE TABLE "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + "("
          + CATALOG_NAME
          + " VARCHAR(255) NOT NULL,"
          + NAMESPACE_NAME
          + " VARCHAR(255) NOT NULL,"
          + NAMESPACE_PROPERTY_KEY
          + " VARCHAR(255),"
          + NAMESPACE_PROPERTY_VALUE
          + " VARCHAR(1000),"
          + "PRIMARY KEY ("
          + CATALOG_NAME
          + ", "
          + NAMESPACE_NAME
          + ", "
          + NAMESPACE_PROPERTY_KEY
          + ")"
          + ")";
  static final String GET_NAMESPACE_PROPERTIES_SQL =
      "SELECT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ( "
          + NAMESPACE_NAME
          + " = ? OR "
          + NAMESPACE_NAME
          + " LIKE ? ESCAPE '\\' "
          + " ) ";
  static final String INSERT_NAMESPACE_PROPERTIES_SQL =
      "INSERT INTO "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " ("
          + CATALOG_NAME
          + ", "
          + NAMESPACE_NAME
          + ", "
          + NAMESPACE_PROPERTY_KEY
          + ", "
          + NAMESPACE_PROPERTY_VALUE
          + ") VALUES ";
  static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";
  static final String GET_ALL_NAMESPACE_PROPERTIES_SQL =
      "SELECT * "
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ? ";
  static final String DELETE_NAMESPACE_PROPERTIES_SQL =
      "DELETE FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ? AND "
          + NAMESPACE_PROPERTY_KEY
          + " IN ";
  static final String DELETE_ALL_NAMESPACE_PROPERTIES_SQL =
      "DELETE FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " = ?";
  static final String LIST_PROPERTY_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + NAMESPACE_NAME
          + " LIKE ?";
  static final String LIST_ALL_PROPERTY_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + NAMESPACE_NAME
          + " FROM "
          + NAMESPACE_PROPERTIES_TABLE_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";

  // Utilities
  private static final Joiner JOINER_DOT = Joiner.on('.');
  private static final Splitter SPLITTER_DOT = Splitter.on('.');

  private JdbcUtil() {}

  public static Namespace stringToNamespace(String namespace) {
    Preconditions.checkArgument(namespace != null, "Invalid namespace %s", namespace);
    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(namespace), String.class));
  }

  public static String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

  public static Properties filterAndRemovePrefix(Map<String, String> properties, String prefix) {
    Properties result = new Properties();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(prefix)) {
            result.put(key.substring(prefix.length()), value);
          }
        });

    return result;
  }

  public static String updatePropertiesStatement(int size) {
    StringBuilder sqlStatement =
        new StringBuilder(
            "UPDATE "
                + NAMESPACE_PROPERTIES_TABLE_NAME
                + " SET "
                + NAMESPACE_PROPERTY_VALUE
                + " = CASE");
    for (int i = 0; i < size; i += 1) {
      sqlStatement.append(" WHEN " + NAMESPACE_PROPERTY_KEY + " = ? THEN ?");
    }
    sqlStatement.append(
        " END WHERE "
            + CATALOG_NAME
            + " = ? AND "
            + NAMESPACE_NAME
            + " = ? AND "
            + NAMESPACE_PROPERTY_KEY
            + " IN ");

    String values = String.join(",", Collections.nCopies(size, String.valueOf('?')));
    sqlStatement.append("(").append(values).append(")");

    return sqlStatement.toString();
  }

  public static String insertPropertiesStatement(int size) {
    StringBuilder sqlStatement = new StringBuilder(JdbcUtil.INSERT_NAMESPACE_PROPERTIES_SQL);

    for (int i = 0; i < size; i++) {
      if (i != 0) {
        sqlStatement.append(", ");
      }
      sqlStatement.append(JdbcUtil.INSERT_PROPERTIES_VALUES_BASE);
    }

    return sqlStatement.toString();
  }

  public static String deletePropertiesStatement(Set<String> properties) {
    StringBuilder sqlStatement = new StringBuilder(JdbcUtil.DELETE_NAMESPACE_PROPERTIES_SQL);
    String values = String.join(",", Collections.nCopies(properties.size(), String.valueOf('?')));
    sqlStatement.append("(").append(values).append(")");

    return sqlStatement.toString();
  }

  static boolean namespaceExists(
      String catalogName, JdbcClientPool connections, Namespace namespace) {

    String namespaceEquals = JdbcUtil.namespaceToString(namespace);
    // when namespace has sub-namespace then additionally checking it with LIKE statement.
    // catalog.db can exists as: catalog.db.ns1 or catalog.db.ns1.ns2
    String namespaceStartsWith =
        namespaceEquals.replace("\\", "\\\\").replace("_", "\\_").replace("%", "\\%") + ".%";
    if (exists(
        connections,
        JdbcUtil.GET_NAMESPACE_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith)) {
      return true;
    }

    if (exists(
        connections,
        JdbcUtil.GET_NAMESPACE_PROPERTIES_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith)) {
      return true;
    }

    return false;
  }

  @SuppressWarnings("checkstyle:NestedTryDepth")
  private static boolean exists(JdbcClientPool connections, String sql, String... args) {
    try {
      return connections.run(
          conn -> {
            try (PreparedStatement preparedStatement = conn.prepareStatement(sql)) {
              for (int pos = 0; pos < args.length; pos += 1) {
                preparedStatement.setString(pos + 1, args[pos]);
              }

              try (ResultSet rs = preparedStatement.executeQuery()) {
                if (rs.next()) {
                  return true;
                }
              }
            }

            return false;
          });
    } catch (SQLException e) {
      throw new UncheckedSQLException(e, "Failed to execute exists query: %s", sql);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted in SQL query");
    }
  }
}
