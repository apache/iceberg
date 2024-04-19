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
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

final class JdbcUtil {
  // property to control strict-mode (aka check if namespace exists when creating a table)
  static final String STRICT_MODE_PROPERTY = JdbcCatalog.PROPERTY_PREFIX + "strict-mode";
  // property to control if view support is added to the existing database
  static final String SCHEMA_VERSION_PROPERTY = JdbcCatalog.PROPERTY_PREFIX + "schema-version";

  enum SchemaVersion {
    V0,
    V1
  }

  // Catalog Table & View
  static final String CATALOG_TABLE_VIEW_NAME = "iceberg_tables";
  static final String CATALOG_NAME = "catalog_name";
  static final String TABLE_NAME = "table_name";
  static final String TABLE_NAMESPACE = "table_namespace";
  static final String RECORD_TYPE = "iceberg_type";
  static final String TABLE_RECORD_TYPE = "TABLE";
  static final String VIEW_RECORD_TYPE = "VIEW";

  private static final String V1_DO_COMMIT_TABLE_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? , "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? AND ("
          + RECORD_TYPE
          + " = '"
          + TABLE_RECORD_TYPE
          + "'"
          + " OR "
          + RECORD_TYPE
          + " IS NULL)";
  private static final String V1_DO_COMMIT_VIEW_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? , "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? AND "
          + RECORD_TYPE
          + " = "
          + "'"
          + VIEW_RECORD_TYPE
          + "'";
  private static final String V0_DO_COMMIT_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ? , "
          + JdbcTableOperations.PREVIOUS_METADATA_LOCATION_PROP
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + JdbcTableOperations.METADATA_LOCATION_PROP
          + " = ?";
  static final String V0_CREATE_CATALOG_SQL =
      "CREATE TABLE "
          + CATALOG_TABLE_VIEW_NAME
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
  static final String V1_UPDATE_CATALOG_SQL =
      "ALTER TABLE " + CATALOG_TABLE_VIEW_NAME + " ADD COLUMN " + RECORD_TYPE + " VARCHAR(5)";

  private static final String GET_VIEW_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + RECORD_TYPE
          + " = "
          + "'"
          + VIEW_RECORD_TYPE
          + "'";
  private static final String V1_GET_TABLE_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND ("
          + RECORD_TYPE
          + " = "
          + "'"
          + TABLE_RECORD_TYPE
          + "'"
          + " OR "
          + RECORD_TYPE
          + " IS NULL)";
  private static final String V0_GET_TABLE_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ?";
  static final String LIST_VIEW_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + RECORD_TYPE
          + " = "
          + "'"
          + VIEW_RECORD_TYPE
          + "'";
  static final String V1_LIST_TABLE_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND ("
          + RECORD_TYPE
          + " = "
          + "'"
          + TABLE_RECORD_TYPE
          + "'"
          + " OR "
          + RECORD_TYPE
          + " IS NULL)";
  static final String V0_LIST_TABLE_SQL =
      "SELECT * FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ?";
  static final String RENAME_VIEW_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + TABLE_NAMESPACE
          + " = ?, "
          + TABLE_NAME
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + RECORD_TYPE
          + " = "
          + "'"
          + VIEW_RECORD_TYPE
          + "'";
  static final String V1_RENAME_TABLE_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + TABLE_NAMESPACE
          + " = ?, "
          + TABLE_NAME
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ? AND ("
          + RECORD_TYPE
          + " = "
          + "'"
          + TABLE_RECORD_TYPE
          + "'"
          + " OR "
          + RECORD_TYPE
          + " IS NULL)";
  static final String V0_RENAME_TABLE_SQL =
      "UPDATE "
          + CATALOG_TABLE_VIEW_NAME
          + " SET "
          + TABLE_NAMESPACE
          + " = ?, "
          + TABLE_NAME
          + " = ?"
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " = ? AND "
          + TABLE_NAME
          + " = ?";
  static final String DROP_VIEW_SQL =
      "DELETE FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + "  = ? AND "
          + TABLE_NAME
          + " = ? AND "
          + RECORD_TYPE
          + " = "
          + "'"
          + VIEW_RECORD_TYPE
          + "'";
  static final String V1_DROP_TABLE_SQL =
      "DELETE FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + "  = ? AND "
          + TABLE_NAME
          + " = ? AND ("
          + RECORD_TYPE
          + " = "
          + "'"
          + TABLE_RECORD_TYPE
          + "'"
          + " OR "
          + RECORD_TYPE
          + " IS NULL)";
  static final String V0_DROP_TABLE_SQL =
      "DELETE FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + "  = ? AND "
          + TABLE_NAME
          + " = ?";
  private static final String GET_NAMESPACE_SQL =
      "SELECT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + " ("
          + TABLE_NAMESPACE
          + " = ? OR "
          + TABLE_NAMESPACE
          + " LIKE ? ESCAPE '!')"
          + " LIMIT 1";
  static final String LIST_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ? AND "
          + TABLE_NAMESPACE
          + " LIKE ?";
  static final String LIST_ALL_NAMESPACES_SQL =
      "SELECT DISTINCT "
          + TABLE_NAMESPACE
          + " FROM "
          + CATALOG_TABLE_VIEW_NAME
          + " WHERE "
          + CATALOG_NAME
          + " = ?";
  private static final String V1_DO_COMMIT_CREATE_SQL =
      "INSERT INTO "
          + CATALOG_TABLE_VIEW_NAME
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
          + ", "
          + RECORD_TYPE
          + ") "
          + " VALUES (?,?,?,?,null,?)";
  private static final String V0_DO_COMMIT_CREATE_SQL =
      "INSERT INTO "
          + CATALOG_TABLE_VIEW_NAME
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

  static final String CREATE_NAMESPACE_PROPERTIES_TABLE_SQL =
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
          + " LIKE ? ESCAPE '!' "
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

  static Namespace stringToNamespace(String namespace) {
    Preconditions.checkArgument(namespace != null, "Invalid namespace %s", namespace);
    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(namespace), String.class));
  }

  static String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

  static Properties filterAndRemovePrefix(Map<String, String> properties, String prefix) {
    Properties result = new Properties();
    properties.forEach(
        (key, value) -> {
          if (key.startsWith(prefix)) {
            result.put(key.substring(prefix.length()), value);
          }
        });

    return result;
  }

  private static int update(
      boolean isTable,
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier identifier,
      String newMetadataLocation,
      String oldMetadataLocation)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          try (PreparedStatement sql =
              conn.prepareStatement(
                  (schemaVersion == SchemaVersion.V1)
                      ? (isTable ? V1_DO_COMMIT_TABLE_SQL : V1_DO_COMMIT_VIEW_SQL)
                      : V0_DO_COMMIT_SQL)) {
            // UPDATE
            sql.setString(1, newMetadataLocation);
            sql.setString(2, oldMetadataLocation);
            // WHERE
            sql.setString(3, catalogName);
            sql.setString(4, namespaceToString(identifier.namespace()));
            sql.setString(5, identifier.name());
            sql.setString(6, oldMetadataLocation);

            return sql.executeUpdate();
          }
        });
  }

  static int updateTable(
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier tableIdentifier,
      String newMetadataLocation,
      String oldMetadataLocation)
      throws SQLException, InterruptedException {
    return update(
        true,
        schemaVersion,
        connections,
        catalogName,
        tableIdentifier,
        newMetadataLocation,
        oldMetadataLocation);
  }

  static int updateView(
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier viewIdentifier,
      String newMetadataLocation,
      String oldMetadataLocation)
      throws SQLException, InterruptedException {
    return update(
        false,
        SchemaVersion.V1,
        connections,
        catalogName,
        viewIdentifier,
        newMetadataLocation,
        oldMetadataLocation);
  }

  private static Map<String, String> tableOrView(
      boolean isTable,
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier identifier)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          Map<String, String> tableOrView = Maps.newHashMap();

          try (PreparedStatement sql =
              conn.prepareStatement(
                  isTable
                      ? ((schemaVersion == SchemaVersion.V1) ? V1_GET_TABLE_SQL : V0_GET_TABLE_SQL)
                      : GET_VIEW_SQL)) {
            sql.setString(1, catalogName);
            sql.setString(2, namespaceToString(identifier.namespace()));
            sql.setString(3, identifier.name());
            ResultSet rs = sql.executeQuery();

            if (rs.next()) {
              tableOrView.put(CATALOG_NAME, rs.getString(CATALOG_NAME));
              tableOrView.put(TABLE_NAMESPACE, rs.getString(TABLE_NAMESPACE));
              tableOrView.put(TABLE_NAME, rs.getString(TABLE_NAME));
              tableOrView.put(
                  BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                  rs.getString(BaseMetastoreTableOperations.METADATA_LOCATION_PROP));
              tableOrView.put(
                  BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP,
                  rs.getString(BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP));
            }

            rs.close();
          }

          return tableOrView;
        });
  }

  static Map<String, String> loadTable(
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier identifier)
      throws SQLException, InterruptedException {
    return tableOrView(true, schemaVersion, connections, catalogName, identifier);
  }

  static Map<String, String> loadView(
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      TableIdentifier identifier)
      throws SQLException, InterruptedException {
    return tableOrView(false, schemaVersion, connections, catalogName, identifier);
  }

  private static int doCommitCreate(
      boolean isTable,
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      Namespace namespace,
      TableIdentifier identifier,
      String newMetadataLocation)
      throws SQLException, InterruptedException {
    return connections.run(
        conn -> {
          try (PreparedStatement sql =
              conn.prepareStatement(
                  (schemaVersion == SchemaVersion.V1)
                      ? V1_DO_COMMIT_CREATE_SQL
                      : V0_DO_COMMIT_CREATE_SQL)) {
            sql.setString(1, catalogName);
            sql.setString(2, namespaceToString(namespace));
            sql.setString(3, identifier.name());
            sql.setString(4, newMetadataLocation);
            if (schemaVersion == SchemaVersion.V1) {
              sql.setString(5, isTable ? TABLE_RECORD_TYPE : VIEW_RECORD_TYPE);
            }

            return sql.executeUpdate();
          }
        });
  }

  static int doCommitCreateTable(
      SchemaVersion schemaVersion,
      JdbcClientPool connections,
      String catalogName,
      Namespace namespace,
      TableIdentifier tableIdentifier,
      String newMetadataLocation)
      throws SQLException, InterruptedException {
    return doCommitCreate(
        true,
        schemaVersion,
        connections,
        catalogName,
        namespace,
        tableIdentifier,
        newMetadataLocation);
  }

  static int doCommitCreateView(
      JdbcClientPool connections,
      String catalogName,
      Namespace namespace,
      TableIdentifier viewIdentifier,
      String newMetadataLocation)
      throws SQLException, InterruptedException {
    return doCommitCreate(
        false,
        SchemaVersion.V1,
        connections,
        catalogName,
        namespace,
        viewIdentifier,
        newMetadataLocation);
  }

  static boolean viewExists(
      String catalogName, JdbcClientPool connections, TableIdentifier viewIdentifier) {
    return exists(
        connections,
        GET_VIEW_SQL,
        catalogName,
        namespaceToString(viewIdentifier.namespace()),
        viewIdentifier.name());
  }

  static boolean tableExists(
      SchemaVersion schemaVersion,
      String catalogName,
      JdbcClientPool connections,
      TableIdentifier tableIdentifier) {
    return exists(
        connections,
        (schemaVersion == SchemaVersion.V1) ? V1_GET_TABLE_SQL : V0_GET_TABLE_SQL,
        catalogName,
        namespaceToString(tableIdentifier.namespace()),
        tableIdentifier.name());
  }

  static String updatePropertiesStatement(int size) {
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

  static String insertPropertiesStatement(int size) {
    StringBuilder sqlStatement = new StringBuilder(JdbcUtil.INSERT_NAMESPACE_PROPERTIES_SQL);

    for (int i = 0; i < size; i++) {
      if (i != 0) {
        sqlStatement.append(", ");
      }
      sqlStatement.append(JdbcUtil.INSERT_PROPERTIES_VALUES_BASE);
    }

    return sqlStatement.toString();
  }

  static String deletePropertiesStatement(Set<String> properties) {
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
        namespaceEquals.replace("!", "!!").replace("_", "!_").replace("%", "!%") + ".%";
    if (exists(connections, GET_NAMESPACE_SQL, catalogName, namespaceEquals, namespaceStartsWith)) {
      return true;
    }

    return exists(
        connections,
        JdbcUtil.GET_NAMESPACE_PROPERTIES_SQL,
        catalogName,
        namespaceEquals,
        namespaceStartsWith);
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
