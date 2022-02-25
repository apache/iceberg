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
  // Catalog Table
  protected static final String CATALOG_TABLE_NAME = "iceberg_tables";
  protected static final String CATALOG_NAME = "catalog_name";
  protected static final String TABLE_NAMESPACE = "table_namespace";
  protected static final String TABLE_NAME = "table_name";
  protected static final String METADATA_LOCATION = "metadata_location";
  protected static final String PREVIOUS_METADATA_LOCATION = "previous_metadata_location";

  public static final String DO_COMMIT_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET " + METADATA_LOCATION + " = ? , " + PREVIOUS_METADATA_LOCATION + " = ? " +
      " WHERE " + CATALOG_NAME + " = ? AND " +
      TABLE_NAMESPACE + " = ? AND " +
      TABLE_NAME + " = ? AND " +
      METADATA_LOCATION + " = ?";
  protected static final String CREATE_CATALOG_TABLE =
      "CREATE TABLE " + CATALOG_TABLE_NAME +
          "(" +
          CATALOG_NAME + " VARCHAR(255) NOT NULL," +
          TABLE_NAMESPACE + " VARCHAR(255) NOT NULL," +
          TABLE_NAME + " VARCHAR(255) NOT NULL," +
          METADATA_LOCATION + " VARCHAR(5500)," +
          PREVIOUS_METADATA_LOCATION + " VARCHAR(5500)," +
          "PRIMARY KEY (" + CATALOG_NAME + ", " + TABLE_NAMESPACE + ", " + TABLE_NAME + ")" +
          ")";
  protected static final String GET_TABLE_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  protected static final String LIST_TABLES_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ?";
  protected static final String RENAME_TABLE_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET " + TABLE_NAMESPACE + " = ? , " + TABLE_NAME + " = ? " +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  protected static final String DROP_TABLE_SQL = "DELETE FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " = ? AND " + TABLE_NAME + " = ? ";
  protected static final String GET_NAMESPACE_SQL = "SELECT " + TABLE_NAMESPACE + " FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " LIKE ? LIMIT 1";
  protected static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT " + TABLE_NAMESPACE +
      " FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + TABLE_NAMESPACE + " LIKE ?";
  protected static final String LIST_ALL_TABLE_NAMESPACES_SQL = "SELECT DISTINCT " + TABLE_NAMESPACE +
      " FROM " + CATALOG_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ?";
  protected static final String DO_COMMIT_CREATE_TABLE_SQL = "INSERT INTO " + CATALOG_TABLE_NAME +
      " (" + CATALOG_NAME + ", " + TABLE_NAMESPACE + ", " + TABLE_NAME +
      ", " + METADATA_LOCATION + ", " + PREVIOUS_METADATA_LOCATION + ") " +
      " VALUES (?,?,?,?,null)";

  // Catalog Namespace Properties
  protected static final String NAMESPACE_PROPERTIES_TABLE_NAME = "iceberg_namespace_properties";
  protected static final String NAMESPACE_NAME = "namespace";
  protected static final String NAMESPACE_PROPERTY_KEY = "key";
  protected static final String NAMESPACE_PROPERTY_VALUE = "value";

  protected static final String CREATE_NAMESPACE_PROPERTIES_TABLE =
      "CREATE TABLE " + NAMESPACE_PROPERTIES_TABLE_NAME +
          "(" +
          CATALOG_NAME + " VARCHAR(255) NOT NULL," +
          NAMESPACE_NAME + " VARCHAR(255) NOT NULL," +
          NAMESPACE_PROPERTY_KEY + " VARCHAR(5500)," +
          NAMESPACE_PROPERTY_VALUE + " VARCHAR(5500)," +
          "PRIMARY KEY (" + CATALOG_NAME + ", " + NAMESPACE_NAME + ", " + NAMESPACE_PROPERTY_KEY + ")" +
          ")";
  protected static final String GET_NAMESPACE_PROPERTIES_SQL = "SELECT " + NAMESPACE_NAME +
      " FROM " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " LIKE ? LIMIT 1";
  protected static final String INSERT_NAMESPACE_PROPERTIES_SQL = "INSERT INTO " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " (" + CATALOG_NAME + ", " + NAMESPACE_NAME + ", " + NAMESPACE_PROPERTY_KEY +
      ", " + NAMESPACE_PROPERTY_VALUE + ") VALUES ";
  protected static final String INSERT_PROPERTIES_VALUES_BASE = "(?,?,?,?)";
  protected static final String GET_ALL_NAMESPACE_PROPERTIES_SQL = "SELECT * " +
      " FROM " + NAMESPACE_PROPERTIES_TABLE_NAME + " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " = ? ";
  protected static final String DELETE_NAMESPACE_PROPERTIES_SQL = "DELETE FROM " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " = ? AND " + NAMESPACE_PROPERTY_KEY + " IN ";
  protected static final String DELETE_ALL_NAMESPACE_PROPERTIES_SQL =
      "DELETE FROM " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " = ?";
  protected static final String LIST_PROPERTY_NAMESPACES_SQL = "SELECT DISTINCT " + NAMESPACE_NAME +
      " FROM " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " LIKE ?";
  protected static final String LIST_ALL_PROPERTY_NAMESPACES_SQL = "SELECT DISTINCT " + NAMESPACE_NAME +
      " FROM " + NAMESPACE_PROPERTIES_TABLE_NAME +
      " WHERE " + CATALOG_NAME + " = ?";

  // Utilities
  private static final Joiner JOINER_DOT = Joiner.on('.');
  private static final Splitter SPLITTER_DOT = Splitter.on('.');

  private JdbcUtil() {
  }

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

  public static Properties filterAndRemovePrefix(Map<String, String> properties,
                                                 String prefix) {
    Properties result = new Properties();
    properties.forEach((key, value) -> {
      if (key.startsWith(prefix)) {
        result.put(key.substring(prefix.length()), value);
      }
    });

    return result;
  }

  public static String updatePropertiesStatement(int size) {
    StringBuilder sqlStatement = new StringBuilder("UPDATE " + NAMESPACE_PROPERTIES_TABLE_NAME +
            " SET " + NAMESPACE_PROPERTY_VALUE + " = CASE");
    for (int i = 0; i < size; i += 1) {
      sqlStatement.append(" WHEN key = ? THEN ?");
    }
    sqlStatement.append(" END WHERE " +  CATALOG_NAME + " = ? AND " +
            NAMESPACE_NAME + " = ? AND " + NAMESPACE_PROPERTY_KEY + " IN ");

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
}
