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

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

final class JdbcUtil {
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
  protected static final String DO_COMMIT_CREATE_TABLE_SQL = "INSERT INTO " + CATALOG_TABLE_NAME +
      " (" + CATALOG_NAME + ", " + TABLE_NAMESPACE + ", " + TABLE_NAME +
      ", " + METADATA_LOCATION + ", " + PREVIOUS_METADATA_LOCATION + ") " +
      " VALUES (?,?,?,?,null)";
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

}
