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
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

public final class JdbcUtil {
  protected static final String CATALOG_TABLE_NAME = "ICEBERG_TABLES";
  protected static final String CREATE_CATALOG_TABLE =
      "CREATE TABLE " + CATALOG_TABLE_NAME +
          "(catalog_name VARCHAR(1255) NOT NULL," +
          "table_namespace VARCHAR(1255) NOT NULL," +
          "table_name VARCHAR(1255) NOT NULL," +
          "metadata_location VARCHAR(32768)," +
          "previous_metadata_location VARCHAR(32768)," +
          "PRIMARY KEY (catalog_name, table_namespace, table_name)" +
          ")";
  protected static final String LOAD_TABLE_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  protected static final String LIST_TABLES_SQL = "SELECT * FROM " + CATALOG_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ?";
  protected static final String RENAME_TABLE_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET table_namespace = ? , table_name = ? " +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  protected static final String DROP_TABLE_SQL = "DELETE FROM " + CATALOG_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? ";
  protected static final String GET_NAMESPACE_SQL = "SELECT table_namespace FROM " + CATALOG_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace LIKE ? LIMIT 1";
  protected static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT table_namespace FROM " + CATALOG_TABLE_NAME +
      " WHERE catalog_name = ? AND table_namespace LIKE ?";
  public static final String DO_COMMIT_SQL = "UPDATE " + CATALOG_TABLE_NAME +
      " SET metadata_location = ? , previous_metadata_location = ? " +
      " WHERE catalog_name = ? AND table_namespace = ? AND table_name = ? AND metadata_location = ?";
  protected static final String DO_COMMIT_CREATE_SQL = "INSERT INTO " + CATALOG_TABLE_NAME +
      " (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location) " +
      " VALUES (?,?,?,?,null)";

  private static final Joiner JOINER_DOT = Joiner.on('.');
  private static final Splitter SPLITTER_DOT = Splitter.on('.');

  private JdbcUtil() {
  }

  public static Namespace stringToNamespace(String string) {
    if (string == null) {
      return null;
    }

    return Namespace.of(Iterables.toArray(SPLITTER_DOT.split(string), String.class));
  }

  public static String namespaceToString(Namespace namespace) {
    return JOINER_DOT.join(namespace.levels());
  }

  public static TableIdentifier stringToTableIdentifier(String tableNamespace, String tableName) {
    return TableIdentifier.of(JdbcUtil.stringToNamespace(tableNamespace), tableName);
  }

}
