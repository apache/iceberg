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

public class JdbcCatalogNamespace {
  protected static final String CATALOG_NAMESPACE_TABLE_NAME = "iceberg_namespaces";
  protected static final String CATALOG_NAME = "catalog_name";
  protected static final String NAMESPACE_NAME = "namespace_name";
  protected static final String NAMESPACE_LOCATION = "location";
  protected static final String NAMESPACE_METADATA = "metadata";
  protected static final String NAMESPACE_PROPERTIES = "properties";

  protected static final String CREATE_NAMESPACE_TABLE =
          "CREATE TABLE " + CATALOG_NAMESPACE_TABLE_NAME +
                  "(" +
                  CATALOG_NAME + " VARCHAR(255) NOT NULL," +
                  NAMESPACE_NAME + " VARCHAR(255) NOT NULL," +
                  NAMESPACE_LOCATION + " VARCHAR(5500)," +
                  NAMESPACE_METADATA + " VARCHAR(65535)," +
                  NAMESPACE_PROPERTIES + " VARCHAR(65535)," +
                  "PRIMARY KEY (" + CATALOG_NAME + ", " + NAMESPACE_NAME + ")" +
                  ")";

  protected static final String GET_NAMESPACE_SQL = "SELECT " + NAMESPACE_NAME + " FROM " +
          CATALOG_NAMESPACE_TABLE_NAME + " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME +
          " LIKE ? LIMIT 1";

  protected static final String LIST_NAMESPACES_SQL = "SELECT DISTINCT " + NAMESPACE_NAME +
          " FROM " + CATALOG_NAMESPACE_TABLE_NAME +
          " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " LIKE ?";

  protected static final String DO_COMMIT_CREATE_NAMESPACE_SQL = "INSERT INTO " + CATALOG_NAMESPACE_TABLE_NAME +
          " (" + CATALOG_NAME + ", " + NAMESPACE_NAME + ", " + NAMESPACE_LOCATION + ", " + NAMESPACE_METADATA +
          ", " + NAMESPACE_PROPERTIES + ") " +
          " VALUES (?,?,?,?,?)";

  protected static final String DELETE_NAMESPACE_SQL = "DELETE FROM " + CATALOG_NAMESPACE_TABLE_NAME +
          " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " = ?";

  protected static final String UPDATE_NAMESPACE_PROPERTIES_SQL = "UPDATE " + CATALOG_NAMESPACE_TABLE_NAME +
          " SET " + NAMESPACE_PROPERTIES + " = ? " + " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME +
          " = ? ";

  protected static final String GET_NAMESPACE_PROPERTIES_SQL = "SELECT " + NAMESPACE_NAME + " , " +
          NAMESPACE_PROPERTIES + " FROM " +
          CATALOG_NAMESPACE_TABLE_NAME + " WHERE " + CATALOG_NAME + " = ? AND " + NAMESPACE_NAME + " = ? ";

  private JdbcCatalogNamespace() {
  }
}
