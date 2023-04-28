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
package org.apache.iceberg.gcp.biglake;

import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import java.util.Map;

/** A client interface of Google BigLake service. */
interface BigLakeClient {

  /**
   * Creates and returns a new catalog.
   *
   * @param name full catalog name
   * @param catalog body of catalog to create
   */
  Catalog createCatalog(CatalogName name, Catalog catalog);

  /**
   * Returns a catalog.
   *
   * @param name full catalog name
   */
  Catalog getCatalog(CatalogName name);

  /**
   * Deletes a catalog.
   *
   * @param name full catalog name
   */
  void deleteCatalog(CatalogName name);

  /**
   * Creates and returns a new database.
   *
   * @param name full database name
   * @param db body of database to create
   */
  Database createDatabase(DatabaseName name, Database db);

  /**
   * Returns a database.
   *
   * @param name full database name
   */
  Database getDatabase(DatabaseName name);

  /**
   * Updates the parameters of a Hive database and returns the updated database.
   *
   * @param name full database name
   * @param parameters Hive options parameters to fully update
   */
  Database updateDatabaseParameters(DatabaseName name, Map<String, String> parameters);

  /**
   * Returns all databases in a catalog.
   *
   * @param name full catalog name
   */
  Iterable<Database> listDatabases(CatalogName name);

  /**
   * Deletes a database.
   *
   * @param name full database name
   */
  void deleteDatabase(DatabaseName name);

  /**
   * Creates and returns a new table.
   *
   * @param name full database name
   * @param table body of table to create
   */
  Table createTable(TableName name, Table table);

  /**
   * Returns a table.
   *
   * @param name full table name
   */
  Table getTable(TableName name);

  /**
   * Updates the parameters of a Hive table and returns the updated table.
   *
   * @param name full table name
   * @param parameters Hive options parameters to fully update
   * @param etag representation of table fields for concurrent update detection, see
   *     https://www.rfc-editor.org/rfc/rfc7232#section-2.3
   */
  Table updateTableParameters(TableName name, Map<String, String> parameters, String etag);

  /**
   * Renames a table.
   *
   * @param name full table name
   * @param newName new full table name
   */
  Table renameTable(TableName name, TableName newName);

  /**
   * Deletes a table.
   *
   * @param name full table name
   */
  Table deleteTable(TableName name);

  /**
   * Returns all tables in a database.
   *
   * @param name full database name
   */
  Iterable<Table> listTables(DatabaseName name);
}
