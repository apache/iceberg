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
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

class FakeBigLakeClient implements BigLakeClient {

  private final Map<String, Catalog> catalogs;
  private final Map<String, Database> dbs;
  private final Map<String, Table> tables;

  FakeBigLakeClient() {
    this.catalogs = new HashMap<String, Catalog>();
    this.dbs = new HashMap<String, Database>();
    this.tables = new HashMap<String, Table>();
  }

  @Override
  public Catalog createCatalog(CatalogName name, Catalog catalog) {
    if (catalogs.containsKey(name.toString())) {
      throw new AlreadyExistsException("BigLake resource %s already exists", name.getCatalog());
    }
    catalogs.put(name.toString(), catalog.toBuilder().setName(name.toString()).build());
    return catalog;
  }

  @Override
  public Catalog getCatalog(CatalogName name) {
    if (catalogs.containsKey(name.toString())) {
      return catalogs.get(name.toString());
    }
    throw new NoSuchNamespaceException(
        "Catalog %s does not exist or permission denied", name.toString());
  }

  @Override
  public void deleteCatalog(CatalogName name) {
    if (!catalogs.containsKey(name.toString())) {
      throw new NoSuchNamespaceException(
          "Catalog %s does not exist or permission denied", name.toString());
    }
  }

  @Override
  public Database createDatabase(DatabaseName name, Database db) {
    if (dbs.containsKey(name.toString())) {
      throw new AlreadyExistsException("BigLake resource %s already exists", name.getDatabase());
    }
    dbs.put(name.toString(), db.toBuilder().setName(name.toString()).build());
    return db;
  }

  @Override
  public Database getDatabase(DatabaseName name) {
    if (dbs.containsKey(name.toString())) {
      return dbs.get(name.toString());
    }
    throw new NoSuchNamespaceException("Namespace does not exist: %s", name.getDatabase());
  }

  @Override
  public Database updateDatabaseParameters(DatabaseName name, Map<String, String> parameters) {
    if (!dbs.containsKey(name.toString())) {
      throw new NoSuchNamespaceException(
          "Database %s does not exist or permission denied", name.toString());
    }
    Database.Builder dbBuilder = dbs.get(name.toString()).toBuilder();
    dbBuilder.getHiveOptionsBuilder().clearParameters().putAllParameters(parameters);
    Database newDb = dbBuilder.build();
    dbs.put(name.toString(), newDb);
    return newDb;
  }

  @Override
  public Iterable<Database> listDatabases(CatalogName name) {
    return dbs.values();
  }

  @Override
  public void deleteDatabase(DatabaseName name) {
    if (!dbs.containsKey(name.toString())) {
      throw new NoSuchNamespaceException(
          "Database %s does not exist or permission denied", name.toString());
    }
    dbs.remove(name.toString());
  }

  @Override
  public Table createTable(TableName name, Table table) {
    if (tables.containsKey(name.toString())) {
      throw new AlreadyExistsException("Table already exists: %s", name.getTable());
    }
    tables.put(name.toString(), table.toBuilder().setName(name.toString()).setEtag("etag").build());
    return table;
  }

  @Override
  public Table getTable(TableName name) {
    if (name.getTable().isEmpty()) {
      throw new NoSuchTableException("BigLake API does not allow tables with empty ID");
    }
    if (tables.containsKey(name.toString())) {
      return tables.get(name.toString());
    }
    throw new NoSuchTableException("Table %s does not exist or permission denied", name.toString());
  }

  @Override
  public Table updateTableParameters(TableName name, Map<String, String> parameters, String etag) {
    if (!tables.containsKey(name.toString())) {
      throw new NoSuchTableException(
          "Table %s does not exist or permission denied", name.toString());
    }
    Table.Builder tableBuilder = tables.get(name.toString()).toBuilder();
    tableBuilder.getHiveOptionsBuilder().clearParameters().putAllParameters(parameters);
    Table newTable = tableBuilder.build();
    tables.put(name.toString(), newTable);
    return newTable;
  }

  @Override
  public Table renameTable(TableName name, TableName newName) {
    if (!tables.containsKey(name.toString())) {
      throw new NoSuchTableException("Table does not exist or permission denied");
    }
    if (tables.containsKey(newName.toString())) {
      throw new AlreadyExistsException("Table already exists");
    }
    Table table = tables.get(name.toString());
    Table newTable = table.toBuilder().setName(newName.toString()).build();
    tables.put(newName.toString(), newTable);
    tables.remove(name.toString());
    return newTable;
  }

  @Override
  public Table deleteTable(TableName name) {
    if (!tables.containsKey(name.toString())) {
      throw new NoSuchTableException(
          "Table %s does not exist or permission denied", name.toString());
    }
    return tables.remove(name.toString());
  }

  @Override
  public Iterable<Table> listTables(DatabaseName name) {
    return tables.values().stream()
        .filter(t -> t.getName().contains(name.toString()))
        .collect(ImmutableList.toImmutableList());
  }
}
