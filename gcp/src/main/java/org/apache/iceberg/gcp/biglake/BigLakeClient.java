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

import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.cloud.bigquery.biglake.v1.Catalog;
import com.google.cloud.bigquery.biglake.v1.CatalogName;
import com.google.cloud.bigquery.biglake.v1.CreateCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.CreateDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.CreateTableRequest;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DatabaseName;
import com.google.cloud.bigquery.biglake.v1.DeleteCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.DeleteDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.DeleteTableRequest;
import com.google.cloud.bigquery.biglake.v1.GetCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.GetDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.GetTableRequest;
import com.google.cloud.bigquery.biglake.v1.ListDatabasesRequest;
import com.google.cloud.bigquery.biglake.v1.ListTablesRequest;
import com.google.cloud.bigquery.biglake.v1.LocationName;
import com.google.cloud.bigquery.biglake.v1.MetastoreServiceClient;
import com.google.cloud.bigquery.biglake.v1.MetastoreServiceSettings;
import com.google.cloud.bigquery.biglake.v1.RenameTableRequest;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import com.google.cloud.bigquery.biglake.v1.UpdateDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.UpdateTableRequest;
import com.google.protobuf.Empty;
import com.google.protobuf.FieldMask;
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;

/** A client of Google BigLake service. */
final class BigLakeClient implements Closeable {

  private final MetastoreServiceClient stub;

  /**
   * Constructs a client of Google BigLake Service.
   *
   * @param settings BigLake service settings
   */
  BigLakeClient(MetastoreServiceSettings settings) throws IOException {
    this.stub = MetastoreServiceClient.create(settings);
  }

  /**
   * Constructs a client of Google BigLake Service.
   *
   * @param biglakeEndpoint BigLake service gRPC endpoint, e.g., "biglake.googleapis.com:443"
   */
  BigLakeClient(String biglakeEndpoint) throws IOException {
    this(MetastoreServiceSettings.newBuilder().setEndpoint(biglakeEndpoint).build());
  }

  public Catalog createCatalog(CatalogName name, Catalog catalog) {
    return convertException(
        () ->
            stub.createCatalog(
                CreateCatalogRequest.newBuilder()
                    .setParent(LocationName.of(name.getProject(), name.getLocation()).toString())
                    .setCatalogId(name.getCatalog())
                    .setCatalog(catalog)
                    .build()),
        name.getCatalog());
  }

  public Catalog catalog(CatalogName name) {
    return convertException(
        () -> {
          try {
            return stub.getCatalog(GetCatalogRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Namespace does not exist: %s (or permission denied)", name.getCatalog());
          }
        },
        name.getCatalog());
  }

  public void deleteCatalog(CatalogName name) {
    convertException(
        () -> {
          try {
            stub.deleteCatalog(DeleteCatalogRequest.newBuilder().setName(name.toString()).build());
            return Empty.getDefaultInstance();
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Namespace does not exist: %s (or permission denied)", name.getCatalog());
          }
        },
        name.getCatalog());
  }

  public Database createDatabase(DatabaseName name, Database db) {
    return convertException(
        () ->
            stub.createDatabase(
                CreateDatabaseRequest.newBuilder()
                    .setParent(
                        CatalogName.of(name.getProject(), name.getLocation(), name.getCatalog())
                            .toString())
                    .setDatabaseId(name.getDatabase())
                    .setDatabase(db)
                    .build()),
        name.getDatabase());
  }

  public Database database(DatabaseName name) {
    return convertException(
        () -> {
          try {
            return stub.getDatabase(
                GetDatabaseRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Namespace does not exist: %s (or permission denied)", name.getDatabase());
          }
        },
        name.getDatabase());
  }

  public Database updateDatabaseParameters(DatabaseName name, Map<String, String> parameters) {
    Database.Builder builder = Database.newBuilder().setName(name.toString());
    builder.getHiveOptionsBuilder().putAllParameters(parameters);
    return convertException(
        () -> {
          try {
            return stub.updateDatabase(
                UpdateDatabaseRequest.newBuilder()
                    .setDatabase(builder)
                    .setUpdateMask(FieldMask.newBuilder().addPaths("hive_options.parameters"))
                    .build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Namespace does not exist: %s (or permission denied)", name.getDatabase());
          }
        },
        name.getDatabase());
  }

  public Iterable<Database> listDatabases(CatalogName name) {
    return convertException(
        () ->
            stub.listDatabases(ListDatabasesRequest.newBuilder().setParent(name.toString()).build())
                .iterateAll(),
        name.getCatalog());
  }

  public void deleteDatabase(DatabaseName name) {
    convertException(
        () -> {
          try {
            stub.deleteDatabase(
                DeleteDatabaseRequest.newBuilder().setName(name.toString()).build());
            return Empty.getDefaultInstance();
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Namespace does not exist: %s (or permission denied)", name.getDatabase());
          }
        },
        name.getDatabase());
  }

  public Table createTable(TableName name, Table table) {
    return convertException(
        () -> {
          try {
            return stub.createTable(
                CreateTableRequest.newBuilder()
                    .setParent(getDatabase(name).toString())
                    .setTableId(name.getTable())
                    .setTable(table)
                    .build());
          } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
            throw new AlreadyExistsException(e, "Table already exists: %s", name.getTable());
          }
        },
        name.getTable());
  }

  public Table table(TableName name) {
    if (name.getTable().isEmpty()) {
      throw new NoSuchTableException("BigLake API does not allow tables with empty ID");
    }
    return convertException(
        () -> {
          try {
            return stub.getTable(GetTableRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table does not exist: %s (or permission denied)", name.getTable());
          }
        },
        name.getTable());
  }

  public Table updateTableParameters(TableName name, Map<String, String> parameters, String etag) {
    Table.Builder builder = Table.newBuilder().setName(name.toString()).setEtag(etag);
    builder.getHiveOptionsBuilder().putAllParameters(parameters);
    return convertException(
        () -> {
          try {
            return stub.updateTable(
                UpdateTableRequest.newBuilder()
                    .setTable(builder)
                    .setUpdateMask(FieldMask.newBuilder().addPaths("hive_options.parameters"))
                    .build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table does not exist: %s (or permission denied)", name.getTable());
          }
        },
        name.getTable());
  }

  public Table renameTable(TableName name, TableName newName) {
    return convertException(
        () -> {
          try {
            return stub.renameTable(
                RenameTableRequest.newBuilder()
                    .setName(name.toString())
                    .setNewName(newName.toString())
                    .build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table does not exist: %s (or permission denied)", name.getTable());
          } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
            throw new AlreadyExistsException(e, "Table already exists: %s", newName.getTable());
          }
        },
        name.getTable());
  }

  public Table deleteTable(TableName name) {
    return convertException(
        () -> {
          try {
            return stub.deleteTable(
                DeleteTableRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table does not exist: %s (or permission denied)", name.getTable());
          }
        },
        name.getTable());
  }

  public Iterable<Table> listTables(DatabaseName name) {
    return convertException(
        () ->
            stub.listTables(ListTablesRequest.newBuilder().setParent(name.toString()).build())
                .iterateAll(),
        name.getDatabase());
  }

  @Override
  public void close() {
    stub.close();
  }

  // Converts BigLake API errors to Iceberg errors.
  private <T> T convertException(Supplier<T> result, String resourceId) {
    try {
      return result.get();
    } catch (PermissionDeniedException e) {
      throw new NotAuthorizedException(e, "Permission denied");
    } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
      // "Table already exists" error should be caught earlier.
      throw new AlreadyExistsException(e, "Namespace already exists: %s", resourceId);
    }
  }

  private static DatabaseName getDatabase(TableName tableName) {
    return DatabaseName.of(
        tableName.getProject(),
        tableName.getLocation(),
        tableName.getCatalog(),
        tableName.getDatabase());
  }
}
