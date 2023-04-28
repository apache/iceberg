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
import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;

/** A client implementation of Google BigLake service. */
final class BigLakeClientImpl implements BigLakeClient {

  private final String projectId;
  private final String location;
  private final MetastoreServiceClient stub;

  /**
   * Constructs a client of Google BigLake Service.
   *
   * @param biglakeEndpoint BigLake service gRPC endpoint, e.g., "biglake.googleapis.com:443"
   * @param projectId GCP project ID
   * @param location GCP region supported by BigLake, e.g., "us"
   */
  BigLakeClientImpl(String biglakeEndpoint, String projectId, String location) throws IOException {
    this.projectId = projectId;
    this.location = location;
    this.stub =
        MetastoreServiceClient.create(
            MetastoreServiceSettings.newBuilder().setEndpoint(biglakeEndpoint).build());
  }

  @Override
  public Catalog createCatalog(CatalogName name, Catalog catalog) {
    return convertException(
        () ->
            stub.createCatalog(
                CreateCatalogRequest.newBuilder()
                    .setParent(LocationName.of(name.getProject(), name.getLocation()).toString())
                    .setCatalogId(name.getCatalog())
                    .setCatalog(catalog)
                    .build()));
  }

  @Override
  public Catalog getCatalog(CatalogName name) {
    return convertException(
        () -> {
          try {
            return stub.getCatalog(GetCatalogRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Catalog %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
  public void deleteCatalog(CatalogName name) {
    convertException(
        () -> {
          try {
            stub.deleteCatalog(DeleteCatalogRequest.newBuilder().setName(name.toString()).build());
            return Empty.getDefaultInstance();
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Catalog %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
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
                    .build()));
  }

  @Override
  public Database getDatabase(DatabaseName name) {
    return convertException(
        () -> {
          try {
            return stub.getDatabase(
                GetDatabaseRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Database %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
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
                e, "Database %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
  public Iterable<Database> listDatabases(CatalogName name) {
    return convertException(
        () ->
            stub.listDatabases(ListDatabasesRequest.newBuilder().setParent(name.toString()).build())
                .iterateAll());
  }

  @Override
  public void deleteDatabase(DatabaseName name) {
    convertException(
        () -> {
          try {
            stub.deleteDatabase(
                DeleteDatabaseRequest.newBuilder().setName(name.toString()).build());
            return Empty.getDefaultInstance();
          } catch (PermissionDeniedException e) {
            throw new NoSuchNamespaceException(
                e, "Database %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
  public Table createTable(TableName name, Table table) {
    return convertException(
        () ->
            stub.createTable(
                CreateTableRequest.newBuilder()
                    .setParent(getDatabase(name).toString())
                    .setTableId(name.getTable())
                    .setTable(table)
                    .build()));
  }

  @Override
  public Table getTable(TableName name) {
    return convertException(
        () -> {
          try {
            return stub.getTable(GetTableRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
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
                e, "Table %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
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
                e, "Table %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
  public Table deleteTable(TableName name) {
    return convertException(
        () -> {
          try {
            return stub.deleteTable(
                DeleteTableRequest.newBuilder().setName(name.toString()).build());
          } catch (PermissionDeniedException e) {
            throw new NoSuchTableException(
                e, "Table %s not found or permission denied", name.toString());
          }
        });
  }

  @Override
  public Iterable<Table> listTables(DatabaseName name) {
    return convertException(
        () ->
            stub.listTables(ListTablesRequest.newBuilder().setParent(name.toString()).build())
                .iterateAll());
  }

  // Converts BigLake API errors to Iceberg errors.
  private <T> T convertException(Supplier<T> result) {
    try {
      return result.get();
    } catch (PermissionDeniedException e) {
      throw new NotAuthorizedException(e, "Not authorized to call BigLake API");
    } catch (com.google.api.gax.rpc.AlreadyExistsException e) {
      throw new AlreadyExistsException(e, "BigLake resource already exists");
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
