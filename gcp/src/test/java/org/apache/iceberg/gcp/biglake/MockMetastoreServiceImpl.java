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
import com.google.cloud.bigquery.biglake.v1.CreateCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.CreateDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.CreateTableRequest;
import com.google.cloud.bigquery.biglake.v1.Database;
import com.google.cloud.bigquery.biglake.v1.DeleteCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.DeleteDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.DeleteTableRequest;
import com.google.cloud.bigquery.biglake.v1.GetCatalogRequest;
import com.google.cloud.bigquery.biglake.v1.GetDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.GetTableRequest;
import com.google.cloud.bigquery.biglake.v1.ListCatalogsRequest;
import com.google.cloud.bigquery.biglake.v1.ListCatalogsResponse;
import com.google.cloud.bigquery.biglake.v1.ListDatabasesRequest;
import com.google.cloud.bigquery.biglake.v1.ListDatabasesResponse;
import com.google.cloud.bigquery.biglake.v1.ListTablesRequest;
import com.google.cloud.bigquery.biglake.v1.ListTablesResponse;
import com.google.cloud.bigquery.biglake.v1.MetastoreServiceGrpc.MetastoreServiceImplBase;
import com.google.cloud.bigquery.biglake.v1.RenameTableRequest;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.UpdateDatabaseRequest;
import com.google.cloud.bigquery.biglake.v1.UpdateTableRequest;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/** Mock the BigLake Metastore service for testing. */
public class MockMetastoreServiceImpl extends MetastoreServiceImplBase {

  private final Map<String, Catalog> catalogs;
  private final Map<String, Database> dbs;
  private final Map<String, Table> tables;

  public MockMetastoreServiceImpl() {
    this.catalogs = new HashMap<String, Catalog>();
    this.dbs = new HashMap<String, Database>();
    this.tables = new HashMap<String, Table>();
  }

  public void reset() {
    this.catalogs.clear();
    this.dbs.clear();
    this.tables.clear();
  }

  @Override
  public void createCatalog(
      CreateCatalogRequest request, StreamObserver<Catalog> responseObserver) {
    String name = String.format("%s/catalogs/%s", request.getParent(), request.getCatalogId());
    if (catalogs.containsKey(name)) {
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription(String.format("Catalog already exists: %s", request.getCatalogId()))
              .asRuntimeException());
      return;
    }

    Catalog catalog = request.getCatalog().toBuilder().setName(name).build();
    catalogs.put(name, catalog);
    responseObserver.onNext(catalog);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteCatalog(
      DeleteCatalogRequest request, StreamObserver<Catalog> responseObserver) {
    String name = request.getName();
    if (catalogs.containsKey(name)) {
      responseObserver.onNext(catalogs.remove(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Catalog %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void getCatalog(GetCatalogRequest request, StreamObserver<Catalog> responseObserver) {
    String name = request.getName();
    if (catalogs.containsKey(name)) {
      responseObserver.onNext(catalogs.get(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Catalog %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void listCatalogs(
      ListCatalogsRequest request, StreamObserver<ListCatalogsResponse> responseObserver) {
    List<Catalog> result =
        catalogs.values().stream()
            .filter(c -> c.getName().startsWith(request.getParent()))
            .collect(ImmutableList.toImmutableList());
    responseObserver.onNext(ListCatalogsResponse.newBuilder().addAllCatalogs(result).build());
    responseObserver.onCompleted();
  }

  @Override
  public void createDatabase(
      CreateDatabaseRequest request, StreamObserver<Database> responseObserver) {
    if (request.getDatabaseId().contains("/")) {
      responseObserver.onError(
          Status.INVALID_ARGUMENT.withDescription("Database ID is invalid").asRuntimeException());
      return;
    }

    String name = String.format("%s/databases/%s", request.getParent(), request.getDatabaseId());
    if (dbs.containsKey(name)) {
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription(
                  String.format("Database already exists: %s", request.getDatabaseId()))
              .asRuntimeException());
      return;
    }

    Database db = request.getDatabase().toBuilder().setName(name).build();
    dbs.put(name, db);
    responseObserver.onNext(db);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteDatabase(
      DeleteDatabaseRequest request, StreamObserver<Database> responseObserver) {
    String name = request.getName();
    if (dbs.containsKey(name)) {
      responseObserver.onNext(dbs.remove(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Database %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void updateDatabase(
      UpdateDatabaseRequest request, StreamObserver<Database> responseObserver) {
    String name = request.getDatabase().getName();
    if (dbs.containsKey(name)) {
      Database.Builder dbBuilder = dbs.get(name).toBuilder();
      dbBuilder
          .getHiveOptionsBuilder()
          .clearParameters()
          .putAllParameters(request.getDatabase().getHiveOptions().getParameters());
      Database newDb = dbBuilder.build();
      dbs.put(name, newDb);
      responseObserver.onNext(newDb);
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Database %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void getDatabase(GetDatabaseRequest request, StreamObserver<Database> responseObserver) {
    String name = request.getName();
    if (dbs.containsKey(name)) {
      responseObserver.onNext(dbs.get(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Database %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void listDatabases(
      ListDatabasesRequest request, StreamObserver<ListDatabasesResponse> responseObserver) {
    List<Database> result =
        dbs.values().stream()
            .filter(db -> db.getName().startsWith(request.getParent()))
            .collect(ImmutableList.toImmutableList());
    responseObserver.onNext(ListDatabasesResponse.newBuilder().addAllDatabases(result).build());
    responseObserver.onCompleted();
  }

  @Override
  public void createTable(CreateTableRequest request, StreamObserver<Table> responseObserver) {
    String name = String.format("%s/tables/%s", request.getParent(), request.getTableId());
    if (tables.containsKey(name)) {
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription(String.format("Table already exists: %s", request.getTableId()))
              .asRuntimeException());
      return;
    }

    Table table =
        request.getTable().toBuilder().setName(name).setEtag(UUID.randomUUID().toString()).build();
    tables.put(name, table);
    responseObserver.onNext(table);
    responseObserver.onCompleted();
  }

  @Override
  public void deleteTable(DeleteTableRequest request, StreamObserver<Table> responseObserver) {
    String name = request.getName();
    if (tables.containsKey(name)) {
      responseObserver.onNext(tables.remove(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Table %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void updateTable(UpdateTableRequest request, StreamObserver<Table> responseObserver) {
    String name = request.getTable().getName();
    if (tables.containsKey(name)) {
      Table.Builder tableBuilder = tables.get(name).toBuilder();
      tableBuilder
          .getHiveOptionsBuilder()
          .clearParameters()
          .putAllParameters(request.getTable().getHiveOptions().getParameters());
      Table newTable = tableBuilder.setEtag(UUID.randomUUID().toString()).build();
      tables.put(name, newTable);
      responseObserver.onNext(newTable);
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Table %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void renameTable(RenameTableRequest request, StreamObserver<Table> responseObserver) {
    if (!tables.containsKey(request.getName())) {
      responseObserver.onError(
          Status.PERMISSION_DENIED
              .withDescription("Table does not exist or permission denied")
              .asRuntimeException());
      return;
    }

    if (tables.containsKey(request.getNewName())) {
      responseObserver.onError(
          Status.ALREADY_EXISTS
              .withDescription("Table does not exist or permission denied")
              .asRuntimeException());
      return;
    }

    Table table = tables.get(request.getName());
    Table newTable =
        table
            .toBuilder()
            .setName(request.getNewName())
            .setEtag(UUID.randomUUID().toString())
            .build();
    tables.put(request.getNewName(), newTable);
    tables.remove(request.getName());
    responseObserver.onNext(newTable);
    responseObserver.onCompleted();
  }

  @Override
  public void getTable(GetTableRequest request, StreamObserver<Table> responseObserver) {
    String name = request.getName();
    if (tables.containsKey(name)) {
      responseObserver.onNext(tables.get(name));
      responseObserver.onCompleted();
      return;
    }

    responseObserver.onError(
        Status.PERMISSION_DENIED
            .withDescription(String.format("Table %s does not exist or permission denied", name))
            .asRuntimeException());
  }

  @Override
  public void listTables(
      ListTablesRequest request, StreamObserver<ListTablesResponse> responseObserver) {
    List<Table> result =
        tables.values().stream()
            .filter(t -> t.getName().startsWith(request.getParent()))
            .collect(ImmutableList.toImmutableList());
    responseObserver.onNext(ListTablesResponse.newBuilder().addAllTables(result).build());
    responseObserver.onCompleted();
  }
}
