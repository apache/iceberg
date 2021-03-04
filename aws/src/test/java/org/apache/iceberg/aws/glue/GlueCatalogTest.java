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

package org.apache.iceberg.aws.glue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

public class GlueCatalogTest {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "glue";
  private GlueClient glue;
  private GlueCatalog glueCatalog;

  @Before
  public void before() {
    glue = Mockito.mock(GlueClient.class);
    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(CATALOG_NAME, WAREHOUSE_PATH, new AwsProperties(), glue,
        LockManagers.defaultLockManager(), null);
  }

  @Test
  public void constructor_emptyWarehousePath() {
    AssertHelpers.assertThrows("warehouse path cannot be null",
        IllegalArgumentException.class,
        "Cannot initialize GlueCatalog because warehousePath must not be null",
        () -> {
            GlueCatalog catalog = new GlueCatalog();
            catalog.initialize(CATALOG_NAME, null, new AwsProperties(), glue,
                LockManagers.defaultLockManager(), null);
        });
  }

  @Test
  public void constructor_warehousePathWithEndSlash() {
    GlueCatalog catalogWithSlash = new GlueCatalog();
    catalogWithSlash.initialize(
        CATALOG_NAME, WAREHOUSE_PATH + "/", new AwsProperties(), glue, LockManagers.defaultLockManager(), null);
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = catalogWithSlash.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assert.assertEquals(WAREHOUSE_PATH + "/db.db/table", location);
  }

  @Test
  public void defaultWarehouseLocation_noDbUri() {
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assert.assertEquals(WAREHOUSE_PATH + "/db.db/table", location);
  }

  @Test
  public void defaultWarehouseLocation_dbUri() {
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db").locationUri("s3://bucket2/db").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assert.assertEquals("s3://bucket2/db/table", location);
  }

  @Test
  public void listTables() {
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(GetTablesResponse.builder()
        .tableList(
            Table.builder().databaseName("db1").name("t1").parameters(
                ImmutableMap.of(
                    BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
                )
            ).build(),
            Table.builder().databaseName("db1").name("t2").parameters(
                ImmutableMap.of(
                    "key", "val",
                    BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
                )
            ).build(),
            Table.builder().databaseName("db1").name("t3").parameters(
                ImmutableMap.of(
                    "key", "val",
                    BaseMetastoreTableOperations.TABLE_TYPE_PROP, "wrongVal"
                )
            ).build(),
            Table.builder().databaseName("db1").name("t4").parameters(
                ImmutableMap.of(
                    "key", "val"
                )
            ).build(),
            Table.builder().databaseName("db1").name("t5").parameters(null).build()
        ).build())
        .when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Assert.assertEquals(
        Lists.newArrayList(
            TableIdentifier.of("db1", "t1"),
            TableIdentifier.of("db1", "t2")
        ),
        glueCatalog.listTables(Namespace.of("db1"))
    );
  }

  @Test
  public void listTables_pagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (counter.decrementAndGet() > 0) {
          return GetTablesResponse.builder()
              .tableList(
                  Table.builder()
                      .databaseName("db1")
                      .name(UUID.randomUUID().toString().replace("-", ""))
                      .parameters(ImmutableMap.of(
                          BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                          BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
                      ))
                      .build()
              )
              .nextToken("token")
              .build();
        } else {
          return GetTablesResponse.builder()
              .tableList(Table.builder().databaseName("db1").name("tb1").parameters(ImmutableMap.of(
                  BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                  BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
              )).build())
              .build();
        }
      }
    }).when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Assert.assertEquals(10, glueCatalog.listTables(Namespace.of("db1")).size());
  }

  @Test
  public void dropTable() {
    Map<String, String> properties = new HashMap<>();
    properties.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(GetTableResponse.builder()
        .table(Table.builder().databaseName("db1").name("t1").parameters(properties).build()).build())
        .when(glue).getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue).deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
  }

  @Test
  public void renameTable() {
    AtomicInteger counter = new AtomicInteger(1);
    Map<String, String> properties = new HashMap<>();
    properties.put(BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(GetTableResponse.builder()
        .table(Table.builder().databaseName("db1").name("t1").parameters(properties).build()).build())
        .when(glue).getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        counter.decrementAndGet();
        return DeleteTableResponse.builder().build();
      }
    }).when(glue).deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
    Assert.assertEquals(0, counter.get());
  }

  @Test
  public void createNamespace() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue).createDatabase(Mockito.any(CreateDatabaseRequest.class));
    glueCatalog.createNamespace(Namespace.of("db"));
  }

  @Test
  public void createNamespace_badName() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue).createDatabase(Mockito.any(CreateDatabaseRequest.class));
    List<Namespace> invalidNamespaces = Lists.newArrayList(
        Namespace.of("db-1"),
        Namespace.of("db", "db2")
    );

    for (Namespace namespace : invalidNamespaces) {
      AssertHelpers.assertThrows("should not create namespace with invalid or nested names",
          ValidationException.class,
          "Cannot convert namespace",
          () -> glueCatalog.createNamespace(namespace));
    }
  }

  @Test
  public void listNamespaces_all() {
    Mockito.doReturn(GetDatabasesResponse.builder()
        .databaseList(
            Database.builder().name("db1").build(),
            Database.builder().name("db2").build()
        ).build())
        .when(glue).getDatabases(Mockito.any(GetDatabasesRequest.class));
    Assert.assertEquals(
        Lists.newArrayList(
            Namespace.of("db1"),
            Namespace.of("db2")
        ),
        glueCatalog.listNamespaces()
    );
  }

  @Test
  public void listNamespaces_pagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (counter.decrementAndGet() > 0) {
          return GetDatabasesResponse.builder()
              .databaseList(
                  Database.builder().name(UUID.randomUUID().toString().replace("-", "")).build()
              )
              .nextToken("token")
              .build();
        } else {
          return GetDatabasesResponse.builder()
              .databaseList(Database.builder().name("db").build())
              .build();
        }
      }
    }).when(glue).getDatabases(Mockito.any(GetDatabasesRequest.class));
    Assert.assertEquals(10, glueCatalog.listNamespaces().size());
  }

  @Test
  public void listNamespaces_self() {
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Assert.assertEquals(
        "list self should return empty list",
        Lists.newArrayList(),
        glueCatalog.listNamespaces(Namespace.of("db1"))
    );
  }

  @Test
  public void listNamespaces_selfInvalid() {
    AssertHelpers.assertThrows("table name invalid",
        ValidationException.class,
        "Cannot convert namespace",
        () -> glueCatalog.listNamespaces(Namespace.of("db-1")));
  }

  @Test
  public void loadNamespaceMetadata() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key", "val");
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1")
            .parameters(parameters)
            .build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Assert.assertEquals(parameters, glueCatalog.loadNamespaceMetadata(Namespace.of("db1")));
  }

  @Test
  public void dropNamespace() {
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue).deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));
    glueCatalog.dropNamespace(Namespace.of("db1"));
  }

  @Test
  public void dropNamespace_notEmpty_containsIcebergTable() {
    Mockito.doReturn(GetTablesResponse.builder()
        .tableList(
            Table.builder().databaseName("db1").name("t1").parameters(
                ImmutableMap.of(
                    BaseMetastoreTableOperations.TABLE_TYPE_PROP, BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE
                )
            ).build()
        ).build())
        .when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue).deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));
    AssertHelpers.assertThrows("namespace should not be dropped when still has Iceberg table",
        NamespaceNotEmptyException.class,
        "still contains Iceberg tables",
        () -> glueCatalog.dropNamespace(Namespace.of("db1")));
  }

  @Test
  public void dropNamespace_notEmpty_containsNonIcebergTable() {
    Mockito.doReturn(GetTablesResponse.builder()
        .tableList(
            Table.builder().databaseName("db1").name("t1").build()
        ).build())
        .when(glue).getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1").build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue).deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));
    AssertHelpers.assertThrows("namespace should not be dropped when still has non-Iceberg table",
        NamespaceNotEmptyException.class,
        "still contains non-Iceberg tables",
        () -> glueCatalog.dropNamespace(Namespace.of("db1")));
  }

  @Test
  public void setProperties() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key", "val");
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1")
            .parameters(parameters)
            .build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue).updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.setProperties(Namespace.of("db1"), parameters);
  }

  @Test
  public void removeProperties() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("key", "val");
    Mockito.doReturn(GetDatabaseResponse.builder()
        .database(Database.builder().name("db1")
            .parameters(parameters)
            .build()).build())
        .when(glue).getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue).updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.removeProperties(Namespace.of("db1"), Sets.newHashSet("key"));
  }
}
