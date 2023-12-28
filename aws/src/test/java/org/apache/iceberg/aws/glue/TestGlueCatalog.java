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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.LockManagers;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseResponse;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseResponse;

public class TestGlueCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "glue";
  private GlueClient glue;
  private GlueCatalog glueCatalog;

  @BeforeEach
  public void before() {
    glue = Mockito.mock(GlueClient.class);
    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
  }

  @Test
  public void testConstructorEmptyWarehousePath() {
    GlueCatalog catalog = new GlueCatalog();
    catalog.initialize(
        CATALOG_NAME,
        null,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doThrow(EntityNotFoundException.builder().build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Assertions.assertThatThrownBy(
            () -> catalog.createTable(TableIdentifier.of("db", "table"), new Schema()))
        .hasMessageContaining(
            "Cannot derive default warehouse location, warehouse path must not be null or empty")
        .isInstanceOf(ValidationException.class);
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    GlueCatalog catalogWithSlash = new GlueCatalog();
    catalogWithSlash.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = catalogWithSlash.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assertions.assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoDbUri() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assertions.assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationDbUri() {
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Assertions.assertThat(location).isEqualTo("s3://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationDbUriTrailingSlash() {
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db/").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    String location = glueCatalog.defaultWarehouseLocation(TableIdentifier.of("db", "table"));

    Assertions.assertThat(location).isEqualTo("s3://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationCustomCatalogId() {
    GlueCatalog catalogWithCustomCatalogId = new GlueCatalog();
    String catalogId = "myCatalogId";
    AwsProperties awsProperties = new AwsProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    awsProperties.setGlueCatalogId(catalogId);
    catalogWithCustomCatalogId.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        awsProperties,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());

    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db").locationUri("s3://bucket2/db").build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    catalogWithCustomCatalogId.defaultWarehouseLocation(TableIdentifier.of("db", "table"));
    Mockito.verify(glue)
        .getDatabase(
            Mockito.argThat((GetDatabaseRequest req) -> req.catalogId().equals(catalogId)));
  }

  @Test
  public void testListTables() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(
                    Table.builder()
                        .databaseName("db1")
                        .name("t1")
                        .parameters(
                            ImmutableMap.of(
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t2")
                        .parameters(
                            ImmutableMap.of(
                                "key",
                                "val",
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t3")
                        .parameters(
                            ImmutableMap.of(
                                "key",
                                "val",
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                "wrongVal"))
                        .build(),
                    Table.builder()
                        .databaseName("db1")
                        .name("t4")
                        .parameters(ImmutableMap.of("key", "val"))
                        .build(),
                    Table.builder().databaseName("db1").name("t5").parameters(null).build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Assertions.assertThat(glueCatalog.listTables(Namespace.of("db1")))
        .isEqualTo(
            Lists.newArrayList(TableIdentifier.of("db1", "t1"), TableIdentifier.of("db1", "t2")));
  }

  @Test
  public void testListTablesPagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                if (counter.decrementAndGet() > 0) {
                  return GetTablesResponse.builder()
                      .tableList(
                          Table.builder()
                              .databaseName("db1")
                              .name(UUID.randomUUID().toString().replace("-", ""))
                              .parameters(
                                  ImmutableMap.of(
                                      BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                      BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                              .build())
                      .nextToken("token")
                      .build();
                } else {
                  return GetTablesResponse.builder()
                      .tableList(
                          Table.builder()
                              .databaseName("db1")
                              .name("tb1")
                              .parameters(
                                  ImmutableMap.of(
                                      BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                      BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                              .build())
                      .build();
                }
              }
            })
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Assertions.assertThat(glueCatalog.listTables(Namespace.of("db1"))).hasSize(10);
  }

  @Test
  public void testDropTable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder().databaseName("db1").name("t1").parameters(properties).build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteTableResponse.builder().build())
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
  }

  @Test
  public void testRenameTable() {
    AtomicInteger counter = new AtomicInteger(1);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder().databaseName("db1").name("t1").parameters(properties).build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                counter.decrementAndGet();
                return DeleteTableResponse.builder().build();
              }
            })
        .when(glue)
        .deleteTable(Mockito.any(DeleteTableRequest.class));
    glueCatalog.dropTable(TableIdentifier.of("db1", "t1"));
    Assertions.assertThat(counter.get()).isEqualTo(0);
  }

  @Test
  public void testRenameTableWithStorageDescriptor() {
    AtomicInteger counter = new AtomicInteger(1);

    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(
        BaseMetastoreTableOperations.TABLE_TYPE_PROP,
        BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE);

    Map<String, String> storageDescriptorParameters = Maps.newHashMap();
    storageDescriptorParameters.put("key_0", "value_0");

    StorageDescriptor storageDescriptor =
        StorageDescriptor.builder().parameters(storageDescriptorParameters).build();

    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .databaseName("db")
                        .name("t_renamed")
                        .parameters(parameters)
                        .storageDescriptor(storageDescriptor)
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                CreateTableRequest createTableRequest =
                    (CreateTableRequest) invocation.getArguments()[0];
                if (createTableRequest.tableInput().storageDescriptor().hasParameters()) {
                  counter.decrementAndGet();
                }
                return CreateTableResponse.builder().build();
              }
            })
        .when(glue)
        .createTable(Mockito.any(CreateTableRequest.class));

    glueCatalog.renameTable(TableIdentifier.of("db", "t"), TableIdentifier.of("db", "x_renamed"));
    Assertions.assertThat(counter.get()).isEqualTo(0);
  }

  @Test
  public void testCreateNamespace() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue)
        .createDatabase(Mockito.any(CreateDatabaseRequest.class));
    glueCatalog.createNamespace(Namespace.of("db"));
  }

  @Test
  public void testCreateNamespaceBadName() {
    Mockito.doReturn(CreateDatabaseResponse.builder().build())
        .when(glue)
        .createDatabase(Mockito.any(CreateDatabaseRequest.class));
    List<Namespace> invalidNamespaces =
        Lists.newArrayList(Namespace.of("db-1"), Namespace.of("db", "db2"));

    for (Namespace namespace : invalidNamespaces) {
      Assertions.assertThatThrownBy(() -> glueCatalog.createNamespace(namespace))
          .isInstanceOf(ValidationException.class)
          .hasMessageStartingWith("Cannot convert namespace")
          .hasMessageEndingWith(
              "to Glue database name, "
                  + "because it must be 1-252 chars of lowercase letters, numbers, underscore");
    }
  }

  @Test
  public void testListAllNamespaces() {
    Mockito.doReturn(
            GetDatabasesResponse.builder()
                .databaseList(
                    Database.builder().name("db1").build(), Database.builder().name("db2").build())
                .build())
        .when(glue)
        .getDatabases(Mockito.any(GetDatabasesRequest.class));
    Assertions.assertThat(glueCatalog.listNamespaces())
        .isEqualTo(Lists.newArrayList(Namespace.of("db1"), Namespace.of("db2")));
  }

  @Test
  public void testListNamespacesPagination() {
    AtomicInteger counter = new AtomicInteger(10);
    Mockito.doAnswer(
            new Answer() {
              @Override
              public Object answer(InvocationOnMock invocation) throws Throwable {
                if (counter.decrementAndGet() > 0) {
                  return GetDatabasesResponse.builder()
                      .databaseList(
                          Database.builder()
                              .name(UUID.randomUUID().toString().replace("-", ""))
                              .build())
                      .nextToken("token")
                      .build();
                } else {
                  return GetDatabasesResponse.builder()
                      .databaseList(Database.builder().name("db").build())
                      .build();
                }
              }
            })
        .when(glue)
        .getDatabases(Mockito.any(GetDatabasesRequest.class));
    Assertions.assertThat(glueCatalog.listNamespaces()).hasSize(10);
  }

  @Test
  public void testListNamespacesWithNameShouldReturnItself() {
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Assertions.assertThat(glueCatalog.listNamespaces(Namespace.of("db1")))
        .as("list self should return empty list")
        .isEmpty();
  }

  @Test
  public void testListNamespacesBadName() {

    Assertions.assertThatThrownBy(() -> glueCatalog.listNamespaces(Namespace.of("db-1")))
        .isInstanceOf(ValidationException.class)
        .hasMessage(
            "Cannot convert namespace db-1 to Glue database name, "
                + "because it must be 1-252 chars of lowercase letters, numbers, underscore");
  }

  @Test
  public void testLoadNamespaceMetadata() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    parameters.put(IcebergToGlueConverter.GLUE_DB_LOCATION_KEY, "s3://bucket2/db");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(
                    Database.builder()
                        .name("db1")
                        .parameters(parameters)
                        .locationUri("s3://bucket2/db/")
                        .build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Assertions.assertThat(glueCatalog.loadNamespaceMetadata(Namespace.of("db1")))
        .isEqualTo(parameters);
  }

  @Test
  public void testDropNamespace() {
    Mockito.doReturn(GetTablesResponse.builder().build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));
    glueCatalog.dropNamespace(Namespace.of("db1"));
  }

  @Test
  public void testDropNamespaceThatContainsOnlyIcebergTable() {
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(
                    Table.builder()
                        .databaseName("db1")
                        .name("t1")
                        .parameters(
                            ImmutableMap.of(
                                BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                                BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                        .build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));

    Assertions.assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of("db1")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Cannot drop namespace db1 because it still contains Iceberg tables");
  }

  @Test
  public void testDropNamespaceThatContainsNonIcebergTable() {
    Mockito.doReturn(
            GetTablesResponse.builder()
                .tableList(Table.builder().databaseName("db1").name("t1").build())
                .build())
        .when(glue)
        .getTables(Mockito.any(GetTablesRequest.class));
    Mockito.doReturn(
            GetDatabaseResponse.builder().database(Database.builder().name("db1").build()).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(DeleteDatabaseResponse.builder().build())
        .when(glue)
        .deleteDatabase(Mockito.any(DeleteDatabaseRequest.class));

    Assertions.assertThatThrownBy(() -> glueCatalog.dropNamespace(Namespace.of("db1")))
        .isInstanceOf(NamespaceNotEmptyException.class)
        .hasMessage("Cannot drop namespace db1 because it still contains non-Iceberg tables");
  }

  @Test
  public void testSetProperties() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db1").parameters(parameters).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue)
        .updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.setProperties(Namespace.of("db1"), parameters);
  }

  @Test
  public void testRemoveProperties() {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put("key", "val");
    Mockito.doReturn(
            GetDatabaseResponse.builder()
                .database(Database.builder().name("db1").parameters(parameters).build())
                .build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));
    Mockito.doReturn(UpdateDatabaseResponse.builder().build())
        .when(glue)
        .updateDatabase(Mockito.any(UpdateDatabaseRequest.class));
    glueCatalog.removeProperties(Namespace.of("db1"), Sets.newHashSet("key"));
  }

  @Test
  public void testTablePropsDefinedAtCatalogLevel() {
    ImmutableMap<String, String> catalogProps =
        ImmutableMap.of(
            "table-default.key1", "catalog-default-key1",
            "table-default.key2", "catalog-default-key2",
            "table-default.key3", "catalog-default-key3",
            "table-override.key3", "catalog-override-key3",
            "table-override.key4", "catalog-override-key4");
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        catalogProps);
    Map<String, String> properties = glueCatalog.properties();
    Assertions.assertThat(properties)
        .isNotEmpty()
        .containsEntry("table-default.key1", "catalog-default-key1")
        .containsEntry("table-default.key2", "catalog-default-key2")
        .containsEntry("table-default.key3", "catalog-default-key3")
        .containsEntry("table-override.key3", "catalog-override-key3")
        .containsEntry("table-override.key4", "catalog-override-key4");
  }

  @Test
  public void testValidateIdentifierSkipNameValidation() {
    AwsProperties props = new AwsProperties();
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties();
    props.setGlueCatalogSkipNameValidation(true);
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        props,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
    Assertions.assertThat(glueCatalog.isValidIdentifier(TableIdentifier.parse("db-1.a-1")))
        .isEqualTo(true);
  }

  @Test
  public void testTableLevelS3TagProperties() {
    Map<String, String> properties =
        ImmutableMap.of(
            S3FileIOProperties.WRITE_TABLE_TAG_ENABLED,
            "true",
            S3FileIOProperties.WRITE_NAMESPACE_TAG_ENABLED,
            "true");
    AwsProperties awsProperties = new AwsProperties(properties);
    S3FileIOProperties s3FileIOProperties = new S3FileIOProperties(properties);
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        awsProperties,
        s3FileIOProperties,
        glue,
        LockManagers.defaultLockManager(),
        properties);
    GlueTableOperations glueTableOperations =
        (GlueTableOperations)
            glueCatalog.newTableOps(TableIdentifier.of(Namespace.of("db"), "table"));
    Map<String, String> tableCatalogProperties = glueTableOperations.tableCatalogProperties();

    Assertions.assertThat(tableCatalogProperties)
        .containsEntry(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(S3FileIOProperties.S3_TAG_ICEBERG_TABLE),
            "table")
        .containsEntry(
            S3FileIOProperties.WRITE_TAGS_PREFIX.concat(
                S3FileIOProperties.S3_TAG_ICEBERG_NAMESPACE),
            "db");
  }
}
