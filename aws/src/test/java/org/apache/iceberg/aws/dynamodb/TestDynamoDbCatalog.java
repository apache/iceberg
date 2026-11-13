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
package org.apache.iceberg.aws.dynamodb;

import static org.apache.iceberg.aws.dynamodb.DynamoDbCatalog.toPropertyCol;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

public class TestDynamoDbCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "dynamodb";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of("db", "table");

  private DynamoDbClient dynamo;
  private DynamoDbCatalog dynamoCatalog;

  @BeforeEach
  public void before() {
    dynamo = Mockito.mock(DynamoDbClient.class);
    dynamoCatalog = new DynamoDbCatalog();
    dynamoCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        dynamo,
        null,
        false /* uniqTableLocation */);
  }

  @Test
  public void testConstructorWarehousePathWithEndSlash() {
    DynamoDbCatalog catalogWithSlash = new DynamoDbCatalog();
    catalogWithSlash.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH + "/",
        new AwsProperties(),
        dynamo,
        null,
        false /* uniqTableLocation */);
    Mockito.doReturn(GetItemResponse.builder().item(Maps.newHashMap()).build())
        .when(dynamo)
        .getItem(any(GetItemRequest.class));
    String location = catalogWithSlash.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(location).isEqualTo(WAREHOUSE_PATH + "/db.db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoDbUri() {
    Mockito.doReturn(GetItemResponse.builder().item(Maps.newHashMap()).build())
        .when(dynamo)
        .getItem(any(GetItemRequest.class));

    String warehousePath = WAREHOUSE_PATH + "/db.db/table";
    String defaultWarehouseLocation = dynamoCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(defaultWarehouseLocation).isEqualTo(warehousePath);
  }

  @Test
  public void testDefaultWarehouseLocationDbUri() {
    String dbUri = "s3://bucket2/db";
    Mockito.doReturn(
            GetItemResponse.builder()
                .item(
                    ImmutableMap.of(
                        toPropertyCol(DynamoDbCatalog.defaultLocationProperty()),
                        AttributeValue.builder().s(dbUri).build()))
                .build())
        .when(dynamo)
        .getItem(any(GetItemRequest.class));

    String defaultWarehouseLocation = dynamoCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
    assertThat(defaultWarehouseLocation).isEqualTo("s3://bucket2/db/table");
  }

  @Test
  public void testDefaultWarehouseLocationNoNamespace() {
    Mockito.doReturn(GetItemResponse.builder().build())
        .when(dynamo)
        .getItem(any(GetItemRequest.class));

    assertThatThrownBy(() -> dynamoCatalog.defaultWarehouseLocation(TABLE_IDENTIFIER))
        .as("default warehouse can't be called on non existent namespace")
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Cannot find default warehouse location:");
  }

  @Test
  public void testDefaultWarehouseLocationUniqueWithoutDbUri() throws Exception {
    try (DynamoDbCatalog catalog = new DynamoDbCatalog()) {
      catalog.initialize(
          CATALOG_NAME,
          WAREHOUSE_PATH,
          new AwsProperties(),
          dynamo,
          null,
          true /* uniqTableLocation */);
      Mockito.doReturn(GetItemResponse.builder().item(Maps.newHashMap()).build())
          .when(dynamo)
          .getItem(any(GetItemRequest.class));

      String defaultWarehouseLocation = catalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
      assertThat(defaultWarehouseLocation).matches(WAREHOUSE_PATH + "/db.db/table-[a-z0-9]{32}");
    }
  }

  @Test
  public void testDefaultWarehouseLocationUniqueWithDbUri() throws Exception {
    try (DynamoDbCatalog catalog = new DynamoDbCatalog()) {
      catalog.initialize(
          CATALOG_NAME,
          WAREHOUSE_PATH,
          new AwsProperties(),
          dynamo,
          null,
          true /* uniqTableLocation */);
      String dbUri = "s3://bucket2/db";
      Mockito.doReturn(
              GetItemResponse.builder()
                  .item(
                      ImmutableMap.of(
                          toPropertyCol(DynamoDbCatalog.defaultLocationProperty()),
                          AttributeValue.builder().s(dbUri).build()))
                  .build())
          .when(dynamo)
          .getItem(any(GetItemRequest.class));

      String defaultWarehouseLocation = catalog.defaultWarehouseLocation(TABLE_IDENTIFIER);
      assertThat(defaultWarehouseLocation).matches("s3://bucket2/db/table-[a-z0-9]{32}");
    }
  }

  @Test
  void listNamespacesReturnsOnlyChildren() {
    stubNamespaces("db", "db.a", "db.b", "db.a.c", "dbx", "dbx.z", "other");

    assertThat(dynamoCatalog.listNamespaces(Namespace.of("db")))
        .containsExactlyInAnyOrder(Namespace.of("db", "a"), Namespace.of("db", "b"));
  }

  @Test
  void listNamespacesReturnsOnlyTopLevel() {
    stubNamespaces("db", "db.a", "db.b", "db.a.c", "dbx", "dbx.z", "other");

    assertThat(dynamoCatalog.listNamespaces())
        .containsExactlyInAnyOrder(Namespace.of("db"), Namespace.of("dbx"), Namespace.of("other"));
  }

  @Test
  void listNamespacesOfLeafNamespaceIsEmpty() {
    stubNamespaces("db", "db.a");

    assertThat(dynamoCatalog.listNamespaces(Namespace.of("db", "a"))).isEmpty();
  }

  @Test
  void listNamespacesOfMissingNamespace() {
    stubNamespaces("db", "db.a");

    assertThatThrownBy(() -> dynamoCatalog.listNamespaces(Namespace.of("missing")))
        .isInstanceOf(NoSuchNamespaceException.class)
        .hasMessageContaining("Namespace does not exist: missing");
  }

  @Test
  void listNamespacesWithoutParentEntry() {
    stubNamespaces("db.a", "db.b");

    assertThat(dynamoCatalog.listNamespaces(Namespace.of("db")))
        .containsExactlyInAnyOrder(Namespace.of("db", "a"), Namespace.of("db", "b"));
  }

  // answers like DynamoDB: begins_with on the namespace column, and a key lookup per namespace
  private void stubNamespaces(String... namespaces) {
    List<String> stored = List.of(namespaces);
    Mockito.doAnswer(
            invocation -> {
              QueryRequest request = invocation.getArgument(0);
              AttributeValue prefix = request.expressionAttributeValues().get(":ns");
              List<Map<String, AttributeValue>> items =
                  stored.stream()
                      .filter(ns -> prefix == null || ns.startsWith(prefix.s()))
                      .map(ns -> Map.of("namespace", AttributeValue.builder().s(ns).build()))
                      .collect(Collectors.toList());
              return QueryResponse.builder().items(items).build();
            })
        .when(dynamo)
        .query(any(QueryRequest.class));
    Mockito.doAnswer(
            invocation -> {
              GetItemRequest request = invocation.getArgument(0);
              String ns = request.key().get("namespace").s();
              return stored.contains(ns)
                  ? GetItemResponse.builder().item(request.key()).build()
                  : GetItemResponse.builder().build();
            })
        .when(dynamo)
        .getItem(any(GetItemRequest.class));
  }
}
