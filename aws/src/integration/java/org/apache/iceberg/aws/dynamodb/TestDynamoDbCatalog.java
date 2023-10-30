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

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.aws.AwsClientFactory;
import org.apache.iceberg.aws.AwsIntegTestUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

public class TestDynamoDbCatalog {

  private static final ForkJoinPool POOL = new ForkJoinPool(16);
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));

  private static String catalogTableName;
  private static DynamoDbClient dynamo;
  private static S3Client s3;
  private static DynamoDbCatalog catalog;
  private static String testBucket;

  @BeforeClass
  public static void beforeClass() {
    catalogTableName = genRandomName();
    AwsClientFactory clientFactory = AwsClientFactories.defaultFactory();
    dynamo = clientFactory.dynamo();
    s3 = clientFactory.s3();
    catalog = new DynamoDbCatalog();
    testBucket = AwsIntegTestUtil.testBucketName();
    catalog.initialize(
        "test",
        ImmutableMap.of(
            AwsProperties.DYNAMODB_TABLE_NAME,
            catalogTableName,
            CatalogProperties.WAREHOUSE_LOCATION,
            "s3://" + testBucket + "/" + genRandomName()));
  }

  @AfterClass
  public static void afterClass() {
    dynamo.deleteTable(DeleteTableRequest.builder().tableName(catalogTableName).build());
  }

  @Test
  public void testCreateNamespace() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    GetItemResponse response =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(catalogTableName)
                .key(DynamoDbCatalog.namespacePrimaryKey(namespace))
                .build());
    Assert.assertTrue("namespace must exist", response.hasItem());
    Assert.assertEquals(
        "namespace must be stored in DynamoDB",
        namespace.toString(),
        response.item().get("namespace").s());
    Assertions.assertThatThrownBy(() -> catalog.createNamespace(namespace))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCreateNamespaceBadName() {
    Assertions.assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "", "b")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not be empty");
    Assertions.assertThatThrownBy(() -> catalog.createNamespace(Namespace.of("a", "b.c")))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not contain dot");
  }

  @Test
  public void testListSubNamespaces() {
    Namespace parent = Namespace.of(genRandomName());
    List<Namespace> namespaceList =
        IntStream.range(0, 3)
            .mapToObj(i -> Namespace.of(parent.toString(), genRandomName()))
            .collect(Collectors.toList());
    catalog.createNamespace(parent);
    namespaceList.forEach(ns -> catalog.createNamespace(ns));
    Assert.assertEquals(4, catalog.listNamespaces(parent).size());
  }

  @Test
  public void testNamespaceProperties() {
    Namespace namespace = Namespace.of(genRandomName());
    Map<String, String> properties = Maps.newHashMap();
    properties.put("key1", "val1");
    properties.put("key2", "val2");
    catalog.createNamespace(namespace, properties);
    Assert.assertEquals(properties, catalog.loadNamespaceMetadata(namespace));

    properties.put("key3", "val3");
    properties.put("key2", "val2-1");
    catalog.setProperties(namespace, properties);
    Assert.assertEquals(properties, catalog.loadNamespaceMetadata(namespace));

    properties.remove("key3");
    catalog.removeProperties(namespace, Sets.newHashSet("key3"));
    Assert.assertEquals(properties, catalog.loadNamespaceMetadata(namespace));
  }

  @Test
  public void testCreateTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);
    GetItemResponse response =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(catalogTableName)
                .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier))
                .build());
    Assert.assertTrue("table must exist", response.hasItem());
    Assert.assertEquals(
        "table must be stored in DynamoDB with table identifier as partition key",
        tableIdentifier.toString(),
        response.item().get("identifier").s());
    Assert.assertEquals(
        "table must be stored in DynamoDB with namespace as sort key",
        namespace.toString(),
        response.item().get("namespace").s());
    Assertions.assertThatThrownBy(() -> catalog.createTable(tableIdentifier, SCHEMA))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
  }

  @Test
  public void testCreateTableBadName() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    Assertions.assertThatThrownBy(
            () -> catalog.createTable(TableIdentifier.of(Namespace.empty(), "a"), SCHEMA))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Table namespace must not be empty");
    Assertions.assertThatThrownBy(
            () -> catalog.createTable(TableIdentifier.of(namespace, "a.b"), SCHEMA))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("must not contain dot");
  }

  @Test
  public void testListTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    List<TableIdentifier> tableIdentifiers =
        IntStream.range(0, 3)
            .mapToObj(i -> TableIdentifier.of(namespace, genRandomName()))
            .collect(Collectors.toList());
    tableIdentifiers.forEach(id -> catalog.createTable(id, SCHEMA));
    Assert.assertEquals(3, catalog.listTables(namespace).size());
  }

  @Test
  public void testDropTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);
    String metadataLocation =
        dynamo
            .getItem(
                GetItemRequest.builder()
                    .tableName(catalogTableName)
                    .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier))
                    .build())
            .item()
            .get("p.metadata_location")
            .s();
    catalog.dropTable(tableIdentifier, true);
    Assert.assertFalse(
        "table entry should not exist in dynamo",
        dynamo
            .getItem(
                GetItemRequest.builder()
                    .tableName(catalogTableName)
                    .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier))
                    .build())
            .hasItem());
    Assertions.assertThatThrownBy(
            () ->
                s3.headObject(
                    HeadObjectRequest.builder()
                        .bucket(testBucket)
                        .key(
                            metadataLocation.substring(
                                testBucket.length() + 6)) // s3:// + end slash
                        .build()))
        .as("metadata location should be deleted")
        .isInstanceOf(NoSuchKeyException.class)
        .hasMessageContaining("not found");
  }

  @Test
  public void testRenameTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    Namespace namespace2 = Namespace.of(genRandomName());
    catalog.createNamespace(namespace2);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);
    TableIdentifier tableIdentifier2 = TableIdentifier.of(namespace2, genRandomName());
    Assertions.assertThatThrownBy(
            () -> catalog.renameTable(TableIdentifier.of(namespace, "a"), tableIdentifier2))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("does not exist");

    Assertions.assertThatThrownBy(() -> catalog.renameTable(tableIdentifier, tableIdentifier))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");

    String metadataLocation =
        dynamo
            .getItem(
                GetItemRequest.builder()
                    .tableName(catalogTableName)
                    .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier))
                    .build())
            .item()
            .get("p.metadata_location")
            .s();

    catalog.renameTable(tableIdentifier, tableIdentifier2);

    String metadataLocation2 =
        dynamo
            .getItem(
                GetItemRequest.builder()
                    .tableName(catalogTableName)
                    .key(DynamoDbCatalog.tablePrimaryKey(tableIdentifier2))
                    .build())
            .item()
            .get("p.metadata_location")
            .s();

    Assert.assertEquals(
        "metadata location should be copied to new table entry",
        metadataLocation,
        metadataLocation2);
  }

  @Test
  public void testUpdateTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);
    Table table = catalog.loadTable(tableIdentifier);
    table.updateSchema().addColumn("data", Types.StringType.get()).commit();
    table.refresh();
    Assert.assertEquals(2, table.schema().columns().size());
  }

  @Test
  public void testConcurrentCommits() throws Exception {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, genRandomName());
    catalog.createTable(tableIdentifier, SCHEMA);
    Table table = catalog.loadTable(tableIdentifier);
    POOL.submit(
            () ->
                IntStream.range(0, 16)
                    .parallel()
                    .forEach(
                        i -> {
                          try {
                            table
                                .updateSchema()
                                .addColumn(genRandomName(), Types.StringType.get())
                                .commit();
                          } catch (Exception e) {
                            // ignore
                          }
                        }))
        .get();

    Assert.assertEquals(2, table.schema().columns().size());
  }

  @Test
  public void testDropNamespace() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    catalog.dropNamespace(namespace);
    GetItemResponse response =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(catalogTableName)
                .key(DynamoDbCatalog.namespacePrimaryKey(namespace))
                .build());
    Assert.assertFalse("namespace must not exist", response.hasItem());
  }

  @Test
  public void testRegisterTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, catalogTableName);
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    Assertions.assertThat(catalog.dropTable(identifier, false)).isTrue();
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((DynamoDbTableOperations) ops).currentMetadataLocation();
    Table registeredTable = catalog.registerTable(identifier, metadataLocation);
    Assertions.assertThat(registeredTable).isNotNull();
    String expectedMetadataLocation =
        ((HasTableOperations) registeredTable).operations().current().metadataFileLocation();
    Assertions.assertThat(metadataLocation).isEqualTo(expectedMetadataLocation);
    Assertions.assertThat(catalog.loadTable(identifier)).isNotNull();
    Assertions.assertThat(catalog.dropTable(identifier, true)).isTrue();
    Assertions.assertThat(catalog.dropNamespace(namespace)).isTrue();
  }

  @Test
  public void testDefaultWarehousePathWithLocation() {
    String namespaceName = genRandomName();
    String defaultLocation = "s3://" + testBucket + "/namespace/" + namespaceName;

    Namespace namespace = Namespace.of(namespaceName);
    Map<String, String> properties = Maps.newHashMap();
    properties.put(DynamoDbCatalog.defaultLocationProperty(), defaultLocation);
    catalog.createNamespace(namespace, properties);
    String tableName = genRandomName();
    Assertions.assertThat(
            catalog.defaultWarehouseLocation(TableIdentifier.of(namespaceName, tableName)))
        .isEqualTo(defaultLocation + "/" + tableName);
  }

  @Test
  public void testRegisterExistingTable() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    TableIdentifier identifier = TableIdentifier.of(namespace, catalogTableName);
    catalog.createTable(identifier, SCHEMA);
    Table registeringTable = catalog.loadTable(identifier);
    TableOperations ops = ((HasTableOperations) registeringTable).operations();
    String metadataLocation = ((DynamoDbTableOperations) ops).currentMetadataLocation();
    Assertions.assertThatThrownBy(() -> catalog.registerTable(identifier, metadataLocation))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessageContaining("already exists");
    Assertions.assertThat(catalog.dropTable(identifier, true)).isTrue();
    Assertions.assertThat(catalog.dropNamespace(namespace)).isTrue();
  }

  private static String genRandomName() {
    return UUID.randomUUID().toString().replace("-", "");
  }
}
