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

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

public class TestDynamoDbV2SchemaCatalog extends TestDynamoDbCatalog {

  @BeforeClass
  public static void setupCatalog() {
    catalog.initialize(
        "testV2",
        ImmutableMap.of(
            AwsProperties.DYNAMODB_TABLE_NAME,
            catalogTableName,
            AwsProperties.DYNAMODB_CATALOG_SCHEMA_VERSION,
            "2",
            CatalogProperties.WAREHOUSE_LOCATION,
            "s3://" + testBucket + "/" + genRandomName()));
  }

  @Test
  public void testCreateNamespace() {
    Namespace namespace = Namespace.of(genRandomName());
    catalog.createNamespace(namespace);
    GetItemResponse response =
        dynamo.getItem(
            GetItemRequest.builder()
                .tableName(catalogTableName)
                .key(DynamoDbCatalog.namespacePrimaryKey(namespace, 2))
                .build());
    Assert.assertTrue("namespace must exist", response.hasItem());
    Assert.assertEquals(
        "identifier attribute should contain namespace in DynamoDB",
        namespace.toString(),
        response.item().get("identifier").s());

    AssertHelpers.assertThrows(
        "should not create duplicated namespace",
        AlreadyExistsException.class,
        "already exists",
        () -> catalog.createNamespace(namespace));
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
                .key(DynamoDbCatalog.namespacePrimaryKey(namespace, 2))
                .build());
    Assert.assertFalse("namespace must not exist", response.hasItem());
  }
}
