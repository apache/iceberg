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
package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

public class TestAwsProperties {

  @Test
  public void testDynamoDbDefaults() {
    Map<String, String> properties = Maps.newHashMap();
    AwsProperties awsProperties = new AwsProperties(properties);
    Assert.assertEquals(
        "DynamoDb schema version should be default version",
        AwsProperties.DYNAMODB_DEFAULT_SCHEMA_VERSION,
        awsProperties.dynamoDbSchemaVersion());
    Assert.assertEquals(
        "DynamoDb table name should be default name",
        AwsProperties.DYNAMODB_TABLE_NAME_DEFAULT,
        awsProperties.dynamoDbTableName());
  }

  @Test
  public void testDynamoDbWithSchemaOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.DYNAMODB_CATALOG_SCHEMA_VERSION, "2");
    AwsProperties awsProperties = new AwsProperties(properties);
    Assert.assertEquals(
        "DynamoDb schema version should be 2", 2, awsProperties.dynamoDbSchemaVersion());
    Assert.assertEquals(
        "DynamoDb table name should be default name for 2",
        AwsProperties.DYNAMODB_V2_SCHEMA_DEFAULT_TABLE_NAME,
        awsProperties.dynamoDbTableName());

    // with v1 schema version
    properties.put(AwsProperties.DYNAMODB_CATALOG_SCHEMA_VERSION, "1");
    awsProperties = new AwsProperties(properties);
    Assert.assertEquals(
        "DynamoDb schema version should be 1", 1, awsProperties.dynamoDbSchemaVersion());
    Assert.assertEquals(
        "DynamoDb table name should be default name for 1",
        AwsProperties.DYNAMODB_TABLE_NAME_DEFAULT,
        awsProperties.dynamoDbTableName());

    // invalid schema version
    properties.put(AwsProperties.DYNAMODB_CATALOG_SCHEMA_VERSION, "3");
    AssertHelpers.assertThrows(
        "Should fail as invalid dynamodb schema version specified",
        IllegalArgumentException.class,
        "Invalid dynamodb catalog schema version",
        () -> new AwsProperties(properties));
  }

  @Test
  public void testDynamoDbWithTableName() {
    String tableName = "dynamo-catalog-table";
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.DYNAMODB_CATALOG_SCHEMA_VERSION, "2");
    properties.put(AwsProperties.DYNAMODB_TABLE_NAME, tableName);
    AwsProperties awsProperties = new AwsProperties(properties);
    Assert.assertEquals(
        "DynamoDb schema version should be 2", 2, awsProperties.dynamoDbSchemaVersion());
    Assert.assertEquals(
        "DynamoDb table name should be specified table name",
        tableName,
        awsProperties.dynamoDbTableName());
  }
}
