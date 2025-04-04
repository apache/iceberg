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

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class DynamoDbProperties implements Serializable {
  /** Configure an alternative endpoint of the DynamoDB service to access. */
  public static final String DYNAMODB_ENDPOINT = "dynamodb.endpoint";

  /** DynamoDB table name for {@link DynamoDbCatalog} */
  public static final String DYNAMODB_TABLE_NAME = "dynamodb.table-name";

  public static final String DYNAMODB_TABLE_NAME_DEFAULT = "iceberg";

  private String dynamoDbTableName;
  private final String dynamoDbEndpoint;

  public DynamoDbProperties() {
    this.dynamoDbEndpoint = null;
    this.dynamoDbTableName = DYNAMODB_TABLE_NAME_DEFAULT;
  }

  public DynamoDbProperties(Map<String, String> properties) {
    this.dynamoDbEndpoint = properties.get(DYNAMODB_ENDPOINT);
    this.dynamoDbTableName =
        PropertyUtil.propertyAsString(properties, DYNAMODB_TABLE_NAME, DYNAMODB_TABLE_NAME_DEFAULT);
  }

  public String dynamoDbTableName() {
    return dynamoDbTableName;
  }

  public void setDynamoDbTableName(String name) {
    this.dynamoDbTableName = name;
  }

  /**
   * Override the endpoint for a dynamoDb client.
   *
   * <p>Sample usage:
   *
   * <pre>
   *     DynamoDbClient.builder().applyMutation(dynamoDbProperties::applyDynamoDbEndpointConfigurations)
   * </pre>
   */
  public <T extends DynamoDbClientBuilder> void applyEndpointConfigurations(T builder) {
    if (dynamoDbEndpoint != null) {
      builder.endpointOverride(URI.create(dynamoDbEndpoint));
    }
  }
}
