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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.net.URI;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;

public class TestDynamoDbProperties {

  @Test
  void testDefaultConstructor() {
    DynamoDbProperties props = new DynamoDbProperties();
    assertThat(props.dynamoDbTableName()).isEqualTo("iceberg");
  }

  @Test
  void testConstructorWithProperties() {
    Map<String, String> config = ImmutableMap.of("dynamodb.table-name", "custom-table");

    DynamoDbProperties props = new DynamoDbProperties(config);
    assertThat(props.dynamoDbTableName()).isEqualTo("custom-table");
  }

  @Test
  void testApplyEndpointConfigurations() {
    Map<String, String> config = ImmutableMap.of("dynamodb.endpoint", "http://localhost:1234");

    DynamoDbProperties props = new DynamoDbProperties(config);

    DynamoDbClientBuilder builder = mock(DynamoDbClientBuilder.class);
    props.applyEndpointConfigurations(builder);

    verify(builder).endpointOverride(URI.create("http://localhost:1234"));
  }

  @Test
  void testSettersWorkCorrectly() {
    DynamoDbProperties props = new DynamoDbProperties();
    props.setDynamoDbTableName("custom-table");

    assertThat(props.dynamoDbTableName()).isEqualTo("custom-table");
  }
}
