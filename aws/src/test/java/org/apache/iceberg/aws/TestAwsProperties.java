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

import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED;
import static org.apache.iceberg.aws.AwsProperties.DYNAMODB_TABLE_NAME;
import static org.apache.iceberg.aws.AwsProperties.GLUE_CATALOG_ID;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class TestAwsProperties {

  @Test
  public void testKryoSerialization() throws IOException {
    AwsProperties awsPropertiesWithProps =
        new AwsProperties(ImmutableMap.of(GLUE_CATALOG_ID, "foo", DYNAMODB_TABLE_NAME, "ice"));
    AwsProperties deSerializedAwsPropertiesWithProps =
        TestHelpers.KryoHelpers.roundTripSerialize(awsPropertiesWithProps);
    assertThat(deSerializedAwsPropertiesWithProps.glueCatalogId())
        .isEqualTo(awsPropertiesWithProps.glueCatalogId());
    assertThat(deSerializedAwsPropertiesWithProps.dynamoDbTableName())
        .isEqualTo(awsPropertiesWithProps.dynamoDbTableName());
  }

  @Test
  public void testAssumeRoleShouldUseRegionalEndpoint() {
    AwsProperties awsProperties =
        new AwsProperties(
            ImmutableMap.of(CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED, "true"));
    Assertions.assertThat(awsProperties.clientAssumeRoleStsRegionalEndpointEnabled()).isTrue();

    awsProperties =
        new AwsProperties(
            ImmutableMap.of(CLIENT_ASSUME_ROLE_STS_REGIONAL_ENDPOINT_ENABLED, "false"));
    Assertions.assertThat(awsProperties.clientAssumeRoleStsRegionalEndpointEnabled()).isFalse();

    awsProperties = new AwsProperties(ImmutableMap.of());
    Assertions.assertThat(awsProperties.clientAssumeRoleStsRegionalEndpointEnabled()).isFalse();
  }
}
