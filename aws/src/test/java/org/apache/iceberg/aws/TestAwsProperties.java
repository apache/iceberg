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

import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_ARN;
import static org.apache.iceberg.aws.AwsProperties.CLIENT_ASSUME_ROLE_REGION;
import static org.apache.iceberg.aws.AwsProperties.DYNAMODB_TABLE_NAME;
import static org.apache.iceberg.aws.AwsProperties.GLUE_CATALOG_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_ACCESS_KEY_ID;
import static org.apache.iceberg.aws.AwsProperties.REST_SECRET_ACCESS_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

public class TestAwsProperties {

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerialization(TestHelpers.RoundTripSerializer<AwsProperties> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    AwsProperties awsPropertiesWithProps =
        new AwsProperties(ImmutableMap.of(GLUE_CATALOG_ID, "foo", DYNAMODB_TABLE_NAME, "ice"));
    AwsProperties deSerializedAwsPropertiesWithProps =
        roundTripSerializer.apply(awsPropertiesWithProps);
    assertThat(deSerializedAwsPropertiesWithProps.glueCatalogId())
        .isEqualTo(awsPropertiesWithProps.glueCatalogId());
    assertThat(deSerializedAwsPropertiesWithProps.dynamoDbTableName())
        .isEqualTo(awsPropertiesWithProps.dynamoDbTableName());
  }

  @Test
  public void testRestCredentialsProviderAssumesRoleWhenConfigured() {
    AwsProperties awsProperties =
        new AwsProperties(
            ImmutableMap.of(
                CLIENT_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/test",
                CLIENT_ASSUME_ROLE_REGION,
                "us-east-1"));

    assertThat(awsProperties.restCredentialsProvider())
        .as("REST requests should be signed with the assumed-role credentials")
        .isInstanceOf(StsAssumeRoleCredentialsProvider.class);
  }

  @Test
  public void testRestCredentialsProviderDefaultsWhenAssumeRoleNotConfigured() {
    AwsProperties awsProperties = new AwsProperties(ImmutableMap.of());

    assertThat(awsProperties.restCredentialsProvider())
        .as("REST requests should fall back to the default credentials chain")
        .isInstanceOf(DefaultCredentialsProvider.class);
  }

  @Test
  public void testRestCredentialsProviderPrefersStaticRestCredentials() {
    AwsProperties awsProperties =
        new AwsProperties(
            ImmutableMap.of(
                REST_ACCESS_KEY_ID,
                "accessKeyId",
                REST_SECRET_ACCESS_KEY,
                "secretAccessKey",
                CLIENT_ASSUME_ROLE_ARN,
                "arn:aws:iam::123456789012:role/test",
                CLIENT_ASSUME_ROLE_REGION,
                "us-east-1"));

    assertThat(awsProperties.restCredentialsProvider())
        .as("Explicit static REST credentials should take precedence over assume-role")
        .isInstanceOf(StaticCredentialsProvider.class);
  }

  @Test
  public void testRestCredentialsProviderFallsBackToDefaultWhenAssumeRoleRegionMissing() {
    AwsProperties awsProperties =
        new AwsProperties(
            ImmutableMap.of(CLIENT_ASSUME_ROLE_ARN, "arn:aws:iam::123456789012:role/test"));

    assertThat(awsProperties.restCredentialsProvider())
        .as("Missing assume-role region should fall back to the default credentials chain")
        .isInstanceOf(DefaultCredentialsProvider.class);
  }
}
