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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;

public class TestAssumeRoleAwsClientFactory {

  private static final String TEST_ROLE_ARN = "arn:aws:iam::123456789012:role/test-role";
  private static final String ASSUME_ROLE_REGION = "us-east-1";
  private static final String CLIENT_REGION = "eu-west-1";

  private AssumeRoleAwsClientFactory createFactory(Map<String, String> properties) {
    AssumeRoleAwsClientFactory factory = new AssumeRoleAwsClientFactory();
    factory.initialize(properties);
    return factory;
  }

  @Test
  public void testInitializationRequiresRoleArn() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.CLIENT_ASSUME_ROLE_REGION, ASSUME_ROLE_REGION);
    assertThatThrownBy(() -> createFactory(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Cannot initialize AssumeRoleClientConfigFactory with null role ARN");
  }

  @Test
  public void testInitializationRequiresRegion() {
    Map<String, String> properties =
        ImmutableMap.of(AwsProperties.CLIENT_ASSUME_ROLE_ARN, TEST_ROLE_ARN);
    assertThatThrownBy(() -> createFactory(properties))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Cannot initialize AssumeRoleClientConfigFactory with null region");
  }

  @Test
  public void testPublicClientsUseAssumeRoleRegion() {
    AssumeRoleAwsClientFactory factory =
        createFactory(
            ImmutableMap.of(
                AwsProperties.CLIENT_ASSUME_ROLE_ARN, TEST_ROLE_ARN,
                AwsProperties.CLIENT_ASSUME_ROLE_REGION, ASSUME_ROLE_REGION));

    Region expected = Region.of(ASSUME_ROLE_REGION);
    try (S3Client s3 = factory.s3();
        GlueClient glue = factory.glue();
        KmsClient kms = factory.kms();
        DynamoDbClient dynamo = factory.dynamo()) {
      assertThat(s3.serviceClientConfiguration().region()).isEqualTo(expected);
      assertThat(glue.serviceClientConfiguration().region()).isEqualTo(expected);
      assertThat(kms.serviceClientConfiguration().region()).isEqualTo(expected);
      assertThat(dynamo.serviceClientConfiguration().region()).isEqualTo(expected);
    }
  }

  @Test
  public void testStsClientUsesClientRegionWhenConfigured() {
    AssumeRoleAwsClientFactory factory =
        createFactory(
            ImmutableMap.of(
                AwsProperties.CLIENT_ASSUME_ROLE_ARN, TEST_ROLE_ARN,
                AwsProperties.CLIENT_ASSUME_ROLE_REGION, ASSUME_ROLE_REGION,
                AwsClientProperties.CLIENT_REGION, CLIENT_REGION));

    StsClientBuilder mockStsBuilder = mock(StsClientBuilder.class, RETURNS_SELF);
    when(mockStsBuilder.build()).thenReturn(mock(StsClient.class));
    doAnswer(
            invocation -> {
              invocation.<Consumer<StsClientBuilder>>getArgument(0).accept(mockStsBuilder);
              return mockStsBuilder;
            })
        .when(mockStsBuilder)
        .applyMutation(any());

    try (MockedStatic<StsClient> mockedSts = mockStatic(StsClient.class)) {
      mockedSts.when(StsClient::builder).thenReturn(mockStsBuilder);

      factory.glue();

      verify(mockStsBuilder).region(Region.of(CLIENT_REGION));
    }
  }
}
