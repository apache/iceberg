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
import java.util.UUID;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3CrtAsyncClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class AssumeRoleAwsClientFactory implements AwsClientFactory {
  private AwsProperties awsProperties;
  private HttpClientProperties httpClientProperties;
  private S3FileIOProperties s3FileIOProperties;
  private String roleSessionName;
  private AwsClientProperties awsClientProperties;

  @Override
  public S3Client s3() {
    return S3Client.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(awsClientProperties::applyLegacyMd5Plugin)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
        .applyMutation(s3FileIOProperties::applyServiceConfigurations)
        .applyMutation(s3FileIOProperties::applySignerConfiguration)
        .applyMutation(s3FileIOProperties::applyRetryConfigurations)
        .build();
  }

  @Override
  public S3AsyncClient s3Async() {
    if (s3FileIOProperties.isS3CRTEnabled()) {
      return S3AsyncClient.crtBuilder()
          .applyMutation(this::applyAssumeRoleConfigurations)
          .applyMutation(awsClientProperties::applyClientRegionConfiguration)
          .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
          .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
          .applyMutation(s3FileIOProperties::applyS3CrtConfigurations)
          .build();
    }
    return S3AsyncClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(awsClientProperties::applyClientRegionConfiguration)
        .applyMutation(awsClientProperties::applyClientCredentialConfigurations)
        .applyMutation(awsClientProperties::applyLegacyMd5Plugin)
        .applyMutation(s3FileIOProperties::applyEndpointConfigurations)
        .build();
  }

  @Override
  public GlueClient glue() {
    return GlueClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .build();
  }

  @Override
  public KmsClient kms() {
    return KmsClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .applyMutation(awsClientProperties::applyRetryConfigurations)
        .build();
  }

  @Override
  public DynamoDbClient dynamo() {
    return DynamoDbClient.builder()
        .applyMutation(this::applyAssumeRoleConfigurations)
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .applyMutation(awsProperties::applyDynamoDbEndpointConfigurations)
        .build();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.awsProperties = new AwsProperties(properties);
    this.s3FileIOProperties = new S3FileIOProperties(properties);
    this.httpClientProperties = new HttpClientProperties(properties);
    this.awsClientProperties = new AwsClientProperties(properties);
    this.roleSessionName = genSessionName();
    Preconditions.checkNotNull(
        awsProperties.clientAssumeRoleArn(),
        "Cannot initialize AssumeRoleClientConfigFactory with null role ARN");
    Preconditions.checkNotNull(
        awsProperties.clientAssumeRoleRegion(),
        "Cannot initialize AssumeRoleClientConfigFactory with null region");
  }

  protected <T extends AwsClientBuilder & AwsSyncClientBuilder> T applyAssumeRoleConfigurations(
      T clientBuilder) {
    clientBuilder
        .credentialsProvider(createCredentialsProvider())
        .region(Region.of(awsProperties.clientAssumeRoleRegion()));
    return clientBuilder;
  }

  protected S3AsyncClientBuilder applyAssumeRoleConfigurations(S3AsyncClientBuilder clientBuilder) {
    return clientBuilder
        .credentialsProvider(createCredentialsProvider())
        .region(Region.of(awsProperties.clientAssumeRoleRegion()));
  }

  protected S3CrtAsyncClientBuilder applyAssumeRoleConfigurations(
      S3CrtAsyncClientBuilder clientBuilder) {
    return clientBuilder
        .credentialsProvider(createCredentialsProvider())
        .region(Region.of(awsProperties.clientAssumeRoleRegion()));
  }

  protected String region() {
    return awsProperties.clientAssumeRoleRegion();
  }

  protected AwsProperties awsProperties() {
    return awsProperties;
  }

  protected HttpClientProperties httpClientProperties() {
    return httpClientProperties;
  }

  protected S3FileIOProperties s3FileIOProperties() {
    return s3FileIOProperties;
  }

  protected AwsClientProperties awsClientProperties() {
    return awsClientProperties;
  }

  private StsClient sts() {
    return StsClient.builder()
        .applyMutation(httpClientProperties::applyHttpClientConfigurations)
        .build();
  }

  private String genSessionName() {
    if (awsProperties.clientAssumeRoleSessionName() != null) {
      return awsProperties.clientAssumeRoleSessionName();
    }
    return String.format("iceberg-aws-%s", UUID.randomUUID());
  }

  private StsAssumeRoleCredentialsProvider createCredentialsProvider() {
    return StsAssumeRoleCredentialsProvider.builder()
        .stsClient(sts())
        .refreshRequest(createAssumeRoleRequest())
        .build();
  }

  private AssumeRoleRequest createAssumeRoleRequest() {
    return AssumeRoleRequest.builder()
        .roleArn(awsProperties.clientAssumeRoleArn())
        .roleSessionName(roleSessionName)
        .durationSeconds(awsProperties.clientAssumeRoleTimeoutSec())
        .externalId(awsProperties.clientAssumeRoleExternalId())
        .tags(awsProperties.stsClientAssumeRoleTags())
        .build();
  }
}
