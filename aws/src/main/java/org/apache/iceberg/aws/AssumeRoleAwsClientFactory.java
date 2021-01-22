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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.PropertyUtil;
import software.amazon.awssdk.awscore.client.builder.AwsClientBuilder;
import software.amazon.awssdk.awscore.client.builder.AwsSyncClientBuilder;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;

public class AssumeRoleAwsClientFactory implements AwsClientFactory {

  private static final SdkHttpClient HTTP_CLIENT_DEFAULT = UrlConnectionHttpClient.create();

  private String roleArn;
  private String externalId;
  private int timeout;
  private String region;

  @Override
  public S3Client s3() {
    return S3Client.builder().applyMutation(this::configure).build();
  }

  @Override
  public GlueClient glue() {
    return GlueClient.builder().applyMutation(this::configure).build();
  }

  @Override
  public KmsClient kms() {
    return KmsClient.builder().applyMutation(this::configure).build();
  }

  @Override
  public DynamoDbClient dynamo() {
    return DynamoDbClient.builder().applyMutation(this::configure).build();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    roleArn = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_ARN);
    Preconditions.checkNotNull(roleArn,
        "Cannot initialize AssumeRoleClientConfigFactory with null role ARN");
    timeout = PropertyUtil.propertyAsInt(properties,
        AwsProperties.CLIENT_ASSUME_ROLE_TIMEOUT_SEC, AwsProperties.CLIENT_ASSUME_ROLE_TIMEOUT_SEC_DEFAULT);
    externalId = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_EXTERNAL_ID);

    region = properties.get(AwsProperties.CLIENT_ASSUME_ROLE_REGION);
    Preconditions.checkNotNull(region, "Cannot initialize AssumeRoleClientConfigFactory with null region");
  }

  private <T extends AwsClientBuilder & AwsSyncClientBuilder> T configure(T clientBuilder) {
    AssumeRoleRequest request = AssumeRoleRequest.builder()
        .roleArn(roleArn)
        .roleSessionName(genSessionName())
        .durationSeconds(timeout)
        .externalId(externalId)
        .build();

    clientBuilder.credentialsProvider(
        StsAssumeRoleCredentialsProvider.builder()
            .stsClient(StsClient.builder().httpClient(HTTP_CLIENT_DEFAULT).build())
            .refreshRequest(request)
            .build());

    clientBuilder.region(Region.of(region));
    clientBuilder.httpClient(HTTP_CLIENT_DEFAULT);

    return clientBuilder;
  }

  private String genSessionName() {
    return String.format("iceberg-aws-%s", UUID.randomUUID());
  }
}
