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
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Test;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

public class TestDefaultAwsClientFactory {

  @Test
  public void testGlueEndpointOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.GLUE_CATALOG_ENDPOINT, "https://unknown:1234");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    GlueClient glueClient = factory.glue();
    AssertHelpers.assertThrowsCause(
        "Should refuse connection to unknown endpoint",
        SdkClientException.class,
        "Unable to execute HTTP request: unknown",
        () -> glueClient.getDatabase(GetDatabaseRequest.builder().name("TEST").build()));
  }

  @Test
  public void testS3FileIoEndpointOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ENDPOINT, "https://unknown:1234");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    S3Client s3Client = factory.s3();
    AssertHelpers.assertThrowsCause(
        "Should refuse connection to unknown endpoint",
        SdkClientException.class,
        "Unable to execute HTTP request: bucket.unknown",
        () -> s3Client.getObject(GetObjectRequest.builder().bucket("bucket").key("key").build()));
  }

  @Test
  public void testS3FileIoCredentialsOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, "unknown");
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "unknown");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    S3Client s3Client = factory.s3();
    AssertHelpers.assertThrows(
        "Should fail request because of bad access key",
        S3Exception.class,
        "The AWS Access Key Id you provided does not exist in our records",
        () ->
            s3Client.getObject(
                GetObjectRequest.builder()
                    .bucket(AwsIntegTestUtil.testBucketName())
                    .key("key")
                    .build()));
  }

  @Test
  public void testDynamoDbEndpointOverride() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.DYNAMODB_ENDPOINT, "https://unknown:1234");
    AwsClientFactory factory = AwsClientFactories.from(properties);
    DynamoDbClient dynamoDbClient = factory.dynamo();
    AssertHelpers.assertThrowsCause(
        "Should refuse connection to unknown endpoint",
        SdkClientException.class,
        "Unable to execute HTTP request: unknown",
        dynamoDbClient::listTables);
  }
}
