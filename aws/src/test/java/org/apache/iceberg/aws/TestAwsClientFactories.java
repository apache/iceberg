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
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class TestAwsClientFactories {

  @Test
  public void testLoadDefault() {
    Assert.assertEquals(
        "default client should be singleton",
        AwsClientFactories.defaultFactory(),
        AwsClientFactories.defaultFactory());

    Assert.assertTrue(
        "should load default when not configured",
        AwsClientFactories.from(Maps.newHashMap())
            instanceof AwsClientFactories.DefaultAwsClientFactory);
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, CustomFactory.class.getName());
    Assert.assertTrue(
        "should load custom class", AwsClientFactories.from(properties) instanceof CustomFactory);
  }

  @Test
  public void testS3FileIoCredentialsProviders() {
    AwsCredentialsProvider basicCredentials =
        AwsClientFactories.credentialsProvider("key", "secret", null);
    Assert.assertTrue(
        "Should use basic credentials if access key ID and secret access key are set",
        basicCredentials.resolveCredentials() instanceof AwsBasicCredentials);
    AwsCredentialsProvider sessionCredentials =
        AwsClientFactories.credentialsProvider("key", "secret", "token");
    Assert.assertTrue(
        "Should use session credentials if session token is set",
        sessionCredentials.resolveCredentials() instanceof AwsSessionCredentials);
    Assert.assertTrue(
        "Should use default credentials if nothing is set",
        AwsClientFactories.credentialsProvider(null, null, null)
            instanceof DefaultCredentialsProvider);
  }

  @Test
  public void testS3FileIoCredentialsVerification() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.S3FILEIO_ACCESS_KEY_ID, "key");
    AssertHelpers.assertThrows(
        "Should fail if only access key ID is set",
        ValidationException.class,
        "S3 client access key ID and secret access key must be set at the same time",
        () -> AwsClientFactories.from(properties));

    properties.remove(AwsProperties.S3FILEIO_ACCESS_KEY_ID);
    properties.put(AwsProperties.S3FILEIO_SECRET_ACCESS_KEY, "secret");
    AssertHelpers.assertThrows(
        "Should fail if only secret access key is set",
        ValidationException.class,
        "S3 client access key ID and secret access key must be set at the same time",
        () -> AwsClientFactories.from(properties));
  }

  public static class CustomFactory implements AwsClientFactory {

    public CustomFactory() {}

    @Override
    public S3Client s3() {
      return null;
    }

    @Override
    public GlueClient glue() {
      return null;
    }

    @Override
    public KmsClient kms() {
      return null;
    }

    @Override
    public DynamoDbClient dynamo() {
      return null;
    }

    @Override
    public void initialize(Map<String, String> properties) {}
  }
}
