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

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializationUtil;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.SystemPropertyCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetUrlRequest;

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

  @Test
  public void testDefaultAwsClientFactorySerializable() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    AwsClientFactory roundTripResult =
        TestHelpers.KryoHelpers.roundTripSerialize(defaultAwsClientFactory);
    Assertions.assertThat(roundTripResult)
        .isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);

    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(defaultAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assertions.assertThat(deserializedClientFactory)
        .isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);
  }

  @Test
  public void testAssumeRoleAwsClientFactorySerializable() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn::test");
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    AwsClientFactory assumeRoleAwsClientFactory = AwsClientFactories.from(properties);
    AwsClientFactory roundTripResult =
        TestHelpers.KryoHelpers.roundTripSerialize(assumeRoleAwsClientFactory);
    Assertions.assertThat(roundTripResult).isInstanceOf(AssumeRoleAwsClientFactory.class);

    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(assumeRoleAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assertions.assertThat(deserializedClientFactory).isInstanceOf(AssumeRoleAwsClientFactory.class);
  }

  @Test
  public void testLakeFormationAwsClientFactorySerializable() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, LakeFormationAwsClientFactory.class.getName());
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn::test");
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    properties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER,
        "emr");
    AwsClientFactory lakeFormationAwsClientFactory = AwsClientFactories.from(properties);
    AwsClientFactory roundTripResult =
        TestHelpers.KryoHelpers.roundTripSerialize(lakeFormationAwsClientFactory);
    Assertions.assertThat(roundTripResult).isInstanceOf(LakeFormationAwsClientFactory.class);

    byte[] serializedFactoryBytes =
        SerializationUtil.serializeToBytes(lakeFormationAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assertions.assertThat(deserializedClientFactory)
        .isInstanceOf(LakeFormationAwsClientFactory.class);
  }

  @Test
  public void testDefaultAwsClientFactoryWithCredentialsProvider() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(
        AwsProperties.CLIENT_CREDENTIALS_PROVIDER,
        SystemPropertyCredentialsProvider.class.getName());

    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    assertAwsClientFactory(defaultAwsClientFactory);
  }

  private void assertAwsClientFactory(AwsClientFactory defaultAwsClientFactory) {
    Assertions.assertThat(defaultAwsClientFactory)
        .isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);
    try (S3Client s3Client = defaultAwsClientFactory.s3()) {
      Assertions.assertThat(s3Client).isNotNull();
      URL url =
          s3Client
              .utilities()
              .getUrl(GetUrlRequest.builder().bucket("test-bucket").key("test-key").build());
      Assertions.assertThat(url)
          .isNotNull()
          .hasToString("https://test-bucket.s3.amazonaws.com/test-key");
    }
  }

  @Test
  public void
      testDefaultAwsClientFactoryWithCredentialsProviderAndCreateMethodWithCustomProperties() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(
        AwsProperties.CLIENT_CREDENTIALS_PROVIDER, CustomCredentialsProvider.class.getName());
    properties.put(AwsProperties.CLIENT_CREDENTIALS_PROVIDER + ".param1", "value1");

    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    assertAwsClientFactory(defaultAwsClientFactory);
  }

  @Test
  public void testDefaultAwsClientFactoryWithInvalidCredentialsProvider() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(AwsProperties.CLIENT_CREDENTIALS_PROVIDER, "invalidClassName");
    Assertions.assertThatThrownBy(() -> AwsClientFactories.from(properties).s3().close())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Please make sure the necessary JARs/packages are added to the classpath.");
  }

  @Test
  public void testDefaultAwsClientFactoryWithInvalidCredentialsProviderWithValidClass() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(
        AwsProperties.CLIENT_CREDENTIALS_PROVIDER,
        InvalidNoInterfaceDynamicallyLoadedCredentialsProvider.class.getName());
    Assertions.assertThatThrownBy(() -> AwsClientFactories.from(properties).s3().close())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining(
            "class org.apache.iceberg.aws.TestAwsClientFactories$InvalidNoInterfaceDynamicallyLoadedCredentialsProvider is not an instance of software.amazon.awssdk.auth.credentials.AwsCredentialsProvider, loaded by the classloader for org.apache.iceberg.aws.AwsProperties");
  }

  @Test
  public void testDefaultAwsClientFactoryCredentialsProviderWithNoCreateMethod() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(
        AwsProperties.CLIENT_CREDENTIALS_PROVIDER,
        CustomCredentialsProviderWithNoCreateMethod.class.getName());
    Assertions.assertThatThrownBy(() -> AwsClientFactories.from(properties).s3().close())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Failed to create an instance of org.apache.iceberg.aws.TestAwsClientFactories$CustomCredentialsProviderWithNoCreateMethod. Please ensure that the provider class contains a static 'create' or 'create(Map<String, String>)' method that can be used to instantiate a client credentials provider");
  }

  // publicly visible for testing to be dynamically loaded
  public static class InvalidNoInterfaceDynamicallyLoadedCredentialsProvider {
    // Default no-arg constructor is present, but does not implement interface
    // AwsCredentialsProvider
  }

  // publicly visible for testing to be dynamically loaded
  public static class CustomCredentialsProviderWithNoCreateMethod
      implements AwsCredentialsProvider {

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }
  }

  // publicly visible for testing to be dynamically loaded
  public static class CustomCredentialsProvider implements AwsCredentialsProvider {

    private final Map<String, String> properties;

    CustomCredentialsProvider(Map<String, String> properties) {
      this.properties = Preconditions.checkNotNull(properties, "properties cannot be null");
    }

    public static CustomCredentialsProvider create(Map<String, String> properties) {
      return new CustomCredentialsProvider(properties);
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return null;
    }
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
