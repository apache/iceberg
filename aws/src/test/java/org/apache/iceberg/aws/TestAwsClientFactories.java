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

import java.io.IOException;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializationUtil;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3Client;

public class TestAwsClientFactories {

  @Test
  public void testLoadDefault() {
    assertThat(AwsClientFactories.defaultFactory())
        .as("default client should be singleton")
        .isSameAs(AwsClientFactories.defaultFactory());

    assertThat(AwsClientFactories.from(Maps.newHashMap()))
        .as("should load default when not configured")
        .isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);
  }

  @Test
  public void testLoadCustom() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, CustomFactory.class.getName());
    assertThat(AwsClientFactories.from(properties))
        .as("should load custom class")
        .isInstanceOf(CustomFactory.class);
  }

  @Test
  public void testS3FileIoCredentialsVerification() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, "key");

    assertThatThrownBy(() -> AwsClientFactories.from(properties))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");

    properties.remove(S3FileIOProperties.ACCESS_KEY_ID);
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "secret");

    assertThatThrownBy(() -> AwsClientFactories.from(properties))
        .isInstanceOf(ValidationException.class)
        .hasMessage("S3 client access key ID and secret access key must be set at the same time");
  }

  @Test
  public void testDefaultAwsClientFactorySerializable() throws IOException {
    Map<String, String> properties = Maps.newHashMap();
    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    AwsClientFactory roundTripResult =
        TestHelpers.KryoHelpers.roundTripSerialize(defaultAwsClientFactory);
    assertThat(roundTripResult).isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);

    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(defaultAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    assertThat(deserializedClientFactory)
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
    assertThat(roundTripResult).isInstanceOf(AssumeRoleAwsClientFactory.class);

    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(assumeRoleAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    assertThat(deserializedClientFactory).isInstanceOf(AssumeRoleAwsClientFactory.class);
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
    assertThat(roundTripResult).isInstanceOf(LakeFormationAwsClientFactory.class);

    byte[] serializedFactoryBytes =
        SerializationUtil.serializeToBytes(lakeFormationAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    assertThat(deserializedClientFactory).isInstanceOf(LakeFormationAwsClientFactory.class);
  }

  @Test
  public void testWithDummyValidCredentialsProvider() {
    AwsClientFactory defaultAwsClientFactory =
        getAwsClientFactoryByCredentialsProvider(DummyValidProvider.class.getName());
    assertDefaultAwsClientFactory(defaultAwsClientFactory);
    assertClientObjectsNotNull(defaultAwsClientFactory);
    // Ensuring S3Exception thrown instead exception thrown by resolveCredentials() implemented by
    // test credentials provider
    assertThatThrownBy(() -> defaultAwsClientFactory.s3().listBuckets())
        .isInstanceOf(software.amazon.awssdk.services.s3.model.S3Exception.class)
        .hasMessageContaining("The AWS Access Key Id you provided does not exist in our records");
  }

  @Test
  public void testWithNoCreateMethodCredentialsProvider() {
    String providerClassName = NoCreateMethod.class.getName();
    String containsMessage =
        "it does not contain a static 'create' or 'create(Map<String, String>)' method";
    testProviderAndAssertThrownBy(providerClassName, containsMessage);
  }

  @Test
  public void testWithNoArgCreateMethodCredentialsProvider() {
    String providerClassName = CreateMethod.class.getName();
    String containsMessage = "Unable to load credentials from " + providerClassName;
    testProviderAndAssertThrownBy(providerClassName, containsMessage);
  }

  @Test
  public void testWithMapArgCreateMethodCredentialsProvider() {
    String providerClassName = CreateMapMethod.class.getName();
    String containsMessage = "Unable to load credentials from " + providerClassName;
    testProviderAndAssertThrownBy(providerClassName, containsMessage);
  }

  @Test
  public void testWithClassDoesNotExistsCredentialsProvider() {
    String providerClassName = "invalidClassName";
    String containsMessage = "it does not exist in the classpath";
    testProviderAndAssertThrownBy(providerClassName, containsMessage);
  }

  @Test
  public void testWithClassDoesNotImplementCredentialsProvider() {
    String providerClassName = NoInterface.class.getName();
    String containsMessage =
        "it does not contain a static 'create' or 'create(Map<String, String>)' method";
    testProviderAndAssertThrownBy(providerClassName, containsMessage);
  }

  @Test
  public void testWithClassDoesNotImplementCredentialsProviderButContainsStaticCreate() {
    AwsClientFactory defaultAwsClientFactory =
            getAwsClientFactoryByCredentialsProvider(NoInterfaceButContainsStaticCreate.class.getName());
    assertThat(defaultAwsClientFactory).isNotNull();
  }

  private void testProviderAndAssertThrownBy(String providerClassName, String containsMessage) {
    AwsClientFactory defaultAwsClientFactory =
        getAwsClientFactoryByCredentialsProvider(providerClassName);
    assertDefaultAwsClientFactory(defaultAwsClientFactory);
    assertAllClientObjectsThrownBy(defaultAwsClientFactory, containsMessage);
  }

  public void assertAllClientObjectsThrownBy(
      AwsClientFactory defaultAwsClientFactory, String containsMessage) {
    // invoking sdk client apis to ensure resolveCredentials() being called
    assertIllegalArgumentException(
        () -> defaultAwsClientFactory.s3().listBuckets(), containsMessage);
    assertIllegalArgumentException(
        () -> defaultAwsClientFactory.glue().getTables(GetTablesRequest.builder().build()),
        containsMessage);
    assertIllegalArgumentException(
        () -> defaultAwsClientFactory.dynamo().listTables(), containsMessage);
    assertIllegalArgumentException(
        () -> defaultAwsClientFactory.kms().listAliases(), containsMessage);
  }

  private void assertClientObjectsNotNull(AwsClientFactory defaultAwsClientFactory) {
    assertThat(defaultAwsClientFactory.s3()).isNotNull();
    assertThat(defaultAwsClientFactory.dynamo()).isNotNull();
    assertThat(defaultAwsClientFactory.glue()).isNotNull();
    assertThat(defaultAwsClientFactory.kms()).isNotNull();
  }

  private void assertIllegalArgumentException(
      ThrowableAssert.ThrowingCallable shouldRaiseThrowable, String containsMessage) {
    assertThatThrownBy(shouldRaiseThrowable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(containsMessage);
  }

  private void assertDefaultAwsClientFactory(AwsClientFactory awsClientFactory) {
    assertThat(awsClientFactory).isInstanceOf(AwsClientFactories.DefaultAwsClientFactory.class);
  }

  private AwsClientFactory getAwsClientFactoryByCredentialsProvider(String providerClass) {
    Map<String, String> properties = getDefaultClientFactoryProperties(providerClass);
    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    return defaultAwsClientFactory;
  }

  private Map<String, String> getDefaultClientFactoryProperties(String providerClass) {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER + ".param1", "value1");
    properties.put(AwsClientProperties.CLIENT_REGION, Region.AWS_GLOBAL.toString());
    properties.put(AwsClientProperties.CLIENT_CREDENTIALS_PROVIDER, providerClass);
    return properties;
  }

  private static class NoInterface {}

  private static class NoInterfaceButContainsStaticCreate {
    public static AwsCredentialsProvider create() {
      return StaticCredentialsProvider.create(
              AwsBasicCredentials.create("test-accessKeyId", "test-secretAccessKey"));
    }
  }

  private static class DummyValidProvider implements AwsCredentialsProvider {

    public static DummyValidProvider create() {
      return new DummyValidProvider();
    }

    @Override
    public AwsCredentials resolveCredentials() {
      return AwsBasicCredentials.create("test-accessKeyId", "test-secretAccessKey");
    }
  }

  private abstract static class ProviderTestBase implements AwsCredentialsProvider {

    @Override
    public AwsCredentials resolveCredentials() {
      throw new IllegalArgumentException(
          "Unable to load credentials from " + this.getClass().getName());
    }
  }

  private static class NoCreateMethod extends ProviderTestBase {}

  private static class CreateMethod extends ProviderTestBase {
    public static CreateMethod create() {
      return new CreateMethod();
    }
  }

  private static class CreateMapMethod extends ProviderTestBase {

    private final Map<String, String> properties;

    CreateMapMethod(Map<String, String> properties) {
      this.properties = Preconditions.checkNotNull(properties, "properties cannot be null");
      Preconditions.checkArgument(properties.get("param1") != null, "param1 value cannot be null");
    }

    public static CreateMapMethod create(Map<String, String> properties) {
      return new CreateMapMethod(properties);
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
