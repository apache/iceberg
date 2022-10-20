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
import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.aws.lakeformation.LakeFormationAwsClientFactory;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.SerializationUtil;
import org.junit.Assert;
import org.junit.Test;
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
  public void testDefaultAwsClientFactorySerializable() {
    Map<String, String> properties = Maps.newHashMap();
    AwsClientFactory defaultAwsClientFactory = AwsClientFactories.from(properties);
    try {
      AwsClientFactory roundTripResult =
          TestHelpers.KryoHelpers.roundTripSerialize(defaultAwsClientFactory);
      Assert.assertTrue(
          "DefaultAwsClientFactory should be serializable",
          roundTripResult instanceof AwsClientFactories.DefaultAwsClientFactory);
    } catch (IOException e) {
      Assert.fail("kryoSerializer should serialize and deserialize DefaultAwsClientFactory");
    }
    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(defaultAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assert.assertTrue(
        "DefaultAwsClientFactory should be serializable",
        deserializedClientFactory instanceof AwsClientFactories.DefaultAwsClientFactory);
  }

  @Test
  public void testAssumeRoleAwsClientFactorySerializable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, AssumeRoleAwsClientFactory.class.getName());
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn::test");
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    AwsClientFactory assumeRoleAwsClientFactory = AwsClientFactories.from(properties);
    try {
      AwsClientFactory roundTripResult =
          TestHelpers.KryoHelpers.roundTripSerialize(assumeRoleAwsClientFactory);
      Assert.assertTrue(
          "AssumeRoleAwsClientFactory should be serializable",
          roundTripResult instanceof AssumeRoleAwsClientFactory);
    } catch (IOException e) {
      Assert.fail("kryoSerializer should serialize and deserialize AssumeRoleAwsClientFactory");
    }
    byte[] serializedFactoryBytes = SerializationUtil.serializeToBytes(assumeRoleAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assert.assertTrue(
        "AssumeRoleAwsClientFactory should be serializable",
        deserializedClientFactory instanceof AssumeRoleAwsClientFactory);
  }

  @Test
  public void testLakeFormationAwsClientFactorySerializable() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(AwsProperties.CLIENT_FACTORY, LakeFormationAwsClientFactory.class.getName());
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_ARN, "arn::test");
    properties.put(AwsProperties.CLIENT_ASSUME_ROLE_REGION, "us-east-1");
    properties.put(
        AwsProperties.CLIENT_ASSUME_ROLE_TAGS_PREFIX
            + LakeFormationAwsClientFactory.LF_AUTHORIZED_CALLER,
        "emr");
    AwsClientFactory lakeFormationAwsClientFactory = AwsClientFactories.from(properties);
    try {
      AwsClientFactory roundTripResult =
          TestHelpers.KryoHelpers.roundTripSerialize(lakeFormationAwsClientFactory);
      Assert.assertTrue(
          "LakeFormationAwsClientFactory should be serializable",
          roundTripResult instanceof LakeFormationAwsClientFactory);
    } catch (IOException e) {
      Assert.fail("kryoSerializer should serialize and deserialize LakeFormationAwsClientFactory");
    }
    byte[] serializedFactoryBytes =
        SerializationUtil.serializeToBytes(lakeFormationAwsClientFactory);
    AwsClientFactory deserializedClientFactory =
        SerializationUtil.deserializeFromBytes(serializedFactoryBytes);
    Assert.assertTrue(
        "LakeFormationAwsClientFactory should be serializable",
        deserializedClientFactory instanceof LakeFormationAwsClientFactory);
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
