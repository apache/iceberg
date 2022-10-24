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
package org.apache.iceberg.aws.encryption;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.aws.AwsClientFactories;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DisableKeyRequest;

@SuppressWarnings("VisibilityModifier")
public class TestAwsKmsClient {
  static Map<String, String> properties;
  static AwsKmsClient awsKmsClient;
  static KmsClient kms;

  static String wrappingKeyId;

  @BeforeClass
  public static void beforeClass() {
    awsKmsClient = new AwsKmsClient();
    properties = ImmutableMap.of();
    kms = AwsClientFactories.from(properties).kms();
    awsKmsClient.initialize(kms);
    CreateKeyResponse response = kms.createKey(CreateKeyRequest.builder().build());
    wrappingKeyId = response.keyMetadata().keyId();
  }

  @AfterClass
  public static void afterClass() {
    kms.disableKey(DisableKeyRequest.builder().keyId(wrappingKeyId).build());
  }

  @Test
  public void testGenerateKey() {
    org.apache.iceberg.encryption.KmsClient.KeyGenerationResult result =
        awsKmsClient.generateKey(wrappingKeyId);
    Assert.assertNotNull("Should successfully generate a plainTextKey", result.key());
    Assert.assertNotNull("Should successfully generate a wrapped key", result.key());
  }

  @Test
  public void testWrapKey() {
    org.apache.iceberg.encryption.KmsClient.KeyGenerationResult result =
        awsKmsClient.generateKey(wrappingKeyId);
    ByteBuffer plainTextKey = result.key();
    String wrappedKey = awsKmsClient.wrapKey(plainTextKey, wrappingKeyId);
    Assert.assertNotNull("Should successfully wrap the plainTextKey", wrappedKey);
  }

  @Test
  public void testUnwrapKey() {
    org.apache.iceberg.encryption.KmsClient.KeyGenerationResult result =
        awsKmsClient.generateKey(wrappingKeyId);
    String wrappedKey = result.wrappedKey();
    ByteBuffer plainTextKey = awsKmsClient.unwrapKey(wrappedKey, wrappingKeyId);
    Assert.assertNotNull("Should successfully unwrap the wrappedKey", plainTextKey);
    Assert.assertEquals(
        "The resulting plaintext key should be consistent", result.key(), plainTextKey);
  }

  @Test
  public void testWrapAndUnwrapKey() {
    org.apache.iceberg.encryption.KmsClient.KeyGenerationResult result =
        awsKmsClient.generateKey(wrappingKeyId);
    ByteBuffer plainTextKey = result.key();
    String wrappedKey = awsKmsClient.wrapKey(plainTextKey, wrappingKeyId);
    ByteBuffer unrappedKey = awsKmsClient.unwrapKey(wrappedKey, wrappingKeyId);
    Assert.assertEquals("The plaintext key should be consistent", plainTextKey, unrappedKey);
  }
}
