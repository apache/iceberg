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
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

public class TestAwsKmsClient {
  private KmsClient kms;

  private AwsKmsClient awsKmsClient;

  @Before
  public void before() {
    kms = Mockito.mock(KmsClient.class);
    awsKmsClient = new AwsKmsClient();
    awsKmsClient.initialize(kms);
  }

  @Test
  public void testSupportsKeyGeneration() {
    Assert.assertTrue("AWS KMS supports key generation", awsKmsClient.supportsKeyGeneration());
  }

  @Test
  public void testGenerateKey() {
    String testWrappingKeyId = "testGenerateKey";
    String expectedPlainText = "testPlain";
    String expectedWrappedKey = "testWrappedKey";
    SdkBytes encodedPlaintext =
        SdkBytes.fromByteArray(expectedPlainText.getBytes(StandardCharsets.UTF_8));
    SdkBytes encodedWrappedKey =
        SdkBytes.fromByteArray(expectedWrappedKey.getBytes(StandardCharsets.UTF_8));
    String encodedWrappedKeyString =
        Base64.getEncoder().encodeToString(expectedWrappedKey.getBytes(StandardCharsets.UTF_8));
    Mockito.doReturn(
            GenerateDataKeyResponse.builder()
                .keyId(testWrappingKeyId)
                .plaintext(encodedPlaintext)
                .ciphertextBlob(encodedWrappedKey)
                .build())
        .when(kms)
        .generateDataKey(Mockito.any(GenerateDataKeyRequest.class));
    org.apache.iceberg.encryption.KmsClient.KeyGenerationResult keyGenResult =
        awsKmsClient.generateKey(testWrappingKeyId);
    Assert.assertEquals(
        "The test plaintext key should be testPlain",
        encodedPlaintext,
        SdkBytes.fromByteBuffer(keyGenResult.key()));
    Assert.assertEquals(
        "The test wrapped key should be testWrappedKey",
        encodedWrappedKeyString,
        keyGenResult.wrappedKey());
  }

  @Test
  public void testWrapKey() {
    String testWrappingKeyId = "testWrapKey";
    String testPlainText = "testPlain";
    String testWrappedKey = "testWrappedKey";
    SdkBytes encodedPlaintext =
        SdkBytes.fromByteArray(testPlainText.getBytes(StandardCharsets.UTF_8));
    SdkBytes encodedWrappedKey =
        SdkBytes.fromByteArray(testWrappedKey.getBytes(StandardCharsets.UTF_8));
    String encodedWrappedKeyString =
        Base64.getEncoder().encodeToString(testWrappedKey.getBytes(StandardCharsets.UTF_8));
    Mockito.doReturn(
            EncryptResponse.builder()
                .keyId(testWrappingKeyId)
                .ciphertextBlob(encodedWrappedKey)
                .build())
        .when(kms)
        .encrypt(Mockito.any(EncryptRequest.class));
    String resultWrappedKey =
        awsKmsClient.wrapKey(encodedPlaintext.asByteBuffer(), testWrappingKeyId);
    Assert.assertEquals(
        "The wrapped key should be testWrappedKey", encodedWrappedKeyString, resultWrappedKey);
  }

  @Test
  public void testUnwrapKey() {
    String testWrappingKeyId = "testUnwrapKey";
    String testPlainText = "testPlain";
    String testWrappedKey = "testWrappedKey";
    SdkBytes encodedPlaintext =
        SdkBytes.fromByteArray(testPlainText.getBytes(StandardCharsets.UTF_8));
    String encodedWrappedKeyString =
        Base64.getEncoder().encodeToString(testWrappedKey.getBytes(StandardCharsets.UTF_8));
    Mockito.doReturn(
            DecryptResponse.builder().keyId(testWrappingKeyId).plaintext(encodedPlaintext).build())
        .when(kms)
        .decrypt(Mockito.any(DecryptRequest.class));

    ByteBuffer resultPlaintextKey =
        awsKmsClient.unwrapKey(encodedWrappedKeyString, testWrappingKeyId);
    Assert.assertEquals(
        "The result plaintext should be testPlain",
        encodedPlaintext.asByteBuffer(),
        resultPlaintextKey);
  }
}
