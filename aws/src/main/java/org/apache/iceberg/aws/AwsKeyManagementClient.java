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

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

/**
 * Key management client implementation that uses AWS Key Management Service. To be used for
 * encrypting/decrypting keys with a KMS-managed master key, (by referencing its key ID), and for
 * the generation of new encryption keys.
 */
public class AwsKeyManagementClient implements KeyManagementClient {

  private KmsClient kmsClient;
  private EncryptionAlgorithmSpec encryptionAlgorithmSpec;
  private DataKeySpec dataKeySpec;

  @Override
  public void initialize(Map<String, String> properties) {
    AwsClientFactory clientFactory = AwsClientFactories.from(properties);
    this.kmsClient = clientFactory.kms();

    AwsProperties awsProperties = new AwsProperties(properties);
    this.encryptionAlgorithmSpec = awsProperties.kmsEncryptionAlgorithmSpec();
    this.dataKeySpec = awsProperties.kmsDataKeySpec();
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    EncryptRequest request =
        EncryptRequest.builder()
            .keyId(wrappingKeyId)
            .encryptionAlgorithm(encryptionAlgorithmSpec)
            .plaintext(SdkBytes.fromByteBuffer(key))
            .build();

    EncryptResponse result = kmsClient.encrypt(request);
    return result.ciphertextBlob().asByteBuffer();
  }

  @Override
  public boolean supportsKeyGeneration() {
    return true;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    GenerateDataKeyRequest request =
        GenerateDataKeyRequest.builder().keyId(wrappingKeyId).keySpec(dataKeySpec).build();

    GenerateDataKeyResponse response = kmsClient.generateDataKey(request);
    KeyGenerationResult result =
        new KeyGenerationResult(
            response.plaintext().asByteBuffer(), response.ciphertextBlob().asByteBuffer());
    return result;
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    DecryptRequest request =
        DecryptRequest.builder()
            .keyId(wrappingKeyId)
            .encryptionAlgorithm(encryptionAlgorithmSpec)
            .ciphertextBlob(SdkBytes.fromByteBuffer(wrappedKey))
            .build();

    DecryptResponse result = kmsClient.decrypt(request);
    return result.plaintext().asByteBuffer();
  }

  @Override
  public void close() {
    if (kmsClient != null) {
      kmsClient.close();
    }
  }
}
