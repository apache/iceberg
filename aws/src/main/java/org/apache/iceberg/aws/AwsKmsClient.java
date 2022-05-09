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
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KmsClient;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

public class AwsKmsClient implements KmsClient {
  /*
   * property for enabling data key generation using AWS KMS service, by default it's enabled,
   * please set this property to "false" to disable key generation by AWS KMS server.
   */
  public static final String ENABLE_KEY_GENERATION = "kms.client.aws.key.generation.enabled";
  // property for specifying data key generation key spec: AES_256 or AES_128
  public static final String DATA_KEY_SPEC = "kms.client.aws.generation.data_key_spec";

  private String dataKeySpec;
  private boolean enableKeyGeneration = true;

  @Override
  public boolean supportsKeyGeneration() {
    return enableKeyGeneration;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    try (software.amazon.awssdk.services.kms.KmsClient awsKms = awsKmsClient()) {
      final GenerateDataKeyRequest generateDataKeyRequest = GenerateDataKeyRequest.builder()
              .keyId(wrappingKeyId).keySpec(DataKeySpec.fromValue(dataKeySpec)).build();
      final GenerateDataKeyResponse response = awsKms.generateDataKey(generateDataKeyRequest);
      return new KeyGenerationResult(ByteBuffer.wrap(response.plaintext().asByteArray()),
              Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray())
      );
    }
  }

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    if (enableKeyGeneration) {
      throw new UnsupportedOperationException("wrapKey shouldn't be called as key generation is enabled.");
    } else {
      try (software.amazon.awssdk.services.kms.KmsClient awsKms = awsKmsClient()) {
        final EncryptRequest encryptRequest = EncryptRequest.builder()
                .keyId(wrappingKeyId).plaintext(SdkBytes.fromByteBuffer(key)).build();
        final EncryptResponse response = awsKms.encrypt(encryptRequest);
        return Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray());
      }
    }
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    try (software.amazon.awssdk.services.kms.KmsClient awsKms = awsKmsClient()) {
      final byte[] keyBytes = Base64.getDecoder().decode(wrappedKey);
      final DecryptRequest decryptRequest = DecryptRequest.builder().keyId(wrappingKeyId)
              .ciphertextBlob(SdkBytes.fromByteArray(keyBytes)).build();
      final DecryptResponse decryptResponse = awsKms.decrypt(decryptRequest);
      return ByteBuffer.wrap(decryptResponse.plaintext().asByteArray());
    }
  }

  @Override
  public void initialize(Map<String, String> kmsClientTableProperties) {
    dataKeySpec = kmsClientTableProperties.getOrDefault(DATA_KEY_SPEC, DataKeySpec.AES_256.name());
    enableKeyGeneration = Boolean.parseBoolean(kmsClientTableProperties.getOrDefault(ENABLE_KEY_GENERATION, "true"));
  }

  /**
   * Method for creating aws kms client, users can choose to override this method for providing customized kms client
   */
  protected software.amazon.awssdk.services.kms.KmsClient awsKmsClient() {
    return AwsClientFactories.defaultFactory().kms();
  }
}
