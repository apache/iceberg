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
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.encryption.KmsClient;
import org.apache.iceberg.util.SerializableSupplier;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

public class AwsKmsClient implements KmsClient {
  private final AtomicBoolean isResourceClosed = new AtomicBoolean(false);

  private SerializableSupplier<software.amazon.awssdk.services.kms.KmsClient> kms;
  private transient software.amazon.awssdk.services.kms.KmsClient client;
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
    GenerateDataKeyRequest generateDataKeyRequest =
        GenerateDataKeyRequest.builder()
            .keyId(wrappingKeyId)
            .keySpec(DataKeySpec.fromValue(dataKeySpec))
            .build();
    GenerateDataKeyResponse response = client().generateDataKey(generateDataKeyRequest);
    return new KeyGenerationResult(
        ByteBuffer.wrap(response.plaintext().asByteArray()),
        Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray()));
  }

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    if (enableKeyGeneration) {
      throw new UnsupportedOperationException(
          "wrapKey shouldn't be called as key generation is enabled.");
    } else {
      EncryptRequest encryptRequest =
          EncryptRequest.builder()
              .keyId(wrappingKeyId)
              .plaintext(SdkBytes.fromByteBuffer(key))
              .build();
      EncryptResponse response = client().encrypt(encryptRequest);
      return Base64.getEncoder().encodeToString(response.ciphertextBlob().asByteArray());
    }
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    byte[] keyBytes = Base64.getDecoder().decode(wrappedKey);
    DecryptRequest decryptRequest =
        DecryptRequest.builder()
            .keyId(wrappingKeyId)
            .ciphertextBlob(SdkBytes.fromByteArray(keyBytes))
            .build();
    DecryptResponse decryptResponse = client().decrypt(decryptRequest);
    return ByteBuffer.wrap(decryptResponse.plaintext().asByteArray());
  }

  @Override
  public void initialize(Map<String, String> properties) {
    dataKeySpec = properties.getOrDefault(DATA_KEY_SPEC, DataKeySpec.AES_256.name());
    enableKeyGeneration =
        Boolean.parseBoolean(properties.getOrDefault(ENABLE_KEY_GENERATION, "true"));
    if (kms == null) {
      kms = AwsClientFactories.from(properties)::kms;
    }
  }

  private software.amazon.awssdk.services.kms.KmsClient client() {
    if (client == null) {
      client = kms.get();
    }
    return client;
  }

  @Override
  public void close() {
    if (isResourceClosed.compareAndSet(false, true)) {
      if (client != null) {
        client.close();
      }
    }
  }
}
