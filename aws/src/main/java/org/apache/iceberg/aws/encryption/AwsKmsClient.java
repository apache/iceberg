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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyRequest;
import software.amazon.awssdk.services.kms.model.GenerateDataKeyResponse;

public class AwsKmsClient implements org.apache.iceberg.encryption.KmsClient {

  private KmsClient kms;
  private static final Logger LOG = LoggerFactory.getLogger(AwsKmsClient.class);

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    EncryptResponse response = null;
    SdkBytes plainTextKey = SdkBytes.fromByteBuffer(key);
    try {
      response =
          kms.encrypt(
              EncryptRequest.builder().keyId(wrappingKeyId).plaintext(plainTextKey).build());
    } catch (Exception e) {
      LOG.error("Fail to wrap key {} with wrappingKeyId {}", key.toString(), wrappingKeyId, e);
      throw e;
    }
    String wrappedKey = response.ciphertextBlob().asUtf8String();
    return wrappedKey;
  }

  /** AWS KMS supports key generation */
  @Override
  public boolean supportsKeyGeneration() {
    return true;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    GenerateDataKeyResponse response = null;
    try {
      response = kms.generateDataKey(GenerateDataKeyRequest.builder().keyId(wrappingKeyId).build());

    } catch (Exception e) {
      LOG.error("Fail to generate key with wrappingKeyId {}", wrappingKeyId, e);
      throw e;
    }

    ByteBuffer plainTextKey = response.plaintext().asByteBuffer();
    String wrappedKey = response.ciphertextBlob().asUtf8String();
    return new KeyGenerationResult(plainTextKey, wrappedKey);
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    DecryptResponse response = null;
    try {
      response =
          kms.decrypt(
              DecryptRequest.builder()
                  .ciphertextBlob(SdkBytes.fromUtf8String(wrappedKey))
                  .keyId(wrappingKeyId)
                  .build());
    } catch (Exception e) {
      LOG.error("Fail to decrypt key {} with wrappingKeyId {}", wrappedKey, wrappingKeyId, e);
      throw e;
    }
    ByteBuffer plainTextKey = response.plaintext().asByteBuffer();
    return plainTextKey;
  }

  @Override
  public void initialize(Map<String, String> properties) {
    this.kms = AwsClientFactories.from(properties).kms();
  }
}
