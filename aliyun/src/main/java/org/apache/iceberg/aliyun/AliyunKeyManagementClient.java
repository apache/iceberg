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
package org.apache.iceberg.aliyun;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.DecryptRequest;
import com.aliyun.kms20160120.models.DecryptResponse;
import com.aliyun.kms20160120.models.EncryptRequest;
import com.aliyun.kms20160120.models.EncryptResponse;
import com.aliyun.kms20160120.models.GenerateDataKeyRequest;
import com.aliyun.kms20160120.models.GenerateDataKeyResponse;
import com.aliyun.kms20160120.models.GenerateDataKeyResponseBody;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableMap;

/**
 * Key management client implementation that uses Alibaba Cloud KMS. Encrypts/decrypts keys with a
 * KMS-managed master key (referenced by its key id) and generates new data keys.
 */
public class AliyunKeyManagementClient implements KeyManagementClient {

  private Map<String, String> allProperties;
  private String dataKeySpec;

  private transient volatile Client kmsClient;

  @Override
  public void initialize(Map<String, String> properties) {
    this.allProperties = SerializableMap.copyOf(properties);
    this.dataKeySpec = new AliyunProperties(properties).kmsDataKeySpec();
  }

  @Override
  public boolean supportsKeyGeneration() {
    return true;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    GenerateDataKeyRequest request =
        new GenerateDataKeyRequest().setKeyId(wrappingKeyId).setKeySpec(dataKeySpec);
    try {
      GenerateDataKeyResponse response = kmsClient().generateDataKey(request);
      GenerateDataKeyResponseBody body = response.getBody();
      return new KeyGenerationResult(
          ByteBuffer.wrap(Base64.getDecoder().decode(body.getPlaintext())),
          wrappedKey(body.getCiphertextBlob()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate data key with Aliyun KMS", e);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    EncryptRequest request =
        new EncryptRequest()
            .setKeyId(wrappingKeyId)
            .setPlaintext(Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(key)));
    try {
      EncryptResponse response = kmsClient().encrypt(request);
      return wrappedKey(response.getBody().getCiphertextBlob());
    } catch (Exception e) {
      throw new RuntimeException("Failed to wrap key with Aliyun KMS", e);
    }
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    DecryptRequest request =
        new DecryptRequest()
            .setCiphertextBlob(
                Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(wrappedKey)));
    try {
      DecryptResponse response = kmsClient().decrypt(request);
      return ByteBuffer.wrap(Base64.getDecoder().decode(response.getBody().getPlaintext()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to unwrap key with Aliyun KMS", e);
    }
  }

  // Aliyun KMS returns the ciphertext blob as a Base64 string; decode it so the wrapped key is
  // carried as raw ciphertext bytes, and re-encode to Base64 when calling Decrypt.
  private static ByteBuffer wrappedKey(String ciphertextBlob) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(ciphertextBlob));
  }

  private Client kmsClient() {
    if (kmsClient == null) {
      synchronized (this) {
        if (kmsClient == null) {
          this.kmsClient = AliyunClientFactories.from(allProperties).newKmsClient();
        }
      }
    }
    return kmsClient;
  }
}
