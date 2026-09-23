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
import com.aliyun.teautil.models.RuntimeOptions;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SerializableMap;

/**
 * Key management client implementation that uses Alibaba Cloud KMS. Encrypts/decrypts keys with a
 * KMS-managed master key (referenced by its key id) and generates new data keys.
 */
public class AliyunKeyManagementClient implements KeyManagementClient {

  /**
   * Enables server-side data key generation. When enabled (the default), Iceberg calls {@link
   * #generateKey(String)}; set to {@code false} to have Iceberg generate keys locally and {@link
   * #wrapKey(ByteBuffer, String)} them via KMS.
   */
  public static final String ENABLE_KEY_GENERATION = "kms.client.aliyun.key.generation.enabled";

  /** Maximum number of attempts (including the first) for each KMS call. */
  public static final String CLIENT_MAX_ATTEMPTS = "kms.client.aliyun.max.attempts";

  /** Connect timeout in milliseconds for KMS calls. */
  public static final String CLIENT_CONNECT_TIMEOUT_MS = "kms.client.aliyun.connect.timeout.ms";

  /** Read timeout in milliseconds for KMS calls. */
  public static final String CLIENT_READ_TIMEOUT_MS = "kms.client.aliyun.read.timeout.ms";

  private static final boolean DEFAULT_ENABLE_KEY_GENERATION = true;
  private static final int DEFAULT_MAX_ATTEMPTS = 3;
  private static final int DEFAULT_CONNECT_TIMEOUT_MS = 2_000;
  private static final int DEFAULT_READ_TIMEOUT_MS = 30_000;
  private static final int BACKOFF_PERIOD_MS = 100;
  private static final String ALIAS_PREFIX = "alias/";
  private static final String ARN_KEY_SEPARATOR = "key/";

  private Map<String, String> allProperties;
  private String dataKeySpec;
  private boolean enableKeyGeneration = DEFAULT_ENABLE_KEY_GENERATION;
  private int maxAttempts = DEFAULT_MAX_ATTEMPTS;
  private int connectTimeoutMs = DEFAULT_CONNECT_TIMEOUT_MS;
  private int readTimeoutMs = DEFAULT_READ_TIMEOUT_MS;

  private transient volatile ClientState state;

  @Override
  public void initialize(Map<String, String> properties) {
    this.allProperties = SerializableMap.copyOf(properties);
    this.dataKeySpec = new AliyunProperties(properties).kmsDataKeySpec();
    this.enableKeyGeneration =
        PropertyUtil.propertyAsBoolean(
            properties, ENABLE_KEY_GENERATION, DEFAULT_ENABLE_KEY_GENERATION);
    this.maxAttempts =
        PropertyUtil.propertyAsInt(properties, CLIENT_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS);
    this.connectTimeoutMs =
        PropertyUtil.propertyAsInt(
            properties, CLIENT_CONNECT_TIMEOUT_MS, DEFAULT_CONNECT_TIMEOUT_MS);
    this.readTimeoutMs =
        PropertyUtil.propertyAsInt(properties, CLIENT_READ_TIMEOUT_MS, DEFAULT_READ_TIMEOUT_MS);
  }

  @Override
  public boolean supportsKeyGeneration() {
    return enableKeyGeneration;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    GenerateDataKeyRequest request =
        new GenerateDataKeyRequest().setKeyId(wrappingKeyId).setKeySpec(dataKeySpec);
    try {
      GenerateDataKeyResponse response =
          client().generateDataKeyWithOptions(request, runtimeOptions());
      GenerateDataKeyResponseBody body = response.getBody();
      return new KeyGenerationResult(
          base64ToBuffer(body.getPlaintext()), base64ToBuffer(body.getCiphertextBlob()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate data key with Aliyun KMS", e);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    if (enableKeyGeneration) {
      throw new UnsupportedOperationException(
          "wrapKey shouldn't be called as key generation is enabled.");
    }

    EncryptRequest request =
        new EncryptRequest()
            .setKeyId(wrappingKeyId)
            .setPlaintext(Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(key)));
    try {
      EncryptResponse response = client().encryptWithOptions(request, runtimeOptions());
      return base64ToBuffer(response.getBody().getCiphertextBlob());
    } catch (Exception e) {
      throw new RuntimeException("Failed to wrap key with Aliyun KMS", e);
    }
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    DecryptResponse response;
    try {
      DecryptRequest request =
          new DecryptRequest()
              .setCiphertextBlob(
                  Base64.getEncoder().encodeToString(ByteBuffers.toByteArray(wrappedKey)));
      response = client().decryptWithOptions(request, runtimeOptions());
    } catch (Exception e) {
      throw new RuntimeException("Failed to unwrap key with Aliyun KMS", e);
    }

    // Verify the key that wrapped the data key. Skip aliases: an alias is a mutable pointer whose
    // target may differ from the key that wrapped older data. Only bare ids and ARNs are checked.
    String actualKeyId = response.getBody().getKeyId();
    if (!isAlias(wrappingKeyId)) {
      String expectedKeyId = keyIdFromRef(wrappingKeyId);
      Preconditions.checkState(
          expectedKeyId.equals(actualKeyId),
          "Data key was wrapped by KMS key %s, but unwrap expected key %s",
          actualKeyId,
          expectedKeyId);
    }
    return base64ToBuffer(response.getBody().getPlaintext());
  }

  private static ByteBuffer base64ToBuffer(String base64) {
    return ByteBuffer.wrap(Base64.getDecoder().decode(base64));
  }

  // bare alias "alias/name" or alias ARN "acs:kms:...:alias/name"
  private static boolean isAlias(String keyRef) {
    return keyRef.contains(ALIAS_PREFIX);
  }

  // key ARN -> id after "key/"; bare id -> unchanged
  private static String keyIdFromRef(String keyRef) {
    int idx = keyRef.lastIndexOf(ARN_KEY_SEPARATOR);
    return idx >= 0 ? keyRef.substring(idx + ARN_KEY_SEPARATOR.length()) : keyRef;
  }

  private Client client() {
    return state().client;
  }

  private RuntimeOptions runtimeOptions() {
    return state().runtimeOptions;
  }

  private ClientState state() {
    if (state == null) {
      synchronized (this) {
        if (state == null) {
          Client kmsClient = AliyunClientFactories.from(allProperties).newKmsClient();
          RuntimeOptions options =
              new RuntimeOptions()
                  .setAutoretry(true)
                  .setMaxAttempts(maxAttempts)
                  .setBackoffPolicy("fixed")
                  .setBackoffPeriod(BACKOFF_PERIOD_MS)
                  .setConnectTimeout(connectTimeoutMs)
                  .setReadTimeout(readTimeoutMs);
          state = new ClientState(kmsClient, options);
        }
      }
    }
    return state;
  }

  private static class ClientState {
    private final Client client;
    private final RuntimeOptions runtimeOptions;

    ClientState(Client client, RuntimeOptions runtimeOptions) {
      this.client = client;
      this.runtimeOptions = runtimeOptions;
    }
  }
}
