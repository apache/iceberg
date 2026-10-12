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
import com.aliyun.teautil.models.RuntimeOptions;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableMap;

/**
 * Key management client implementation that uses Alibaba Cloud KMS. Wraps (encrypts) and unwraps
 * (decrypts) data keys with a KMS-managed master key referenced by its key id.
 */
public class AliyunKeyManagementClient implements KeyManagementClient {

  private static final int BACKOFF_PERIOD_MS = 100;
  private static final String ALIAS_PREFIX = "alias/";
  private static final String ARN_KEY_SEPARATOR = "key/";

  private Map<String, String> allProperties;
  private AliyunProperties aliyunProperties;

  private transient volatile ClientState state;

  @Override
  public void initialize(Map<String, String> properties) {
    this.allProperties = SerializableMap.copyOf(properties);
    this.aliyunProperties = new AliyunProperties(properties);
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
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
                  .setMaxAttempts(aliyunProperties.kmsClientMaxAttempts())
                  .setBackoffPolicy("fixed")
                  .setBackoffPeriod(BACKOFF_PERIOD_MS)
                  .setConnectTimeout(aliyunProperties.kmsClientConnectTimeoutMs())
                  .setReadTimeout(aliyunProperties.kmsClientReadTimeoutMs());
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
