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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.aliyun.kms20160120.Client;
import com.aliyun.kms20160120.models.DecryptRequest;
import com.aliyun.kms20160120.models.DecryptResponse;
import com.aliyun.kms20160120.models.DecryptResponseBody;
import com.aliyun.kms20160120.models.EncryptRequest;
import com.aliyun.kms20160120.models.EncryptResponse;
import com.aliyun.kms20160120.models.EncryptResponseBody;
import com.aliyun.kms20160120.models.GenerateDataKeyRequest;
import com.aliyun.kms20160120.models.GenerateDataKeyResponse;
import com.aliyun.kms20160120.models.GenerateDataKeyResponseBody;
import com.aliyun.oss.OSS;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestAliyunKeyManagementClient {

  private static final String WRAPPING_KEY_ID = "test-wrapping-key";
  private static final byte[] RAW_KEY =
      "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8);
  private static final String RAW_KEY_B64 = Base64.getEncoder().encodeToString(RAW_KEY);
  // Aliyun KMS returns the ciphertext blob as a Base64 string; the wrapped key carries the raw
  // (decoded) ciphertext bytes.
  private static final String CIPHERTEXT_BLOB =
      Base64.getEncoder().encodeToString("wrapped-key-material".getBytes(StandardCharsets.UTF_8));
  private static final ByteBuffer WRAPPED_KEY =
      ByteBuffer.wrap(Base64.getDecoder().decode(CIPHERTEXT_BLOB));

  // shared with the reflectively-loaded MockClientFactory below; ThreadLocal keeps it
  // parallel-safe.
  private static final ThreadLocal<Client> MOCK_KMS = new ThreadLocal<>();

  @BeforeEach
  public void before() {
    MOCK_KMS.set(mock(Client.class));
  }

  @AfterEach
  public void after() {
    MOCK_KMS.remove();
  }

  private static Client mockKms() {
    return MOCK_KMS.get();
  }

  @Test
  public void testSupportsKeyGeneration() {
    assertThat(kmsClient().supportsKeyGeneration()).isTrue();
  }

  @Test
  public void testGenerateKey() throws Exception {
    GenerateDataKeyResponse response =
        new GenerateDataKeyResponse()
            .setBody(
                new GenerateDataKeyResponseBody()
                    .setPlaintext(RAW_KEY_B64)
                    .setCiphertextBlob(CIPHERTEXT_BLOB));
    when(mockKms().generateDataKey(any(GenerateDataKeyRequest.class))).thenReturn(response);

    KeyManagementClient.KeyGenerationResult result = kmsClient().generateKey(WRAPPING_KEY_ID);
    assertThat(result.key()).isEqualTo(ByteBuffer.wrap(RAW_KEY));
    assertThat(result.wrappedKey()).isEqualTo(WRAPPED_KEY);

    ArgumentCaptor<GenerateDataKeyRequest> captor =
        ArgumentCaptor.forClass(GenerateDataKeyRequest.class);
    verify(mockKms()).generateDataKey(captor.capture());
    assertThat(captor.getValue().getKeyId()).isEqualTo(WRAPPING_KEY_ID);
    assertThat(captor.getValue().getKeySpec()).isEqualTo("AES_256");
  }

  @Test
  public void testWrapKey() throws Exception {
    EncryptResponse response =
        new EncryptResponse().setBody(new EncryptResponseBody().setCiphertextBlob(CIPHERTEXT_BLOB));
    when(mockKms().encrypt(any(EncryptRequest.class))).thenReturn(response);

    ByteBuffer wrapped = kmsClient().wrapKey(ByteBuffer.wrap(RAW_KEY), WRAPPING_KEY_ID);
    assertThat(wrapped).isEqualTo(WRAPPED_KEY);

    ArgumentCaptor<EncryptRequest> captor = ArgumentCaptor.forClass(EncryptRequest.class);
    verify(mockKms()).encrypt(captor.capture());
    assertThat(captor.getValue().getKeyId()).isEqualTo(WRAPPING_KEY_ID);
    assertThat(captor.getValue().getPlaintext()).isEqualTo(RAW_KEY_B64);
  }

  @Test
  public void testUnwrapKey() throws Exception {
    DecryptResponse response =
        new DecryptResponse().setBody(new DecryptResponseBody().setPlaintext(RAW_KEY_B64));
    when(mockKms().decrypt(any(DecryptRequest.class))).thenReturn(response);

    ByteBuffer unwrapped = kmsClient().unwrapKey(WRAPPED_KEY.duplicate(), WRAPPING_KEY_ID);
    assertThat(unwrapped).isEqualTo(ByteBuffer.wrap(RAW_KEY));

    ArgumentCaptor<DecryptRequest> captor = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(mockKms()).decrypt(captor.capture());
    assertThat(captor.getValue().getCiphertextBlob()).isEqualTo(CIPHERTEXT_BLOB);
  }

  private KeyManagementClient kmsClient() {
    KeyManagementClient client = new AliyunKeyManagementClient();
    client.initialize(
        ImmutableMap.of(AliyunProperties.CLIENT_FACTORY, MockClientFactory.class.getName()));
    return client;
  }

  /**
   * Reflectively loaded by {@link AliyunKeyManagementClient} to hand back the mocked KMS client.
   */
  public static class MockClientFactory implements AliyunClientFactory {
    private AliyunProperties aliyunProperties;

    public MockClientFactory() {}

    @Override
    public OSS newOSSClient() {
      return null;
    }

    @Override
    public Client newKmsClient() {
      return mockKms();
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.aliyunProperties = new AliyunProperties(properties);
    }

    @Override
    public AliyunProperties aliyunProperties() {
      return aliyunProperties;
    }
  }
}
