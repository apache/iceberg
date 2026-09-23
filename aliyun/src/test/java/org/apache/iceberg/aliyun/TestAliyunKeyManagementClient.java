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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
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
import com.aliyun.teautil.models.RuntimeOptions;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

public class TestAliyunKeyManagementClient {

  private static final String WRAPPING_KEY_ID = "test-wrapping-key";
  private static final String SAMPLE_KEY_ID = "202b9877-5a25-46e3-a763-e20791b5abcd";
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

  private static void stubDecrypt(String responseKeyId) throws Exception {
    DecryptResponse response =
        new DecryptResponse()
            .setBody(new DecryptResponseBody().setPlaintext(RAW_KEY_B64).setKeyId(responseKeyId));
    when(mockKms().decryptWithOptions(any(DecryptRequest.class), any(RuntimeOptions.class)))
        .thenReturn(response);
  }

  @Test
  public void testSupportsKeyGenerationByDefault() {
    assertThat(kmsClient(ImmutableMap.of()).supportsKeyGeneration()).isTrue();
  }

  @Test
  public void testGenerateKey() throws Exception {
    GenerateDataKeyResponse response =
        new GenerateDataKeyResponse()
            .setBody(
                new GenerateDataKeyResponseBody()
                    .setPlaintext(RAW_KEY_B64)
                    .setCiphertextBlob(CIPHERTEXT_BLOB));
    when(mockKms()
            .generateDataKeyWithOptions(
                any(GenerateDataKeyRequest.class), any(RuntimeOptions.class)))
        .thenReturn(response);

    KeyManagementClient.KeyGenerationResult result =
        kmsClient(ImmutableMap.of()).generateKey(WRAPPING_KEY_ID);
    assertThat(result.key()).isEqualTo(ByteBuffer.wrap(RAW_KEY));
    assertThat(result.wrappedKey()).isEqualTo(WRAPPED_KEY);

    ArgumentCaptor<GenerateDataKeyRequest> captor =
        ArgumentCaptor.forClass(GenerateDataKeyRequest.class);
    verify(mockKms()).generateDataKeyWithOptions(captor.capture(), any(RuntimeOptions.class));
    assertThat(captor.getValue().getKeyId()).isEqualTo(WRAPPING_KEY_ID);
    assertThat(captor.getValue().getKeySpec()).isEqualTo("AES_256");
  }

  @Test
  public void testWrapKeyRejectedWhenKeyGenerationEnabled() {
    KeyManagementClient client = kmsClient(ImmutableMap.of());
    assertThatThrownBy(() -> client.wrapKey(ByteBuffer.wrap(RAW_KEY), WRAPPING_KEY_ID))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("key generation is enabled");
  }

  @Test
  public void testWrapKey() throws Exception {
    EncryptResponse response =
        new EncryptResponse().setBody(new EncryptResponseBody().setCiphertextBlob(CIPHERTEXT_BLOB));
    when(mockKms().encryptWithOptions(any(EncryptRequest.class), any(RuntimeOptions.class)))
        .thenReturn(response);

    KeyManagementClient client =
        kmsClient(ImmutableMap.of(AliyunKeyManagementClient.ENABLE_KEY_GENERATION, "false"));
    assertThat(client.supportsKeyGeneration()).isFalse();

    ByteBuffer wrapped = client.wrapKey(ByteBuffer.wrap(RAW_KEY), WRAPPING_KEY_ID);
    assertThat(wrapped).isEqualTo(WRAPPED_KEY);

    ArgumentCaptor<EncryptRequest> captor = ArgumentCaptor.forClass(EncryptRequest.class);
    verify(mockKms()).encryptWithOptions(captor.capture(), any(RuntimeOptions.class));
    assertThat(captor.getValue().getKeyId()).isEqualTo(WRAPPING_KEY_ID);
    assertThat(captor.getValue().getPlaintext()).isEqualTo(RAW_KEY_B64);
  }

  @Test
  public void testUnwrapKey() throws Exception {
    stubDecrypt(WRAPPING_KEY_ID);

    ByteBuffer unwrapped =
        kmsClient(ImmutableMap.of()).unwrapKey(WRAPPED_KEY.duplicate(), WRAPPING_KEY_ID);
    assertThat(unwrapped).isEqualTo(ByteBuffer.wrap(RAW_KEY));

    ArgumentCaptor<DecryptRequest> captor = ArgumentCaptor.forClass(DecryptRequest.class);
    verify(mockKms()).decryptWithOptions(captor.capture(), any(RuntimeOptions.class));
    assertThat(captor.getValue().getCiphertextBlob()).isEqualTo(CIPHERTEXT_BLOB);
  }

  @Test
  public void testUnwrapKeyRejectsMismatchedKeyId() throws Exception {
    stubDecrypt("some-other-key");

    KeyManagementClient client = kmsClient(ImmutableMap.of());
    assertThatThrownBy(() -> client.unwrapKey(WRAPPED_KEY.duplicate(), WRAPPING_KEY_ID))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("some-other-key")
        .hasMessageContaining(WRAPPING_KEY_ID);
  }

  @Test
  public void testUnwrapKeyWithArnKeyId() throws Exception {
    String arn = "acs:kms:cn-hangzhou:1234567890123456:key/" + SAMPLE_KEY_ID;
    stubDecrypt(SAMPLE_KEY_ID);

    // an ARN is normalized to its bare key id locally and matches the response
    assertThat(kmsClient(ImmutableMap.of()).unwrapKey(WRAPPED_KEY.duplicate(), arn))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));
  }

  @Test
  public void testUnwrapKeySkipsVerificationForAlias() throws Exception {
    // an alias skips key-id verification, so unwrap succeeds even when the response keyId differs
    stubDecrypt(SAMPLE_KEY_ID);

    assertThat(
            kmsClient(ImmutableMap.of()).unwrapKey(WRAPPED_KEY.duplicate(), "alias/maps-data-key"))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testKmsClientSerialization(
      TestHelpers.RoundTripSerializer<KeyManagementClient> roundTripSerializer) throws Exception {
    when(mockKms()
            .generateDataKeyWithOptions(
                any(GenerateDataKeyRequest.class), any(RuntimeOptions.class)))
        .thenReturn(
            new GenerateDataKeyResponse()
                .setBody(
                    new GenerateDataKeyResponseBody()
                        .setPlaintext(RAW_KEY_B64)
                        .setCiphertextBlob(CIPHERTEXT_BLOB)));
    when(mockKms().encryptWithOptions(any(EncryptRequest.class), any(RuntimeOptions.class)))
        .thenReturn(
            new EncryptResponse()
                .setBody(new EncryptResponseBody().setCiphertextBlob(CIPHERTEXT_BLOB)));
    stubDecrypt(WRAPPING_KEY_ID);

    Map<String, String> baseProps =
        ImmutableMap.of(
            AliyunProperties.CLIENT_REGION, "cn-hangzhou",
            AliyunProperties.KMS_DATA_KEY_SPEC, "AES_256",
            AliyunKeyManagementClient.CLIENT_MAX_ATTEMPTS, "5");

    // key generation enabled (default): generate + unwrap survive serialization
    KeyManagementClient genClient = kmsClient(baseProps);
    assertThat(genClient.supportsKeyGeneration()).isTrue();
    KeyManagementClient genRoundTripped = roundTripSerializer.apply(genClient);
    assertThat(genRoundTripped.supportsKeyGeneration()).isTrue();
    KeyManagementClient.KeyGenerationResult generated =
        genRoundTripped.generateKey(WRAPPING_KEY_ID);
    assertThat(generated.key()).isEqualTo(ByteBuffer.wrap(RAW_KEY));
    assertThat(generated.wrappedKey()).isEqualTo(WRAPPED_KEY);
    assertThat(genClient.unwrapKey(WRAPPED_KEY.duplicate(), WRAPPING_KEY_ID))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));
    assertThat(genRoundTripped.unwrapKey(WRAPPED_KEY.duplicate(), WRAPPING_KEY_ID))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));

    // key generation disabled: wrap + unwrap survive serialization
    Map<String, String> wrapProps =
        ImmutableMap.<String, String>builder()
            .putAll(baseProps)
            .put(AliyunKeyManagementClient.ENABLE_KEY_GENERATION, "false")
            .build();
    KeyManagementClient wrapClient = kmsClient(wrapProps);
    assertThat(wrapClient.supportsKeyGeneration()).isFalse();
    KeyManagementClient wrapRoundTripped = roundTripSerializer.apply(wrapClient);
    assertThat(wrapRoundTripped.supportsKeyGeneration()).isFalse();
    ByteBuffer wrapped = wrapRoundTripped.wrapKey(ByteBuffer.wrap(RAW_KEY), WRAPPING_KEY_ID);
    assertThat(wrapped).isEqualTo(WRAPPED_KEY);
    assertThat(wrapClient.unwrapKey(wrapped.duplicate(), WRAPPING_KEY_ID))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));
    assertThat(wrapRoundTripped.unwrapKey(wrapped.duplicate(), WRAPPING_KEY_ID))
        .isEqualTo(ByteBuffer.wrap(RAW_KEY));
  }

  private KeyManagementClient kmsClient(Map<String, String> extraProps) {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .put(AliyunProperties.CLIENT_FACTORY, MockClientFactory.class.getName())
            .putAll(extraProps)
            .build();
    KeyManagementClient client = new AliyunKeyManagementClient();
    client.initialize(properties);
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
