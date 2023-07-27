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

package org.apache.iceberg.encryption.kms;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import okhttp3.ConnectionSpec;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.iceberg.encryption.KmsClient;


/**
 * Example of a KMS client for testing and demonstrations; not for use in production.
 * An implementation of {@link KmsClient} that relies on Hashicorp Vault transit engine to
 * manage encryption keys.
 * On initialization it is manadatory to set the properties {@link VaultKmsClient#KMS_INSTANCE_URL_PROP}
 * and {@link VaultKmsClient#ACCESS_TOKEN_FILE_PROP}.
 * Authentication to Vault is done using an access token saved in file pointed to by the property
 * {@link VaultKmsClient#ACCESS_TOKEN_FILE_PROP}.
 * The token is refreshed from the file on wrap/unwrap after the number of seconds
 * defined in {@link VaultKmsClient#ACCESS_TOKEN_VALIDITY_SEC_PROP}, or after the
 * default period defined by {@link VaultKmsClient#DEFAULT_ACCESS_TOKEN_VALIDITY_SEC}.
 * Pre-requisite: install Hashicorp Vault and enable transit engine as per
 * https://www.vaultproject.io/docs/secrets/transit
 */
public class VaultKmsClient implements KmsClient {
  /**
   * Property name for Vault access token file path.
   */
  public static final String ACCESS_TOKEN_FILE_PROP = "kms.client.vault.access.token.file";

  /**
   * Property name for Vault instance URL
   */
  public static final String KMS_INSTANCE_URL_PROP = "kms.client.vault.instance.url";

  /**
   * Property name for Vault access token validity in seconds - how often to refresh the token from file
   */
  public static final String ACCESS_TOKEN_VALIDITY_SEC_PROP = "kms.client.vault.access.token.validity.sec";

  private static final String DEFAULT_ACCESS_TOKEN_VALIDITY_SEC = Integer.toString(10 * 60);
  private static final MediaType JSON_MEDIA_TYPE = MediaType.get("application/json; charset=utf-8");
  private static final String TRANSIT_ENGINE = "/v1/transit/";
  private static final String TRANSIT_WRAP_ENDPOINT = "encrypt/";
  private static final String TRANSIT_UNWRAP_ENDPOINT = "decrypt/";
  private static final String TOKEN_HEADER = "X-Vault-Token";
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private String accessTokenFile;
  private String kmsToken;
  private long kmsTokenNextRefreshTimestamp;
  private int kmsTokenValiditySeconds;
  private String endPointPrefix;

  private transient OkHttpClient httpClient = new OkHttpClient.Builder()
      .connectionSpecs(Arrays.asList(ConnectionSpec.MODERN_TLS, ConnectionSpec.COMPATIBLE_TLS,
          ConnectionSpec.CLEARTEXT)) // Disable cleartext if it is not for testing.
      .build();

  @Override
  public void initialize(Map<String, String> properties) {
    kmsTokenValiditySeconds = Integer.parseInt(
        properties.getOrDefault(ACCESS_TOKEN_VALIDITY_SEC_PROP,
            DEFAULT_ACCESS_TOKEN_VALIDITY_SEC));

    accessTokenFile = properties.get(ACCESS_TOKEN_FILE_PROP);
    if (null == accessTokenFile) {
      throw new RuntimeException("Access token file path is not set: " + ACCESS_TOKEN_FILE_PROP);
    }
    kmsTokenNextRefreshTimestamp = 0L;
    refreshToken();

    String kmsInstanceURL = properties.get(KMS_INSTANCE_URL_PROP);
    if (null == kmsInstanceURL) {
      throw new RuntimeException("Required property is not set: " + KMS_INSTANCE_URL_PROP);
    }
    endPointPrefix = kmsInstanceURL + TRANSIT_ENGINE;
  }

  @Override
  public String wrapKey(ByteBuffer key, String wrappingKeyId) {
    refreshToken();
    Map<String, String> writeKeyMap = new HashMap<>(1);
    final String dataKeyStr = Base64.getEncoder().encodeToString(key.array());
    writeKeyMap.put("plaintext", dataKeyStr);
    String response = getContentFromVault(endPointPrefix + TRANSIT_WRAP_ENDPOINT,
        writeKeyMap, wrappingKeyId);
    String ciphertext = parseResponse(response, "ciphertext");
    return ciphertext;
  }

  @Override
  public ByteBuffer unwrapKey(String wrappedKey, String wrappingKeyId) {
    refreshToken();
    Map<String, String> writeKeyMap = new HashMap<>(1);
    writeKeyMap.put("ciphertext", wrappedKey);
    String response = getContentFromVault(endPointPrefix + TRANSIT_UNWRAP_ENDPOINT,
        writeKeyMap, wrappingKeyId);
    String plaintext = parseResponse(response, "plaintext");
    final byte[] key = Base64.getDecoder().decode(plaintext);
    return ByteBuffer.wrap(key);
  }

  private String getContentFromVault(String endPoint, Map<String, String> paramMap,
                                     String masterKeyIdentifier) {
    String jPayload = buildPayload(paramMap);
    final RequestBody requestBody = RequestBody.create(JSON_MEDIA_TYPE, jPayload);
    Request request = new Request.Builder()
        .url(endPoint + masterKeyIdentifier)
        .header(TOKEN_HEADER, kmsToken)
        .post(requestBody).build();

    Response response = null;
    try {
      response = httpClient.newCall(request).execute();
      final String responseBody = response.body().string();
      if (response.isSuccessful()) {
        return responseBody;
      } else {
        if ((401 == response.code()) || (403 == response.code())) {
          throw new RuntimeException(responseBody); // A more specific exception can be defined
        }
        throw new RuntimeException("Vault call [" + request.url() + "] didn't succeed: " + responseBody);
      }
    } catch (IOException e) {
      throw new RuntimeException("Vault call [" + request.url() + "] didn't succeed", e);
    } finally {
      if (null != response) {
        response.close();
      }
    }
  }

  private String buildPayload(Map<String, String> paramMap) {
    String jsonValue;
    try {
      jsonValue = objectMapper.writeValueAsString(paramMap);
    } catch (IOException e) {
      throw new RuntimeException("Failed to build payload", e);
    }
    return jsonValue;
  }

  private static String parseResponse(String response, String searchKey) {
    String matchingValue;
    try {
      matchingValue = objectMapper.readTree(response).findValue(searchKey).textValue();
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse vault response. " + searchKey + " not found in: " + response, e);
    }

    if (null == matchingValue) {
      throw new RuntimeException("Failed to match vault response. " + searchKey + " not found in: " + response);
    }
    return matchingValue;
  }

  private synchronized void refreshToken() {
    long nowTimestamp = System.currentTimeMillis();
    if (nowTimestamp < kmsTokenNextRefreshTimestamp) {
      return;
    }
    kmsTokenNextRefreshTimestamp = System.currentTimeMillis() + kmsTokenValiditySeconds;
    if (null == accessTokenFile) {
      throw new RuntimeException("Access token file path is not set: " + ACCESS_TOKEN_FILE_PROP);
    }
    Path path = Paths.get(accessTokenFile);
    try {
      kmsToken = Files.readAllLines(path).get(0);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read access token from file : " + accessTokenFile, e);
    }

  }
}
