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
package org.apache.iceberg.azure.keymanagement;

import static org.apache.iceberg.azure.AzureProperties.AZURE_KEYVAULT_URL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.models.KeyType;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = "AZURE_KEYVAULT_URL", matches = ".*")
})
public class TestAzureKeyManagementClient {
  private static final String ICEBERG_TEST_KEY_NAME = "iceberg-test-key";

  private static final String KEY_VAULT_URI = System.getenv("AZURE_KEYVAULT_URL");

  private static KeyManagementClient azureKeyManagementClient;
  private static KeyClient keyClient;
  private static String vaultToken;

  @BeforeAll
  public static void beforeClass() {
    TokenCredential credential = new DefaultAzureCredentialBuilder().build();
    keyClient = new KeyClientBuilder().vaultUrl(KEY_VAULT_URI).credential(credential).buildClient();
    keyClient.createKey(ICEBERG_TEST_KEY_NAME, KeyType.RSA);
    // The KMS client no longer falls back to ambient credentials, so vend it a Key Vault-scoped
    // token explicitly (as a catalog would) using the same ambient identity this test runs with.
    vaultToken =
        credential
            .getToken(new TokenRequestContext().addScopes("https://vault.azure.net/.default"))
            .block()
            .getToken();
    azureKeyManagementClient = newClient();
  }

  @AfterAll
  public static void afterClass() {
    if (keyClient != null) {
      keyClient.beginDeleteKey(ICEBERG_TEST_KEY_NAME).waitForCompletion(Duration.ofMinutes(3));
      keyClient.purgeDeletedKey(ICEBERG_TEST_KEY_NAME);
    }
  }

  @Test
  public void wrapKeyFailsWithoutCatalogProvidedCredential() {
    // Configuring only the vault URL used to authenticate via the ambient DefaultAzureCredential.
    // After the security fix that path is rejected: the client requires a catalog-provided
    // credential, so this previously-working configuration now fails fast.
    AzureKeyManagementClient client = new AzureKeyManagementClient();
    client.initialize(ImmutableMap.of(AZURE_KEYVAULT_URL, KEY_VAULT_URI));

    ByteBuffer key = ByteBuffer.wrap("table-master-key".getBytes());

    assertThatThrownBy(() -> client.wrapKey(key, ICEBERG_TEST_KEY_NAME))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("ambient")
        .hasMessageContaining(AzureProperties.ADLS_TOKEN);
  }

  @Test
  public void keyWrapping() {
    ByteBuffer key = ByteBuffer.wrap("table-master-key".getBytes());

    ByteBuffer encryptedKey = azureKeyManagementClient.wrapKey(key, ICEBERG_TEST_KEY_NAME);
    ByteBuffer decryptedKey =
        azureKeyManagementClient.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME);

    assertThat(decryptedKey).isEqualTo(key);
  }

  @Test
  public void keyGenerationNotSupported() {
    assertThat(azureKeyManagementClient.supportsKeyGeneration()).isFalse();
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerialization(
      TestHelpers.RoundTripSerializer<AzureKeyManagementClient> roundTripSerializer)
      throws Exception {
    try (AzureKeyManagementClient keyManagementClient = newClient()) {
      AzureKeyManagementClient result = roundTripSerializer.apply(keyManagementClient);

      ByteBuffer key = ByteBuffer.wrap("super-secret-table-master-key".getBytes());
      ByteBuffer encryptedKey = result.wrapKey(key, ICEBERG_TEST_KEY_NAME);

      assertThat(keyManagementClient.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME)).isEqualTo(key);
      assertThat(result.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME)).isEqualTo(key);
    }
  }

  private static AzureKeyManagementClient newClient() {
    AzureKeyManagementClient client = new AzureKeyManagementClient();
    client.initialize(
        ImmutableMap.of(AZURE_KEYVAULT_URL, KEY_VAULT_URI, AzureProperties.ADLS_TOKEN, vaultToken));
    return client;
  }
}
