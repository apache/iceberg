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

import static org.apache.iceberg.azure.AzureProperties.KEYVAULT_URI;
import static org.assertj.core.api.Assertions.assertThat;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.models.KeyType;
import java.nio.ByteBuffer;
import java.time.Duration;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = "AZURE_CLIENT_ID", matches = ".*"),
  @EnabledIfEnvironmentVariable(named = "AZURE_CLIENT_SECRET", matches = ".*"),
  @EnabledIfEnvironmentVariable(named = "AZURE_TENANT_ID", matches = ".*"),
  @EnabledIfEnvironmentVariable(named = KEYVAULT_URI, matches = ".*")
})
public class AzureKeyManagementClientTest {
  private static final String ICEBERG_TEST_KEY_NAME = "iceberg-test-key";

  private static KeyClient keyClient;

  private static KeyManagementClient azureKeyManagementClient;

  @BeforeAll
  public static void beforeClass() {
    String keyVaultUri = System.getenv(KEYVAULT_URI);
    keyClient =
        new KeyClientBuilder()
            .vaultUrl(keyVaultUri)
            .credential(new DefaultAzureCredentialBuilder().build())
            .buildClient();
    keyClient.createKey(ICEBERG_TEST_KEY_NAME, KeyType.RSA);
    azureKeyManagementClient = new AzureKeyManagementClient();
    azureKeyManagementClient.initialize(ImmutableMap.of(KEYVAULT_URI, keyVaultUri));
  }

  @Test
  public void testKeyWrapping() {
    ByteBuffer key = ByteBuffer.wrap("table-master-key".getBytes());

    ByteBuffer encryptedKey = azureKeyManagementClient.wrapKey(key, ICEBERG_TEST_KEY_NAME);
    ByteBuffer decryptedKey =
        azureKeyManagementClient.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME);

    assertThat(decryptedKey).isEqualTo(key);
  }

  @Test
  public void testKeyGenerationNotSupported() {
    assertThat(azureKeyManagementClient.supportsKeyGeneration()).isFalse();
  }

  @AfterAll
  public static void afterClass() {
    String keyVaultUri = System.getenv(KEYVAULT_URI);
    keyClient =
        new KeyClientBuilder()
            .vaultUrl(keyVaultUri)
            .credential(new DefaultAzureCredentialBuilder().build())
            .buildClient();
    keyClient.beginDeleteKey(ICEBERG_TEST_KEY_NAME).waitForCompletion(Duration.ofMinutes(5));
    keyClient.purgeDeletedKey(ICEBERG_TEST_KEY_NAME);
  }
}
