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
package org.apache.iceberg.hashicorp;

import static org.apache.iceberg.hashicorp.VaultProperties.VAULT_ADDRESS_PROP;
import static org.apache.iceberg.hashicorp.VaultProperties.VAULT_ROLE_ID_PROP;
import static org.apache.iceberg.hashicorp.VaultProperties.VAULT_SECRET_ID_PROP;
import static org.apache.iceberg.hashicorp.VaultProperties.VAULT_TOKEN_PROP;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.vault.VaultContainer;

@Testcontainers
class TestVaultKeyManagementClient {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String ICEBERG_TEST_KEY_NAME = "iceberg-test-key";
  private static final String VAULT_TOKEN = "root-token";
  private static final String APPROLE_ROLE_NAME = "iceberg-role";

  @Container
  @SuppressWarnings("resource")
  private static final VaultContainer<?> VAULT =
      new VaultContainer<>("hashicorp/vault:1.21")
          .withVaultToken(VAULT_TOKEN)
          .withInitCommand(
              "secrets enable transit",
              "write -f transit/keys/" + ICEBERG_TEST_KEY_NAME,
              "auth enable approle",
              "write sys/policy/transit-policy policy='path \"transit/*\" { capabilities = [\"create\", \"read\", \"update\", \"delete\", \"list\"] }'",
              "write auth/approle/role/" + APPROLE_ROLE_NAME + " token_policies=transit-policy");

  @Test
  void tokenAuthentication() {
    try (KeyManagementClient client = new VaultKeyManagementClient()) {
      client.initialize(
          Map.of(VAULT_ADDRESS_PROP, VAULT.getHttpHostAddress(), VAULT_TOKEN_PROP, VAULT_TOKEN));

      ByteBuffer key = ByteBuffer.wrap("table-master-key".getBytes());

      ByteBuffer encryptedKey = client.wrapKey(key, ICEBERG_TEST_KEY_NAME);
      assertThat(encryptedKey).isNotEqualTo(key);

      ByteBuffer decryptedKey = client.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME);
      assertThat(decryptedKey).isEqualTo(key);
    }
  }

  @Test
  void appRoleAuthentication() throws Exception {
    try (KeyManagementClient client = new VaultKeyManagementClient()) {
      client.initialize(
          Map.of(
              VAULT_ADDRESS_PROP,
              VAULT.getHttpHostAddress(),
              VAULT_ROLE_ID_PROP,
              extractRoleId(),
              VAULT_SECRET_ID_PROP,
              extractSecretId()));
      ByteBuffer key = ByteBuffer.wrap("appRole-authenticated-key".getBytes());

      ByteBuffer encryptedKey = client.wrapKey(key, ICEBERG_TEST_KEY_NAME);
      assertThat(encryptedKey).isNotEqualTo(key);

      ByteBuffer decryptedKey = client.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME);
      assertThat(decryptedKey).isEqualTo(key);
    }
  }

  @Test
  void generateKey() {
    try (KeyManagementClient client = new VaultKeyManagementClient()) {
      client.initialize(
          Map.of(VAULT_ADDRESS_PROP, VAULT.getHttpHostAddress(), VAULT_TOKEN_PROP, VAULT_TOKEN));
      assertThat(client.supportsKeyGeneration()).isTrue();

      KeyManagementClient.KeyGenerationResult result = client.generateKey(ICEBERG_TEST_KEY_NAME);

      assertThat(result.key()).isNotNull();
      assertThat(result.wrappedKey()).isNotNull();
      assertThat(result.key().remaining()).isGreaterThan(0);
      assertThat(result.wrappedKey().remaining()).isGreaterThan(0);
    }
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerialization(
      TestHelpers.RoundTripSerializer<VaultKeyManagementClient> roundTripSerializer)
      throws Exception {
    try (VaultKeyManagementClient client = new VaultKeyManagementClient()) {
      client.initialize(
          Map.of(VAULT_ADDRESS_PROP, VAULT.getHttpHostAddress(), VAULT_TOKEN_PROP, VAULT_TOKEN));

      VaultKeyManagementClient result = roundTripSerializer.apply(client);

      ByteBuffer key = ByteBuffer.wrap("table-master-key".getBytes());

      ByteBuffer encryptedKey = result.wrapKey(key, ICEBERG_TEST_KEY_NAME);
      assertThat(encryptedKey).isNotEqualTo(key);

      ByteBuffer decryptedKey = result.unwrapKey(encryptedKey, ICEBERG_TEST_KEY_NAME);
      assertThat(decryptedKey).isEqualTo(key);
    }
  }

  private static String extractRoleId() throws IOException, InterruptedException {
    String roleIdResult =
        VAULT
            .execInContainer(
                "vault",
                "read",
                "-format=json",
                "auth/approle/role/" + APPROLE_ROLE_NAME + "/role-id")
            .getStdout();

    JsonNode roleIdNode = MAPPER.readTree(roleIdResult);
    String roleId = roleIdNode.get("data").get("role_id").asText();
    assertThat(roleId).isNotEmpty();
    return roleId;
  }

  private static String extractSecretId() throws IOException, InterruptedException {
    String secretIdResult =
        VAULT
            .execInContainer(
                "vault",
                "write",
                "-format=json",
                "-f",
                "auth/approle/role/" + APPROLE_ROLE_NAME + "/secret-id")
            .getStdout();

    JsonNode secretIdNode = MAPPER.readTree(secretIdResult);
    String secretId = secretIdNode.get("data").get("secret_id").asText();
    assertThat(secretId).isNotEmpty();
    return secretId;
  }
}
