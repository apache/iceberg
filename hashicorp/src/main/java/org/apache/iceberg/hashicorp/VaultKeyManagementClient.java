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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.util.SerializableMap;

/**
 * KMS client implementation using HashiCorp Vault Transit secrets engine with AppRole
 * authentication.
 */
public class VaultKeyManagementClient implements KeyManagementClient, Closeable {
  @SuppressWarnings("unused")
  private SerializableMap<String, String> properties;

  private String vaultAddress;
  private String transitMount;
  private String appRolePath;
  private String appRoleId;
  private String appSecretId;
  private boolean rotateToken;

  private transient volatile String vaultToken;
  private transient volatile long tokenExpiry;
  private transient volatile VaultClient client;

  @Override
  public void initialize(Map<String, String> newProperties) {
    this.properties = SerializableMap.copyOf(newProperties);

    vaultAddress = newProperties.get(VaultProperties.VAULT_ADDRESS_PROP);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(vaultAddress),
        "%s must be set in newProperties",
        VaultProperties.VAULT_ADDRESS_PROP);

    transitMount = newProperties.getOrDefault(VaultProperties.VAULT_TRANSIT_MOUNT_PROP, "transit");
    appRolePath = newProperties.getOrDefault(VaultProperties.VAULT_APPROLE_PATH_PROP, "approle");
    rotateToken =
        Boolean.parseBoolean(
            newProperties.getOrDefault(VaultProperties.VAULT_ROTATE_TOKEN_PROP, "false"));

    String configuredVaultToken = newProperties.get(VaultProperties.VAULT_TOKEN_PROP);
    appRoleId = newProperties.get(VaultProperties.VAULT_ROLE_ID_PROP);
    appSecretId =
        newProperties.getOrDefault(
            VaultProperties.VAULT_SECRET_ID_PROP,
            System.getenv(VaultProperties.VAULT_SECRET_ID_ENV_VAR));

    boolean hasTokenAuth = !Strings.isNullOrEmpty(configuredVaultToken);
    boolean hasAppRoleAuth =
        !Strings.isNullOrEmpty(appRoleId) && !Strings.isNullOrEmpty(appSecretId);

    Preconditions.checkArgument(
        hasTokenAuth || hasAppRoleAuth,
        "Either token or both role id and secret id must be set in newProperties");

    // Initialize vaultToken for direct token auth (don't mark as transient field)
    if (hasTokenAuth) {
      vaultToken = configuredVaultToken;
    }
  }

  private void authenticate() {
    if (Strings.isNullOrEmpty(vaultToken)) {
      authenticateWithAppRole();
    }
  }

  private void authenticateWithAppRole() {
    VaultClient.AuthResult result = client().authenticate(appRoleId, appSecretId);
    vaultToken = result.clientToken();

    if (rotateToken) {
      long leaseDuration = result.leaseDuration().orElseThrow();
      tokenExpiry = System.currentTimeMillis() + (leaseDuration * 1000);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    ensureValidToken();

    byte[] keyBytes = new byte[key.remaining()];
    key.duplicate().get(keyBytes);
    String plaintext = Base64.getEncoder().encodeToString(keyBytes);

    String ciphertext = client().encrypt(vaultToken, wrappingKeyId, plaintext);
    return ByteBuffer.wrap(ciphertext.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    ensureValidToken();

    byte[] ciphertextBytes = new byte[wrappedKey.remaining()];
    wrappedKey.duplicate().get(ciphertextBytes);
    String ciphertext = new String(ciphertextBytes, StandardCharsets.UTF_8);

    String plaintext = client().decrypt(vaultToken, wrappingKeyId, ciphertext);
    byte[] keyBytes = Base64.getDecoder().decode(plaintext);
    return ByteBuffer.wrap(keyBytes);
  }

  @Override
  public boolean supportsKeyGeneration() {
    return true;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    ensureValidToken();

    VaultClient.DataKey dataKey = client().generateKey(vaultToken, wrappingKeyId);

    byte[] keyBytes = Base64.getDecoder().decode(dataKey.plaintext());
    ByteBuffer key = ByteBuffer.wrap(keyBytes);
    ByteBuffer wrappedKey = ByteBuffer.wrap(dataKey.ciphertext().getBytes(StandardCharsets.UTF_8));

    return new KeyGenerationResult(key, wrappedKey);
  }

  private void ensureValidToken() {
    if (vaultToken == null) {
      synchronized (this) {
        if (vaultToken == null) {
          // After deserialization, restore token from properties if using token auth
          String configuredToken = properties.get(VaultProperties.VAULT_TOKEN_PROP);
          if (!Strings.isNullOrEmpty(configuredToken)) {
            vaultToken = configuredToken;
          } else {
            authenticate();
          }
        }
      }
    }

    if (!rotateToken) {
      return;
    }

    if (System.currentTimeMillis() > (tokenExpiry - 300_000)) {
      synchronized (this) {
        if (System.currentTimeMillis() > (tokenExpiry - 300_000)) {
          authenticateWithAppRole();
        }
      }
    }
  }

  private VaultClient client() {
    if (client == null) {
      synchronized (this) {
        if (client == null) {
          client = new VaultClient(vaultAddress, transitMount, appRolePath);
        }
      }
    }
    return client;
  }

  @Override
  public void close() {
    if (client != null) {
      client.close();
    }
  }
}
