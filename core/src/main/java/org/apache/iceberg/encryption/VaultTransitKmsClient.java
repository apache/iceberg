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
package org.apache.iceberg.encryption;

import com.bettercloud.vault.SslConfig;
import com.bettercloud.vault.Vault;
import com.bettercloud.vault.VaultConfig;
import com.bettercloud.vault.VaultException;
import com.bettercloud.vault.response.AuthResponse;
import com.bettercloud.vault.response.LogicalResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

/**
 * KMS client implementation using HashiCorp Vault Transit secrets engine with AppRole
 * authentication.
 *
 * <p>Required properties:
 * <ul>
 *   <li>vault.addr: Vault server address (e.g., https://vault.example.com:8200)</li>
 *   <li>vault.transit.mount: Transit secrets engine mount path (default: transit)</li>
 *   <li>vault.approle.path: AppRole authentication path (default: approle)</li>
 * </ul>
 *
 * <p>Required environment variables:
 * <ul>
 *   <li>VAULT_ROLE_ID: AppRole role ID</li>
 *   <li>VAULT_SECRET_ID: AppRole secret ID</li>
 * </ul>
 *
 * <p>Optional environment variables:
 * <ul>
 *   <li>VAULT_ENABLE_TOKEN_ROTATION: Enable automatic token rotation (true/false, default: false)</li>
 *   <li>VAULT_SSL_CERT: Used to verify connection to vault instance. When enabled, tokens are
 *       proactively refreshed 5 minutes before expiry. When disabled, the Vault library handles
 *       token expiry automatically on-demand.</li>
 * </ul>
 */
public class VaultTransitKmsClient implements KeyManagementClient {

  // Vault server address
  public static final String VAULT_ADDR_PROP = "vault.addr";

  // Transit secrets engine mount path (default: "transit")
  public static final String VAULT_TRANSIT_MOUNT_PROP = "vault.transit.mount";
  public static final String DEFAULT_TRANSIT_MOUNT = "transit";

  // AppRole authentication path (default: "approle")
  public static final String VAULT_APPROLE_PATH_PROP = "vault.approle.path";
  public static final String DEFAULT_APPROLE_PATH = "approle";

  // Environment variables for AppRole credentials
  public static final String VAULT_ROLE_ID_ENV_VAR = "VAULT_ROLE_ID";
  public static final String VAULT_SECRET_ID_ENV_VAR = "VAULT_SECRET_ID";
  public static final String VAULT_ENABLE_TOKEN_ROTATION_ENV_VAR = "VAULT_ENABLE_TOKEN_ROTATION";

  private transient Vault vault;
  private String vaultAddr;
  private String transitMount;
  private String appRolePath;
  private boolean enableTokenRotation;
  private volatile String token;
  private volatile long tokenExpiry;

  @Override
  public void initialize(Map<String, String> properties) {
    vaultAddr = properties.get(VAULT_ADDR_PROP);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(vaultAddr), VAULT_ADDR_PROP + " must be set in properties");

    transitMount = properties.getOrDefault(VAULT_TRANSIT_MOUNT_PROP, DEFAULT_TRANSIT_MOUNT);
    appRolePath = properties.getOrDefault(VAULT_APPROLE_PATH_PROP, DEFAULT_APPROLE_PATH);

    // Check if token rotation is enabled (default: false)
    String enableRotation = System.getenv(VAULT_ENABLE_TOKEN_ROTATION_ENV_VAR);
    enableTokenRotation = "true".equalsIgnoreCase(enableRotation);

    String roleId = System.getenv(VAULT_ROLE_ID_ENV_VAR);
    Preconditions.checkNotNull(roleId, VAULT_ROLE_ID_ENV_VAR + " environment variable must be set");

    String secretId = System.getenv(VAULT_SECRET_ID_ENV_VAR);
    Preconditions.checkNotNull(
        secretId, VAULT_SECRET_ID_ENV_VAR + " environment variable must be set");

    try {
      // Authenticate using AppRole
      authenticate(roleId, secretId, appRolePath);

    } catch (RuntimeException e) {
      throw new RuntimeException("Failed to initialize Vault client", e);
    }
  }

  private void authenticate(String roleId, String secretId, String appRolePath) {
    try {
      VaultConfig config =
          new VaultConfig()
              .address(vaultAddr)
              .engineVersion(1)
              .sslConfig(new SslConfig().build())
              .build();
      Vault tempVault = new Vault(config);

      // Authenticate using AppRole
      AuthResponse response = tempVault.auth().loginByAppRole(appRolePath, roleId, secretId);

      token = response.getAuthClientToken();

      // Store token expiry if rotation is enabled
      if (enableTokenRotation) {
        tokenExpiry = System.currentTimeMillis() + (response.getAuthLeaseDuration() * 1000);
      }

      VaultConfig authenticatedConfig =
          new VaultConfig()
              .address(vaultAddr)
              .token(token)
              .engineVersion(1)
              .sslConfig(new SslConfig().build())
              .build();
      vault = new Vault(authenticatedConfig);

    } catch (VaultException e) {
      throw new RuntimeException("Failed to authenticate with Vault using AppRole", e);
    }
  }

  private void ensureValidToken() {
    // Only refresh token if rotation is enabled
    if (!enableTokenRotation) {
      return;
    }

    // Refresh token if it expires in less than 5 minutes
    if (System.currentTimeMillis() > (tokenExpiry - 300000)) {
      String roleId = System.getenv(VAULT_ROLE_ID_ENV_VAR);
      String secretId = System.getenv(VAULT_SECRET_ID_ENV_VAR);
      authenticate(roleId, secretId, appRolePath);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    ensureValidToken();

    try {
      // Encode key as base64
      byte[] keyBytes = new byte[key.remaining()];
      key.duplicate().get(keyBytes);
      String plaintext = Base64.getEncoder().encodeToString(keyBytes);

      // Encrypt using Transit
      LogicalResponse response =
          vault
              .logical()
              .write(transitMount + "/encrypt/" + wrappingKeyId, Map.of("plaintext", plaintext));

      String ciphertext = response.getData().get("ciphertext");
      if (ciphertext == null) {
        throw new RuntimeException("Failed to wrap key: no ciphertext returned");
      }

      // Return the ciphertext as bytes
      return ByteBuffer.wrap(ciphertext.getBytes(StandardCharsets.UTF_8));

    } catch (VaultException e) {
      throw new RuntimeException("Failed to wrap key with wrapping key " + wrappingKeyId, e);
    }
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    ensureValidToken();

    try {
      byte[] ciphertextBytes = new byte[wrappedKey.remaining()];
      wrappedKey.duplicate().get(ciphertextBytes);
      String ciphertext = new String(ciphertextBytes, StandardCharsets.UTF_8);

      // Decrypt using Transit
      LogicalResponse response =
          vault
              .logical()
              .write(transitMount + "/decrypt/" + wrappingKeyId, Map.of("ciphertext", ciphertext));

      String plaintext = response.getData().get("plaintext");
      if (plaintext == null) {
        throw new RuntimeException("Failed to unwrap key: no plaintext returned");
      }

      byte[] keyBytes = Base64.getDecoder().decode(plaintext);
      return ByteBuffer.wrap(keyBytes);

    } catch (VaultException e) {
      throw new RuntimeException("Failed to unwrap key with wrapping key " + wrappingKeyId, e);
    }
  }

  @Override
  public boolean supportsKeyGeneration() {
    return true;
  }

  @Override
  public KeyGenerationResult generateKey(String wrappingKeyId) {
    ensureValidToken();

    try {
      // Generate a data encryption key using Transit
      LogicalResponse response =
          vault.logical().write(transitMount + "/datakey/plaintext/" + wrappingKeyId, Map.of());

      String plaintext = response.getData().get("plaintext");
      String ciphertext = response.getData().get("ciphertext");

      if (plaintext == null || ciphertext == null) {
        throw new RuntimeException("Failed to generate key: missing plaintext or ciphertext");
      }

      byte[] keyBytes = Base64.getDecoder().decode(plaintext);
      ByteBuffer key = ByteBuffer.wrap(keyBytes);

      ByteBuffer wrappedKey = ByteBuffer.wrap(ciphertext.getBytes(StandardCharsets.UTF_8));

      return new KeyGenerationResult(key, wrappedKey);

    } catch (VaultException e) {
      throw new RuntimeException("Failed to generate key with wrapping key " + wrappingKeyId, e);
    }
  }
}
