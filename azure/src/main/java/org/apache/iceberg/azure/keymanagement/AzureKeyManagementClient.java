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

import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.UnwrapResult;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.azure.AdlsTokenCredentialProviders;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.SerializableMap;

/** Azure key management client which connects to Azure Key Vault. */
public class AzureKeyManagementClient implements KeyManagementClient {

  private Map<String, String> allProperties;

  private transient volatile ClientState state;

  @Override
  public void initialize(Map<String, String> properties) {
    this.allProperties = SerializableMap.copyOf(properties);
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    WrapResult wrapResult =
        keyClient()
            .getCryptographyClient(wrappingKeyId)
            .wrapKey(keyWrapAlgorithm(), ByteBuffers.toByteArray(key));
    return ByteBuffer.wrap(wrapResult.getEncryptedKey());
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    UnwrapResult unwrapResult =
        keyClient()
            .getCryptographyClient(wrappingKeyId)
            .unwrapKey(keyWrapAlgorithm(), ByteBuffers.toByteArray(wrappedKey));
    return ByteBuffer.wrap(unwrapResult.getKey());
  }

  private KeyClient keyClient() {
    return state().keyClient;
  }

  private KeyWrapAlgorithm keyWrapAlgorithm() {
    return state().keyWrapAlgorithm;
  }

  private ClientState state() {
    if (state == null) {
      synchronized (this) {
        if (state == null) {
          AzureProperties azureProperties = new AzureProperties(allProperties);
          KeyClientBuilder keyClientBuilder = new KeyClientBuilder();
          azureProperties.keyVaultUrl().ifPresent(keyClientBuilder::vaultUrl);
          KeyClient keyClient =
              keyClientBuilder
                  .credential(AdlsTokenCredentialProviders.from(allProperties).credential())
                  .buildClient();
          KeyWrapAlgorithm keyWrapAlgorithm = azureProperties.keyWrapAlgorithm();
          state = new ClientState(keyClient, keyWrapAlgorithm);
        }
      }
    }
    return state;
  }

  private static final class ClientState {

    KeyClient keyClient;
    KeyWrapAlgorithm keyWrapAlgorithm;

    ClientState(KeyClient keyClient, KeyWrapAlgorithm keyWrapAlgorithm) {
      this.keyClient = keyClient;
      this.keyWrapAlgorithm = keyWrapAlgorithm;
    }
  }
}
