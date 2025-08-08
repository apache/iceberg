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

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.UnwrapResult;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.util.ByteBuffers;

/** Azure key management client which connects to Azure Key Vault. */
public class AzureKeyManagementClient implements KeyManagementClient {
  private KeyClient keyClient;
  private KeyWrapAlgorithm keyWrapAlgorithm;

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    WrapResult wrapResult =
        keyClient
            .getCryptographyClient(wrappingKeyId)
            .wrapKey(keyWrapAlgorithm, ByteBuffers.toByteArray(key));
    return ByteBuffer.wrap(wrapResult.getEncryptedKey());
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    UnwrapResult unwrapResult =
        keyClient
            .getCryptographyClient(wrappingKeyId)
            .unwrapKey(keyWrapAlgorithm, ByteBuffers.toByteArray(wrappedKey));
    return ByteBuffer.wrap(unwrapResult.getKey());
  }

  @Override
  public void initialize(Map<String, String> properties) {
    AzureProperties azureProperties = new AzureProperties(properties);

    String vaultUrl = azureProperties.keyVaultUri();
    this.keyWrapAlgorithm = azureProperties.keyWrapAlgorithm();
    this.keyClient =
        new KeyClientBuilder()
            .vaultUrl(vaultUrl)
            .credential(new DefaultAzureCredentialBuilder().build())
            .buildClient();
  }
}
