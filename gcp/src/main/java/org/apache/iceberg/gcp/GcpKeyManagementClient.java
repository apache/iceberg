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
package org.apache.iceberg.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.kms.v1.DecryptRequest;
import com.google.cloud.kms.v1.DecryptResponse;
import com.google.cloud.kms.v1.EncryptRequest;
import com.google.cloud.kms.v1.EncryptResponse;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyManagementServiceSettings;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.io.CloseableGroup;

public class GcpKeyManagementClient implements KeyManagementClient {
  private KeyManagementServiceClient kmsClient;
  private CloseableGroup closeableGroup = new CloseableGroup();

  @Override
  public void initialize(Map<String, String> properties) {
    this.closeableGroup = new CloseableGroup();
    closeableGroup.setSuppressCloseFailure(true);

    GCPProperties gcpProperties = new GCPProperties(properties);

    try {
      KeyManagementServiceSettings.Builder kmsBuilder = KeyManagementServiceSettings.newBuilder();
      if (gcpProperties.oauth2Token().isPresent()) {
        OAuth2Credentials oAuth2Credentials =
            GCPAuthUtils.oauth2CredentialsFromGcpProperties(gcpProperties, closeableGroup);
        kmsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(oAuth2Credentials));
      }
      // if not OAuth then defaults to GoogleCredentials.getApplicationDefault()
      this.kmsClient = KeyManagementServiceClient.create(kmsBuilder.build());
      closeableGroup.addCloseable(kmsClient);

    } catch (IOException e) {
      throw new RuntimeException("Failed to create GCP cloud KMS service client", e);
    }
  }

  @Override
  public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
    EncryptRequest encryptRequest =
        EncryptRequest.newBuilder()
            .setPlaintext(ByteString.copyFrom(key))
            .setName(wrappingKeyId)
            .build();
    EncryptResponse encryptResponse = kmsClient.encrypt(encryptRequest);
    // need ByteString.copyFrom leaves the BB in an end position, need to reset
    key.position(0);
    return ByteBuffer.wrap(encryptResponse.getCiphertext().toByteArray());
  }

  @Override
  public ByteBuffer unwrapKey(ByteBuffer wrappedKey, String wrappingKeyId) {
    DecryptRequest decryptRequest =
        DecryptRequest.newBuilder()
            .setCiphertext(ByteString.copyFrom(wrappedKey))
            .setName(wrappingKeyId)
            .build();
    DecryptResponse decryptResponse = kmsClient.decrypt(decryptRequest);
    // need ByteString.copyFrom leaves the BB in an end position, need to reset
    wrappedKey.position(0);
    return ByteBuffer.wrap(decryptResponse.getPlaintext().toByteArray());
  }

  @Override
  public void close() {
    try {
      closeableGroup.close();
    } catch (IOException ioe) {
      // closure exceptions already suppressed and logged in closeableGroup
    }
  }
}
