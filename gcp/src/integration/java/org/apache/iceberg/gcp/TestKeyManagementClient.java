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

import static org.assertj.core.api.Assertions.assertThat;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.kms.v1.CryptoKey;
import com.google.cloud.kms.v1.CryptoKeyName;
import com.google.cloud.kms.v1.CryptoKeyVersion;
import com.google.cloud.kms.v1.CryptoKeyVersionName;
import com.google.cloud.kms.v1.CryptoKeyVersionTemplate;
import com.google.cloud.kms.v1.KeyManagementServiceClient;
import com.google.cloud.kms.v1.KeyRing;
import com.google.cloud.kms.v1.KeyRingName;
import com.google.cloud.kms.v1.LocationName;
import com.google.cloud.kms.v1.ProtectionLevel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public abstract class TestKeyManagementClient {

  private static final String LOCATION = "global";
  private static final String KEY_RING_ID = "iceberg-gcp-it-keyring";
  private static final String KEY_VERSION = "1";
  private final String keyId = "iceberg-gcp-it-key-" + UUID.randomUUID();

  private KeyManagementServiceClient kmsClient;
  private String projectId;

  protected abstract void init() throws IOException;

  protected abstract Map<String, String> properties();

  public void setKmsClient(KeyManagementServiceClient kmsClient) {
    this.kmsClient = kmsClient;
  }

  public void setProjectId(String projectId) {
    this.projectId = projectId;
  }

  @BeforeEach
  public void before() throws IOException {
    init();

    LocationName locationName = LocationName.of(projectId, LOCATION);
    try {
      kmsClient.createKeyRing(locationName, KEY_RING_ID, KeyRing.newBuilder().build());
    } catch (AlreadyExistsException e) {
      // we're going to reuse - in GCP key rings are not removable anyway
    }

    CryptoKeyVersionTemplate keyVersionTemplate =
        CryptoKeyVersionTemplate.newBuilder()
            .setProtectionLevel(ProtectionLevel.SOFTWARE)
            .setAlgorithm(CryptoKeyVersion.CryptoKeyVersionAlgorithm.GOOGLE_SYMMETRIC_ENCRYPTION)
            .build();

    // API only allows 24 hr of key retention after a key destruction request
    CryptoKey cryptoKey =
        CryptoKey.newBuilder()
            .setPurpose(CryptoKey.CryptoKeyPurpose.ENCRYPT_DECRYPT)
            .setVersionTemplate(keyVersionTemplate)
            .build();

    kmsClient.createCryptoKey(KeyRingName.of(projectId, LOCATION, KEY_RING_ID), keyId, cryptoKey);
  }

  @AfterEach
  public void after() {
    CryptoKeyVersionName cryptoKeyVersionName =
        CryptoKeyVersionName.of(projectId, LOCATION, KEY_RING_ID, keyId, KEY_VERSION);
    kmsClient.destroyCryptoKeyVersion(cryptoKeyVersionName);

    // key rings are not removable by design in GCP

    kmsClient.close();
  }

  @Test
  public void testKeyWrapping() {
    String keyname = CryptoKeyName.of(projectId, LOCATION, KEY_RING_ID, keyId).toString();

    try (GcpKeyManagementClient keyManagementClient = new GcpKeyManagementClient(); ) {
      keyManagementClient.initialize(properties());

      ByteBuffer key = ByteBuffer.wrap(new String("super-secret-table-master-key").getBytes());
      ByteBuffer encryptedKey = keyManagementClient.wrapKey(key, keyname);

      assertThat(keyManagementClient.unwrapKey(encryptedKey, keyname)).isEqualTo(key);
    }
  }
}
