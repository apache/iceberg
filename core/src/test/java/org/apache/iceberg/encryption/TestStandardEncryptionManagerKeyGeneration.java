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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

class TestStandardEncryptionManagerKeyGeneration {

  private static final String MASTER_KEY = UnitestKMS.MASTER_KEY_NAME1;

  @Test
  @SuppressWarnings("deprecation")
  void keyWrappedWithDeprecatedConstructor() {
    TrackingKMS kms = new TrackingKMS(true);
    StandardEncryptionManager manager =
        new StandardEncryptionManager(List.of(), MASTER_KEY, 16, kms);

    manager.keyEncryptionKeyID();

    assertThat(kms.wrapCalls).isEqualTo(1);
    assertThat(kms.generateCalls).isEqualTo(0);
  }

  @Test
  void keyWrappedWhenGenerationDisabled() {
    TrackingKMS kms = new TrackingKMS(true);
    StandardEncryptionManager manager =
        new StandardEncryptionManager(List.of(), MASTER_KEY, 16, kms, false);

    manager.keyEncryptionKeyID();

    assertThat(kms.wrapCalls).isEqualTo(1);
    assertThat(kms.generateCalls).isEqualTo(0);
  }

  @Test
  void keyGeneratedInKmsWhenEnabled() {
    TrackingKMS kms = new TrackingKMS(true);
    StandardEncryptionManager manager =
        new StandardEncryptionManager(List.of(), MASTER_KEY, 16, kms, true);

    String keyId = manager.keyEncryptionKeyID();

    assertThat(kms.generateCalls).isEqualTo(1);
    assertThat(kms.wrapCalls).isEqualTo(0);
    assertThat(manager.encryptionKeys()).containsKey(keyId);

    ByteBuffer unwrapped =
        kms.unwrapKey(manager.encryptionKeys().get(keyId).encryptedKeyMetadata(), MASTER_KEY);
    assertThat(unwrapped).isEqualTo(ByteBuffer.wrap(kms.generatedKey));
  }

  @Test
  void failsWhenEnabledButClientDoesNotSupportGeneration() {
    TrackingKMS kms = new TrackingKMS(false);
    StandardEncryptionManager manager =
        new StandardEncryptionManager(List.of(), MASTER_KEY, 16, kms, true);

    assertThatThrownBy(manager::keyEncryptionKeyID)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot generate key encryption key in KMS: encryption.kms-key-generation-enabled "
                + "is enabled, but "
                + kms.getClass().getName()
                + " does not support key generation");

    assertThat(kms.wrapCalls).isEqualTo(0);
    assertThat(kms.generateCalls).isEqualTo(0);
  }

  @Test
  void existingKeyReusedAfterTogglingProperty() {
    TrackingKMS kms = new TrackingKMS(true);
    StandardEncryptionManager enabled =
        new StandardEncryptionManager(List.of(), MASTER_KEY, 16, kms, true);
    String keyId = enabled.keyEncryptionKeyID();
    List<EncryptedKey> keys = List.copyOf(enabled.encryptionKeys().values());

    StandardEncryptionManager disabled =
        new StandardEncryptionManager(keys, MASTER_KEY, 16, kms, false);

    assertThat(disabled.keyEncryptionKeyID()).isEqualTo(keyId);
    assertThat(kms.generateCalls).isEqualTo(1);
    assertThat(kms.wrapCalls).isEqualTo(0);
  }

  @Test
  void tablePropertyEnablesKmsKeyGeneration() {
    TrackingKMS kms = new TrackingKMS(true);
    Map<String, String> tableProperties =
        ImmutableMap.of(
            TableProperties.ENCRYPTION_TABLE_KEY,
            MASTER_KEY,
            TableProperties.ENCRYPTION_KMS_KEY_GENERATION_ENABLED,
            "true");

    EncryptionManager manager =
        EncryptionUtil.createEncryptionManager(List.of(), tableProperties, kms);
    EncryptionTestHelpers.keyEncryptionKeyID(manager);

    assertThat(kms.generateCalls).isEqualTo(1);
    assertThat(kms.wrapCalls).isEqualTo(0);
  }

  @Test
  void tablePropertyDefaultsToWrap() {
    TrackingKMS kms = new TrackingKMS(true);
    Map<String, String> tableProperties =
        ImmutableMap.of(TableProperties.ENCRYPTION_TABLE_KEY, MASTER_KEY);

    EncryptionManager manager =
        EncryptionUtil.createEncryptionManager(List.of(), tableProperties, kms);
    EncryptionTestHelpers.keyEncryptionKeyID(manager);

    assertThat(kms.wrapCalls).isEqualTo(1);
    assertThat(kms.generateCalls).isEqualTo(0);
  }

  @Test
  void propertyRejectedBeforeV3() {
    Map<String, String> tableProperties =
        ImmutableMap.of(TableProperties.ENCRYPTION_KMS_KEY_GENERATION_ENABLED, "true");

    assertThatThrownBy(() -> EncryptionUtil.checkCompatibility(tableProperties, 2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties for v2: [encryption.kms-key-generation-enabled]");
  }

  /** A mock KMS that records which key creation path was used. */
  private static class TrackingKMS extends UnitestKMS {
    private final boolean supportsGeneration;
    private int wrapCalls = 0;
    private int generateCalls = 0;
    private byte[] generatedKey;

    TrackingKMS(boolean supportsGeneration) {
      this.supportsGeneration = supportsGeneration;
      initialize(ImmutableMap.of());
    }

    @Override
    public ByteBuffer wrapKey(ByteBuffer key, String wrappingKeyId) {
      this.wrapCalls += 1;
      return super.wrapKey(key, wrappingKeyId);
    }

    @Override
    public boolean supportsKeyGeneration() {
      return supportsGeneration;
    }

    @Override
    public KeyGenerationResult generateKey(String wrappingKeyId) {
      this.generateCalls += 1;
      byte[] key = new byte[16];
      new SecureRandom().nextBytes(key);
      this.generatedKey = key;
      return new KeyGenerationResult(
          ByteBuffer.wrap(key), super.wrapKey(ByteBuffer.wrap(key), wrappingKeyId));
    }
  }
}
