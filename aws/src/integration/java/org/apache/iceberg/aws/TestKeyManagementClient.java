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
package org.apache.iceberg.aws;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariables;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.NullSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.CreateKeyRequest;
import software.amazon.awssdk.services.kms.model.CreateKeyResponse;
import software.amazon.awssdk.services.kms.model.DataKeySpec;
import software.amazon.awssdk.services.kms.model.KeySpec;
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionRequest;
import software.amazon.awssdk.services.kms.model.ScheduleKeyDeletionResponse;

@EnabledIfEnvironmentVariables({
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_ACCESS_KEY_ID, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SECRET_ACCESS_KEY, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_SESSION_TOKEN, matches = ".*"),
  @EnabledIfEnvironmentVariable(named = AwsIntegTestUtil.AWS_TEST_ACCOUNT_ID, matches = "\\d{12}")
})
public class TestKeyManagementClient {

  private static final Logger LOG = LoggerFactory.getLogger(TestKeyManagementClient.class);

  private static KmsClient kmsClient;
  private static String keyId;

  @BeforeAll
  public static void beforeClass() {
    kmsClient = AwsClientFactories.defaultFactory().kms();
    CreateKeyRequest createKeyRequest =
        CreateKeyRequest.builder()
            .keySpec(KeySpec.SYMMETRIC_DEFAULT)
            .description(
                "Iceberg integration test key for " + TestKeyManagementClient.class.getName())
            .build();
    CreateKeyResponse response = kmsClient.createKey(createKeyRequest);
    keyId = response.keyMetadata().keyId();
  }

  @AfterAll
  public static void afterClass() {
    // AWS KMS doesn't allow instant deletion. Keys can be put to pendingDeletion state instead,
    // with a minimum of 7 days until final removal.
    ScheduleKeyDeletionRequest deletionRequest =
        ScheduleKeyDeletionRequest.builder().keyId(keyId).pendingWindowInDays(7).build();

    ScheduleKeyDeletionResponse deletionResponse = kmsClient.scheduleKeyDeletion(deletionRequest);
    LOG.info(
        "Deletion of test key {} will be finalized at {}", keyId, deletionResponse.deletionDate());

    try {
      kmsClient.close();
    } catch (Exception e) {
      LOG.error("Error closing KMS client", e);
    }
  }

  @Test
  public void testKeyWrapping() {
    try (AwsKeyManagementClient keyManagementClient = new AwsKeyManagementClient()) {
      keyManagementClient.initialize(ImmutableMap.of());

      ByteBuffer key = ByteBuffer.wrap(new String("super-secret-table-master-key").getBytes());
      ByteBuffer encryptedKey = keyManagementClient.wrapKey(key, keyId);

      assertThat(keyManagementClient.unwrapKey(encryptedKey, keyId)).isEqualTo(key);
    }
  }

  @ParameterizedTest
  @NullSource
  @EnumSource(
      value = DataKeySpec.class,
      names = {"AES_128", "AES_256"})
  public void testKeyGeneration(DataKeySpec dataKeySpec) {
    try (AwsKeyManagementClient keyManagementClient = new AwsKeyManagementClient()) {
      Map<String, String> properties =
          dataKeySpec == null
              ? ImmutableMap.of()
              : ImmutableMap.of(AwsProperties.KMS_DATA_KEY_SPEC, dataKeySpec.name());
      keyManagementClient.initialize(properties);
      KeyManagementClient.KeyGenerationResult result = keyManagementClient.generateKey(keyId);

      assertThat(keyManagementClient.unwrapKey(result.wrappedKey(), keyId)).isEqualTo(result.key());
      assertThat(result.key().limit()).isEqualTo(expectedLength(dataKeySpec));
    }
  }

  private static int expectedLength(DataKeySpec spec) {
    if (DataKeySpec.AES_128.equals(spec)) {
      return 128 / 8;
    } else {
      return 256 / 8;
    }
  }
}
