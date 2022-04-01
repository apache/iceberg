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

package org.apache.iceberg.azure;

import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_ACCOUNT_KEY;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_AUTH_TYPE;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_ENDPOINT;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_READ_BLOCK_SIZE;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_SHARED_ACCESS_SIGNATURE;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_WRITE_BLOCK_SIZE;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_WRITE_MAX_CONCURRENCY;
import static org.apache.iceberg.azure.AzureProperties.STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestAzureProperties {

  public static final Integer STORAGE_READ_BLOCK_SIZE_DEFAULT = 8 * 1024 * 1024;
  public static final Long STORAGE_WRITE_BLOCK_SIZE_DEFAULT = 4L * 1024 * 1024;
  public static final Integer STORAGE_WRITE_MAX_CONCURRENCY_DEFAULT = 8;
  public static final Long STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT = 4L * 1024 * 1024;

  private Map<String, String> properties;

  @Before
  public void setupProperties() {
    this.properties = Maps.newHashMap();
  }

  @Test
  public void testDefaultAzureProperties() {
    final String storageAccount = "test-storage-account";
    AzureProperties azureProperties = new AzureProperties(ImmutableMap.of());

    assertThat(azureProperties.authType(storageAccount)).isEqualTo(AuthType.None);
    assertThat(azureProperties.connectionString(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.endpoint(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.accountKey(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.sharedAccessSignature(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.readBlockSize(storageAccount)).isEqualTo(STORAGE_READ_BLOCK_SIZE_DEFAULT);
    assertThat(azureProperties.writeBlockSize(storageAccount)).isEqualTo(STORAGE_WRITE_BLOCK_SIZE_DEFAULT);
    assertThat(azureProperties.maxWriteConcurrency(storageAccount)).isEqualTo(STORAGE_WRITE_MAX_CONCURRENCY_DEFAULT);
    assertThat(azureProperties.maxSingleUploadSize(storageAccount)).isEqualTo(
        STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE_DEFAULT);
  }

  @Test
  public void testSingleStorageAccountProperties() {
    final String storageAccount = "dev-storage-account";
    final Random random = AzureTestUtils.random("testSingleStorageAccountProperties");

    final AuthType authType = AuthType.SharedKey;
    properties.put(format(STORAGE_AUTH_TYPE, storageAccount), authType.toString());
    final String storageAccountKey = "rand-key-123345-qwertyuiop";
    properties.put(format(STORAGE_ACCOUNT_KEY, storageAccount), storageAccountKey);

    final Integer readBlockSize = Math.abs(random.nextInt());
    properties.put(format(STORAGE_READ_BLOCK_SIZE, storageAccount), String.valueOf(readBlockSize));

    final Long writeBlockSize = Math.abs(random.nextLong());
    properties.put(format(STORAGE_WRITE_BLOCK_SIZE, storageAccount), String.valueOf(writeBlockSize));

    final Integer maxWriteConcurrency = Math.abs(random.nextInt());
    properties.put(format(STORAGE_WRITE_MAX_CONCURRENCY, storageAccount), String.valueOf(maxWriteConcurrency));

    final Long maxSingleUploadSize = Math.abs(random.nextLong());
    properties.put(format(STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE, storageAccount), String.valueOf(maxSingleUploadSize));

    final AzureProperties azureProperties = new AzureProperties(properties);
    assertThat(azureProperties.authType(storageAccount)).isEqualTo(authType);
    assertThat(azureProperties.accountKey(storageAccount)).isEqualTo(Optional.of(storageAccountKey));
    assertThat(azureProperties.readBlockSize(storageAccount)).isEqualTo(readBlockSize);
    assertThat(azureProperties.writeBlockSize(storageAccount)).isEqualTo(writeBlockSize);
    assertThat(azureProperties.maxWriteConcurrency(storageAccount)).isEqualTo(maxWriteConcurrency);
    assertThat(azureProperties.maxSingleUploadSize(storageAccount)).isEqualTo(maxSingleUploadSize);

    // Should be empty since the auth type is SharedKey
    assertThat(azureProperties.connectionString(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.endpoint(storageAccount)).isEqualTo(Optional.empty());
    assertThat(azureProperties.sharedAccessSignature(storageAccount)).isEqualTo(Optional.empty());
  }

  @Test
  public void testMultipleStorageAccountProperties() {
    final Random random = AzureTestUtils.random("testMultipleStorageAccountProperties");
    final String[] storageAccounts = {"sa-1", "sa-2", "sa-3"};

    // Storage account 1 auth config (SharedKey).
    final String sa1 = storageAccounts[0];
    final AuthType sa1AuthType = AuthType.SharedKey;
    properties.put(format(STORAGE_AUTH_TYPE, sa1), sa1AuthType.toString());
    final String sa1AccountKey = "rand-key-0987654321-mnbvcxz";
    properties.put(format(STORAGE_ACCOUNT_KEY, sa1), sa1AccountKey);

    // Storage account 2 auth config (SharedAccessSignature).
    final String sa2 = storageAccounts[1];
    final AuthType sa2AuthType = AuthType.SharedAccessSignature;
    properties.put(format(STORAGE_AUTH_TYPE, sa2), sa2AuthType.toString());
    final String sa2SharedAccessSignature = "rand-signature-0987654321-mnbvcxz-weriouweiorufnsdkjf";
    properties.put(format(STORAGE_SHARED_ACCESS_SIGNATURE, sa2), sa2SharedAccessSignature);

    // Storage account 3 auth config (None).
    final String sa3 = storageAccounts[2];
    final AuthType sa3AuthType = AuthType.None;
    final String sa3Endpoint = "https://test-endpoint";
    properties.put(format(STORAGE_AUTH_TYPE, sa3), sa3AuthType.toString());
    properties.put(format(STORAGE_ENDPOINT, sa3), sa3Endpoint);

    // All Storage accounts read block size config.
    for (String storageAccount : storageAccounts) {
      final Integer readBlockSize = Math.abs(random.nextInt());
      properties.put(format(STORAGE_READ_BLOCK_SIZE, storageAccount), String.valueOf(readBlockSize));
    }

    // All Storage accounts write block size config.
    for (String storageAccount : storageAccounts) {
      final Long writeBlockSize = Math.abs(random.nextLong());
      properties.put(format(STORAGE_WRITE_BLOCK_SIZE, storageAccount), String.valueOf(writeBlockSize));
    }

    for (String storageAccount : storageAccounts) {
      // All Storage accounts max write concurrency config.
      final Integer maxWriteConcurrency = Math.abs(random.nextInt());
      properties.put(format(STORAGE_WRITE_MAX_CONCURRENCY, storageAccount), String.valueOf(maxWriteConcurrency));
    }

    // All Storage accounts max single upload size config.
    for (String storageAccount : storageAccounts) {
      final Long sa1maxSingleUploadSize = Math.abs(random.nextLong());
      properties.put(
          format(STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE, storageAccount),
          String.valueOf(sa1maxSingleUploadSize));
    }

    // Create AzureProperties
    final AzureProperties azureProperties = new AzureProperties(properties);

    // Assert auth config for storage account 1.
    assertThat(azureProperties.authType(sa1)).isEqualTo(sa1AuthType);
    assertThat(azureProperties.accountKey(sa1)).isEqualTo(Optional.of(sa1AccountKey));

    // Assert auth config for storage account 2.
    assertThat(azureProperties.authType(sa2)).isEqualTo(sa2AuthType);
    assertThat(azureProperties.sharedAccessSignature(sa2)).isEqualTo(Optional.of(sa2SharedAccessSignature));

    // Assert auth config for storage account 3.
    assertThat(azureProperties.authType(sa3)).isEqualTo(sa3AuthType);
    assertThat(azureProperties.accountKey(sa3)).isEqualTo(Optional.empty());
    assertThat(azureProperties.sharedAccessSignature(sa3)).isEqualTo(Optional.empty());
    assertThat(azureProperties.endpoint(sa3)).isEqualTo(Optional.of(sa3Endpoint));

    // Assert read block size config for all storage accounts.
    for (String storageAccount : storageAccounts) {
      final String readBlockSizeStr = properties.get(format(STORAGE_READ_BLOCK_SIZE, storageAccount));
      final Integer expectedReadBlockSize = Integer.parseInt(readBlockSizeStr);
      assertThat(azureProperties.readBlockSize(storageAccount)).isEqualTo(expectedReadBlockSize);
    }

    // Assert write block size config for all storage accounts.
    for (String storageAccount : storageAccounts) {
      final String writeBlockSizeStr = properties.get(format(STORAGE_WRITE_BLOCK_SIZE, storageAccount));
      final Long expectedWriteBlockSize = Long.parseLong(writeBlockSizeStr);
      assertThat(azureProperties.writeBlockSize(storageAccount)).isEqualTo(expectedWriteBlockSize);
    }

    // Assert max write concurrency size config for all storage accounts.
    for (String storageAccount : storageAccounts) {
      final String maxWriteConcurrencyStr = properties.get(format(STORAGE_WRITE_MAX_CONCURRENCY, storageAccount));
      final Integer expectedMaxWriteConcurrency = Integer.parseInt(maxWriteConcurrencyStr);
      assertThat(azureProperties.maxWriteConcurrency(storageAccount)).isEqualTo(expectedMaxWriteConcurrency);
    }

    // Assert max single upload size config for all storage accounts.
    for (String storageAccount : storageAccounts) {
      final String maxSingleUploadSizeStr =
          properties.get(format(STORAGE_WRITE_MAX_SINGLE_UPLOAD_SIZE, storageAccount));
      final Long maxSingleUploadSize = Long.parseLong(maxSingleUploadSizeStr);
      assertThat(azureProperties.maxSingleUploadSize(storageAccount)).isEqualTo(maxSingleUploadSize);
    }
  }

  @Test
  public void testInvalidStorageAuth() {
    final String storageAccount = "testing";
    final String invalidAuthType = "invalid-value";
    properties.put(format(STORAGE_AUTH_TYPE, storageAccount), invalidAuthType);
    final AzureProperties azureProperties = new AzureProperties(properties);

    AssertHelpers.assertThrows(
        "Should not allow invalid auth type",
        IllegalArgumentException.class,
        String.format("No enum constant org.apache.iceberg.azure.AuthType.%s", invalidAuthType),
        () -> {
          azureProperties.authType(storageAccount);
        });
  }

  @Test
  public void testNullProperties() {
    AssertHelpers.assertThrows(
        "Should not allow null properties map",
        NullPointerException.class,
        "Properties map " + "cannot be null",
        () -> {
          new AzureProperties(null);
        });
  }
}
