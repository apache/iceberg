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

package org.apache.iceberg.azure.blob;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.azure.AuthType;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.assertj.core.api.Assumptions;

public class AzureBlobTestUtils {

  public static final String STORAGE_ACCOUNT_1 = System.getenv("STORAGE_ACCOUNT_1");
  public static final String STORAGE_ACCOUNT_1_KEY = System.getenv("STORAGE_ACCOUNT_1_KEY");
  public static final String STORAGE_ACCOUNT_2 = System.getenv("STORAGE_ACCOUNT_2");
  public static final String STORAGE_ACCOUNT_2_KEY = System.getenv("STORAGE_ACCOUNT_2_KEY");
  public static final String STORAGE_ACCOUNT_3 = System.getenv("STORAGE_ACCOUNT_3");
  public static final String STORAGE_ACCOUNT_3_KEY = System.getenv("STORAGE_ACCOUNT_3_KEY");

  private AzureBlobTestUtils() {
  }

  public static Map<String, String> storageAccount1AuthProperties() {
    return storageAccountAuthProperties(STORAGE_ACCOUNT_1, STORAGE_ACCOUNT_1_KEY);
  }

  public static Map<String, String> storageAccount2AuthProperties() {
    return storageAccountAuthProperties(STORAGE_ACCOUNT_2, STORAGE_ACCOUNT_2_KEY);
  }

  public static Map<String, String> storageAccount3AuthProperties() {
    return storageAccountAuthProperties(STORAGE_ACCOUNT_3, STORAGE_ACCOUNT_3_KEY);
  }

  public static BlobServiceClient storageAccount1BlobServiceClient() {
    return blobServiceClient(STORAGE_ACCOUNT_1, STORAGE_ACCOUNT_1_KEY);
  }

  public static BlobServiceClient storageAccount2BlobServiceClient() {
    return blobServiceClient(STORAGE_ACCOUNT_2, STORAGE_ACCOUNT_2_KEY);
  }

  public static BlobServiceClient storageAccount3BlobServiceClient() {
    return blobServiceClient(STORAGE_ACCOUNT_3, STORAGE_ACCOUNT_3_KEY);
  }

  public static void deleteAndCreateContainer(BlobContainerClient container) {
    deleteContainerIfExists(container);
    container.create();
  }

  public static void deleteContainerIfExists(BlobContainerClient container) {
    if (container.exists()) {
      container.delete();
    }
  }

  public static String abfsLocation(String storageAccount, String container, String blobPath) {
    Assumptions.assumeThat(storageAccount).isNotBlank();
    Assumptions.assumeThat(container).isNotBlank();
    Assumptions.assumeThat(blobPath).isNotBlank();
    return String.format("abfs://%s@%s.dfs.core.windows.net%s", container, storageAccount, blobPath);
  }

  public static AzureURI randomAzureURI(String storageAccount, String container) {
    String location =
        AzureBlobTestUtils.abfsLocation(storageAccount, container, String.format("/data/%s.dat", UUID.randomUUID()));
    return AzureURI.from(location);
  }

  private static BlobServiceClient blobServiceClient(String storageAccount, String storageAccountKey) {
    Assumptions.assumeThat(storageAccount).isNotBlank();
    Assumptions.assumeThat(storageAccountKey).isNotBlank();

    return new BlobServiceClientBuilder().endpoint(endpoint(storageAccount))
        .credential(new StorageSharedKeyCredential(storageAccount, storageAccountKey))
        .buildClient();
  }

  private static Map<String, String> storageAccountAuthProperties(String storageAccount, String storageAccountKey) {
    ImmutableMap.Builder<String, String> builder = ImmutableMap.<String, String>builder()
        .put(String.format(AzureProperties.STORAGE_AUTH_TYPE, storageAccount), AuthType.SharedKey.toString())
        .put(String.format(AzureProperties.STORAGE_ACCOUNT_KEY, storageAccount), storageAccountKey);

    azuriteEndpoint(storageAccount).ifPresent(endpoint -> builder.put(String.format(
        AzureProperties.STORAGE_ENDPOINT,
        storageAccount), endpoint));

    return builder.build();
  }

  private static String endpoint(String storageAccount) {
    return azuriteEndpoint(storageAccount).orElseGet(() -> String.format(
        "https://%s.blob.core.windows.net",
        storageAccount));
  }

  private static Optional<String> azuriteEndpoint(String storageAccount) {
    return Optional.ofNullable(System.getenv("AZURITE_ENDPOINT"))
        .filter(endpoint -> !endpoint.isEmpty())
        .map(endpoint -> String.format("%s/%s", endpoint, storageAccount));
  }
}
