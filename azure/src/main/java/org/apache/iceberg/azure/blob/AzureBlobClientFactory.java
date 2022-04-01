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

import com.azure.core.credential.AzureSasCredential;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import java.util.Optional;
import org.apache.iceberg.azure.AuthType;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AzureBlobClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(AzureBlobClientFactory.class);

  private AzureBlobClientFactory() {
  }

  public static BlobClient createBlobClient(AzureURI azureURI, AzureProperties azureProperties) {
    final String storageAccount = azureURI.storageAccount();
    final BlobClientBuilder builder = new BlobClientBuilder();

    final Optional<String> connectionString = azureProperties.connectionString(storageAccount);
    // Connection string should contain storage endpoint and required auth properties.
    if (connectionString.isPresent()) {
      LOG.debug("Using azure storage connection string to build the blob client for {}", storageAccount);
      Preconditions.checkArgument(!connectionString.get().isEmpty(), "Connection string cannot be empty.");
      builder.connectionString(connectionString.get());
    } else {
      final String endpoint = storageEndpoint(storageAccount, azureProperties);
      LOG.debug("Using {} endpoint for {}", endpoint, storageAccount);
      builder.endpoint(endpoint);
      final AuthType authType = azureProperties.authType(storageAccount);
      setAuth(storageAccount, authType, azureProperties, builder);
    }

    return builder.containerName(azureURI.container()).blobName(azureURI.path()).buildClient();
  }

  private static void setAuth(
      String storageAccount, AuthType authType, AzureProperties azureProperties, BlobClientBuilder builder) {
    switch (authType) {
      case SharedKey:
        LOG.debug("Using SharedKey method for {} authentication", storageAccount);
        final Optional<String> accountKey = azureProperties.accountKey(storageAccount);
        Preconditions.checkArgument(accountKey.isPresent(), "Account key must be set.");
        builder.credential(new StorageSharedKeyCredential(storageAccount, accountKey.get()));
        break;
      case SharedAccessSignature:
        LOG.debug("Using SharedAccessSignature method for {} authentication", storageAccount);
        final Optional<String> sharedAccessSignature = azureProperties.sharedAccessSignature(storageAccount);
        Preconditions.checkArgument(sharedAccessSignature.isPresent(), "Shared access signature must be set.");
        builder.credential(new AzureSasCredential(sharedAccessSignature.get()));
        break;
      case None:
        LOG.debug("Using no authentication for {}", storageAccount);
        break;
    }
  }

  private static String storageEndpoint(String storageAccount, AzureProperties azureProperties) {
    return azureProperties.endpoint(storageAccount)
        .orElseGet(() -> String.format("https://%s.blob.core.windows.net", storageAccount));
  }
}
