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

public class AzureBlobClientFactory {

  private static final String AZURE_BLOB_STORAGE_ENDPOINT_TEMPLATE = "%s.blob.core.windows.net";

  private AzureBlobClientFactory() {
  }

  public static BlobClient createBlobClient(AzureURI azureURI, AzureProperties azureProperties) {
    final String storageAccount = azureURI.storageAccount();
    final BlobClientBuilder builder =
        new BlobClientBuilder().containerName(azureURI.container()).blobName(azureURI.path());

    final Optional<String> connectionString = azureProperties.connectionString(storageAccount);
    // Connection string should contain storage endpoint and required auth properties.
    if (connectionString.isPresent()) {
      Preconditions.checkArgument(!connectionString.get().isEmpty(), "Connection string cannot be empty.");
      builder.connectionString(connectionString.get());
    } else {
      builder.endpoint(storageEndpoint(storageAccount));
      final AuthType authType = azureProperties.authType(storageAccount);
      setAuth(storageAccount, authType, azureProperties, builder);
    }

    return builder.buildClient();
  }

  private static void setAuth(
      String storageAccount, AuthType authType, AzureProperties azureProperties, BlobClientBuilder builder) {
    switch (authType) {
      case SharedKey:
        final Optional<String> accountKey = azureProperties.accountKey(storageAccount);
        Preconditions.checkArgument(accountKey.isPresent(), "Account key must be set.");
        builder.credential(new StorageSharedKeyCredential(storageAccount, accountKey.get()));
        break;
      case SharedAccessSignature:
        final Optional<String> sharedAccessSignature = azureProperties.sharedAccessSignature(storageAccount);
        Preconditions.checkArgument(sharedAccessSignature.isPresent(), "Shared access signature must be set.");
        builder.credential(new AzureSasCredential(sharedAccessSignature.get()));
        break;
      case None:
        break;
    }
  }

  private static String storageEndpoint(String storageAccount) {
    return String.format(AZURE_BLOB_STORAGE_ENDPOINT_TEMPLATE, storageAccount);
  }
}
