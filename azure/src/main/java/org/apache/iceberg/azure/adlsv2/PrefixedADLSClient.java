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
package org.apache.iceberg.azure.adlsv2;

import com.azure.core.http.HttpClient;
import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;

class PrefixedADLSClient implements AutoCloseable {

  private static final HttpClient HTTP = HttpClient.createDefault();
  private final String storagePrefix;
  private final AzureProperties azureProperties;

  private VendedAdlsCredentialProvider vendedAdlsCredentialProvider;

  PrefixedADLSClient(String storagePrefix, Map<String, String> properties) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(storagePrefix), "Invalid storage prefix: null or empty");
    Preconditions.checkArgument(null != properties, "Invalid properties: null");
    this.storagePrefix = storagePrefix;
    this.azureProperties = new AzureProperties(properties);
    this.azureProperties
        .vendedAdlsCredentialProvider()
        .ifPresent(provider -> this.vendedAdlsCredentialProvider = provider);
  }

  DataLakeFileClient fileClient(String storagePath) {
    ADLSLocation location = new ADLSLocation(storagePath);
    return client(storagePath).getFileClient(location.path());
  }

  DataLakeFileSystemClient client(String storagePath) {
    ADLSLocation location = new ADLSLocation(storagePath);
    DataLakeFileSystemClientBuilder clientBuilder =
        new DataLakeFileSystemClientBuilder().httpClient(HTTP);

    location.container().ifPresent(clientBuilder::fileSystemName);
    Optional.ofNullable(vendedAdlsCredentialProvider)
        .map(p -> new VendedAzureSasCredentialPolicy(location.host(), p))
        .ifPresent(clientBuilder::addPolicy);
    azureProperties.applyClientConfiguration(location.host(), clientBuilder);

    return clientBuilder.buildClient();
  }

  AzureProperties azureProperties() {
    return azureProperties;
  }

  public String storagePrefix() {
    return storagePrefix;
  }

  @Override
  public void close() {
    if (vendedAdlsCredentialProvider != null) {
      vendedAdlsCredentialProvider.close();
    }
  }
}
