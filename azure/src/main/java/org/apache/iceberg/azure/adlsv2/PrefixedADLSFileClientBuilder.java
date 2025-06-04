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

import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.azure.AzureProperties;

class PrefixedADLSFileClientBuilder implements AutoCloseable {

  private final String storagePrefix;
  private final AzureProperties azureProperties;

  private VendedAdlsCredentialProvider vendedAdlsCredentialProvider;

  PrefixedADLSFileClientBuilder(String storagePrefix, Map<String, String> properties) {
    this.storagePrefix = storagePrefix;
    this.azureProperties = new AzureProperties(properties);
    this.azureProperties
        .vendedAdlsCredentialProvider()
        .ifPresent(provider -> this.vendedAdlsCredentialProvider = provider);
  }

  void applyConfig(DataLakeFileSystemClientBuilder builder, String storagePath) {
    ADLSLocation location = new ADLSLocation(storagePath);

    Optional.ofNullable(vendedAdlsCredentialProvider)
        .map(p -> new VendedAzureSasCredentialPolicy(location.host(), p))
        .ifPresent(builder::addPolicy);
    azureProperties.applyClientConfiguration(location.host(), builder);
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
