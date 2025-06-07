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

import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_PREFIX;
import static org.apache.iceberg.azure.adlsv2.AzuriteContainer.ACCOUNT_HOST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import com.azure.storage.file.datalake.DataLakeFileClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
public class PrefixedADLSClientTest extends BaseAzuriteTest {

  @Test
  public void invalidParameters() {
    assertThatThrownBy(() -> new PrefixedADLSClient(null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedADLSClient("", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid storage prefix: null or empty");

    assertThatThrownBy(() -> new PrefixedADLSClient("abfs://container", null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
  }

  @Test
  public void validParameters() {
    Map<String, String> properties =
        ImmutableMap.of(ADLS_SAS_TOKEN_PREFIX + ACCOUNT_HOST, "sasToken");

    try (var mockedAzurePropertiesConstruction = mockAzurePropertiesConstruction()) {

      PrefixedADLSClient client = new PrefixedADLSClient("abfs", properties);
      assertThat(client.storagePrefix()).isEqualTo("abfs");

      assertThat(mockedAzurePropertiesConstruction.constructed()).hasSize(1);
      AzureProperties azureProperties = mockedAzurePropertiesConstruction.constructed().get(0);
      verify(azureProperties).vendedAdlsCredentialProvider();

      String storagePath = AZURITE_CONTAINER.location("path/to/file");

      assertThat(client.client(storagePath)).isInstanceOf(DataLakeFileSystemClient.class);
      verify(azureProperties)
          .applyClientConfiguration(eq(ACCOUNT_HOST), any(DataLakeFileSystemClientBuilder.class));

      assertThat(client.fileClient(storagePath)).isInstanceOf(DataLakeFileClient.class);
    }
  }
}
