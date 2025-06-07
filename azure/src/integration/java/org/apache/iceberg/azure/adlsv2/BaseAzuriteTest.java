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

import static org.apache.iceberg.azure.AzureProperties.ADLS_CONNECTION_STRING_PREFIX;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME;
import static org.apache.iceberg.azure.adlsv2.AzuriteContainer.ACCOUNT;
import static org.apache.iceberg.azure.adlsv2.AzuriteContainer.ACCOUNT_HOST;
import static org.apache.iceberg.azure.adlsv2.AzuriteContainer.KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mockConstruction;

import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.util.Map;
import org.apache.iceberg.azure.AzureProperties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.MockedConstruction;

public class BaseAzuriteTest {
  protected static final AzuriteContainer AZURITE_CONTAINER = new AzuriteContainer();

  @BeforeAll
  public static void beforeAll() {
    AZURITE_CONTAINER.start();
  }

  @AfterAll
  public static void afterAll() {
    AZURITE_CONTAINER.stop();
  }

  @BeforeEach
  public void baseBefore() {
    AZURITE_CONTAINER.createStorageContainer();
  }

  @AfterEach
  public void baseAfter() {
    AZURITE_CONTAINER.deleteStorageContainer();
  }

  protected ADLSFileIO createFileIO() {
    ADLSFileIO adlsFileIO = new ADLSFileIO();
    adlsFileIO.initialize(azureProperties());
    return adlsFileIO;
  }

  protected MockedConstruction<AzureProperties> mockAzurePropertiesConstruction() {
    return mockConstruction(
        AzureProperties.class,
        (mock, context) -> {
          doAnswer(
                  invoke -> {
                    DataLakeFileSystemClientBuilder clientBuilder = invoke.getArgument(1);
                    clientBuilder.endpoint(AZURITE_CONTAINER.endpoint());
                    clientBuilder.credential(AZURITE_CONTAINER.credential());
                    return null;
                  })
              .when(mock)
              .applyClientConfiguration(any(), any());
        });
  }

  protected Map<String, String> azureProperties() {
    return Map.of(
        ADLS_SHARED_KEY_ACCOUNT_NAME,
        ACCOUNT,
        ADLS_SHARED_KEY_ACCOUNT_KEY,
        KEY,
        ADLS_CONNECTION_STRING_PREFIX + ACCOUNT_HOST,
        AZURITE_CONTAINER.endpoint());
  }
}
