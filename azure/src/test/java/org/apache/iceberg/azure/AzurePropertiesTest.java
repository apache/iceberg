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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.azure.core.credential.TokenCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

public class AzurePropertiesTest {

  @Test
  public void testWithSasToken() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.sas-token.account1", "token"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    verify(clientBuilder).sasToken(any());
    verify(clientBuilder, times(0)).credential(any(TokenCredential.class));
  }

  @Test
  public void testNoMatchingSasToken() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.sas-token.account1", "token"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account2", clientBuilder);
    verify(clientBuilder, times(0)).sasToken(any());
    verify(clientBuilder).credential(any(TokenCredential.class));
  }

  @Test
  public void testNoSasToken() {
    AzureProperties props = new AzureProperties();

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account", clientBuilder);
    verify(clientBuilder, times(0)).sasToken(any());
    verify(clientBuilder).credential(any(TokenCredential.class));
  }

  @Test
  public void testWithConnectionString() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.connection-string.account1", "http://endpoint"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    verify(clientBuilder).endpoint("http://endpoint");
  }

  @Test
  public void testNoMatchingConnectionString() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.connection-string.account2", "http://endpoint"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    verify(clientBuilder).endpoint("https://account1");
  }

  @Test
  public void testNoConnectionString() {
    AzureProperties props = new AzureProperties();

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account", clientBuilder);
    verify(clientBuilder).endpoint("https://account");
  }
}
