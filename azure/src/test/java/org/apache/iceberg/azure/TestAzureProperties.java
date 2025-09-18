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

import static org.apache.iceberg.azure.AzureProperties.ADLS_CONNECTION_STRING_PREFIX;
import static org.apache.iceberg.azure.AzureProperties.ADLS_READ_BLOCK_SIZE;
import static org.apache.iceberg.azure.AzureProperties.ADLS_REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.azure.AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_PREFIX;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SHARED_KEY_ACCOUNT_KEY;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SHARED_KEY_ACCOUNT_NAME;
import static org.apache.iceberg.azure.AzureProperties.ADLS_WRITE_BLOCK_SIZE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.file.datalake.DataLakeFileSystemClientBuilder;
import java.io.IOException;
import java.util.Optional;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.azure.adlsv2.VendedAdlsCredentialProvider;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import reactor.core.publisher.Mono;

public class TestAzureProperties {

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  public void testSerializable(TestHelpers.RoundTripSerializer<AzureProperties> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    AzureProperties props =
        new AzureProperties(
            ImmutableMap.<String, String>builder()
                .put(ADLS_SAS_TOKEN_PREFIX + "foo", "bar")
                .put(ADLS_CONNECTION_STRING_PREFIX + "foo", "bar")
                .put(ADLS_READ_BLOCK_SIZE, "42")
                .put(ADLS_WRITE_BLOCK_SIZE, "42")
                .put(ADLS_SHARED_KEY_ACCOUNT_NAME, "me")
                .put(ADLS_SHARED_KEY_ACCOUNT_KEY, "secret")
                .build());

    AzureProperties serdedProps = roundTripSerializer.apply(props);
    assertThat(serdedProps.adlsReadBlockSize()).isEqualTo(props.adlsReadBlockSize());
    assertThat(serdedProps.adlsWriteBlockSize()).isEqualTo(props.adlsWriteBlockSize());
  }

  @Test
  public void testWithSasToken() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.sas-token.account1", "token"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    verify(clientBuilder).sasToken(any());
    verify(clientBuilder, times(0)).credential(any(TokenCredential.class));
    verify(clientBuilder, never()).credential(any(StorageSharedKeyCredential.class));
  }

  @Test
  public void testWithRefreshCredentialsEndpoint() {
    AzureProperties props =
        new AzureProperties(
            ImmutableMap.of(
                ADLS_REFRESH_CREDENTIALS_ENDPOINT,
                "endpoint",
                CatalogProperties.URI,
                "catalog-endpoint"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    Optional<VendedAdlsCredentialProvider> vendedAdlsCredentialProvider =
        props.vendedAdlsCredentialProvider();

    verify(clientBuilder, never()).credential(any(AzureSasCredential.class));
    verify(clientBuilder, never()).sasToken(any());
    verify(clientBuilder, never()).credential(any(StorageSharedKeyCredential.class));
    assertThat(vendedAdlsCredentialProvider).isPresent();
  }

  @Test
  public void testWithRefreshCredentialsEndpointDisabled() {
    AzureProperties props =
        new AzureProperties(
            ImmutableMap.of(
                ADLS_REFRESH_CREDENTIALS_ENDPOINT,
                "endpoint",
                ADLS_REFRESH_CREDENTIALS_ENABLED,
                "false"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account1", clientBuilder);
    Optional<VendedAdlsCredentialProvider> vendedAdlsCredentialProvider =
        props.vendedAdlsCredentialProvider();
    verify(clientBuilder).credential(any(DefaultAzureCredential.class));
    assertThat(vendedAdlsCredentialProvider).isEmpty();
  }

  @Test
  public void testNoMatchingSasToken() {
    AzureProperties props =
        new AzureProperties(ImmutableMap.of("adls.sas-token.account1", "token"));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account2", clientBuilder);
    verify(clientBuilder, times(0)).sasToken(any());
    verify(clientBuilder).credential(any(TokenCredential.class));
    verify(clientBuilder, never()).credential(any(StorageSharedKeyCredential.class));
  }

  @Test
  public void testNoSasToken() {
    AzureProperties props = new AzureProperties();

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account", clientBuilder);
    verify(clientBuilder, times(0)).sasToken(any());
    verify(clientBuilder).credential(any(TokenCredential.class));
    verify(clientBuilder, never()).credential(any(StorageSharedKeyCredential.class));
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

  @Test
  public void testSharedKey() {
    assertThatIllegalArgumentException()
        .isThrownBy(
            () ->
                new AzureProperties(
                    ImmutableMap.of(
                        ADLS_SHARED_KEY_ACCOUNT_KEY, "not-really-base64-encoded-key-here")))
        .withMessage(
            String.format(
                "Azure authentication: shared-key requires both %s and %s",
                ADLS_SHARED_KEY_ACCOUNT_NAME, ADLS_SHARED_KEY_ACCOUNT_KEY));
    assertThatIllegalArgumentException()
        .isThrownBy(
            () -> new AzureProperties(ImmutableMap.of(ADLS_SHARED_KEY_ACCOUNT_NAME, "account")))
        .withMessage(
            String.format(
                "Azure authentication: shared-key requires both %s and %s",
                ADLS_SHARED_KEY_ACCOUNT_NAME, ADLS_SHARED_KEY_ACCOUNT_KEY));

    AzureProperties props =
        new AzureProperties(
            ImmutableMap.of(
                ADLS_SHARED_KEY_ACCOUNT_NAME,
                "account",
                ADLS_SHARED_KEY_ACCOUNT_KEY,
                "not-really-base64-encoded-key-here"));
    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    props.applyClientConfiguration("account", clientBuilder);
    verify(clientBuilder).credential(any(StorageSharedKeyCredential.class));
    verify(clientBuilder, never()).credential(any(TokenCredential.class));
  }

  @Test
  public void testAdlsToken() {
    String testToken = "test-token-value";
    AzureProperties props = new AzureProperties(ImmutableMap.of("adls.token", testToken));

    DataLakeFileSystemClientBuilder clientBuilder = mock(DataLakeFileSystemClientBuilder.class);
    ArgumentCaptor<TokenCredential> credentialCaptor =
        ArgumentCaptor.forClass(TokenCredential.class);

    props.applyClientConfiguration("account", clientBuilder);

    verify(clientBuilder).credential(credentialCaptor.capture());
    TokenCredential capturedCredential = credentialCaptor.getValue();

    // Check that the credential returns the correct token
    Mono<AccessToken> tokenMono = capturedCredential.getToken(new TokenRequestContext());
    AccessToken accessToken = tokenMono.block();
    assertThat(accessToken.getToken()).isEqualTo(testToken);

    // Verify other credential types were not used
    verify(clientBuilder, never()).sasToken(any());
    verify(clientBuilder, never()).credential(any(StorageSharedKeyCredential.class));
    verify(clientBuilder, never()).credential(any(com.azure.identity.DefaultAzureCredential.class));
  }
}
