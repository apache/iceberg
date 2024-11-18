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

import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX;
import static org.apache.iceberg.azure.AzureProperties.ADLS_SAS_TOKEN_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.HttpMethod;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

public class VendedAzureSasCredentialProviderTest {
  private static final int PORT = 3232;
  private static final String URI = String.format("http://127.0.0.1:%d/v1/credentials", PORT);
  private static ClientAndServer mockServer;
  private static final String STORAGE_ACCOUNT = "account1";
  private static final String CREDENTIAL_PREFIX =
      "abfs://container@account1.dfs.core.windows.net/dir";
  private static final String STORAGE_ACCOUNT_2 = "account2";
  private static final String CREDENTIAL_PREFIX_2 =
      "abfs://container@account2.dfs.core.windows.net/dir";

  @BeforeAll
  public static void beforeAll() {
    mockServer = startClientAndServer(PORT);
  }

  @AfterAll
  public static void stopServer() {
    mockServer.stop();
  }

  @BeforeEach
  public void before() {
    mockServer.reset();
  }

  @Test
  public void invalidOrMissingUri() {
    assertThatThrownBy(() -> new VendedAzureSasCredentialProvider(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
    assertThatThrownBy(() -> new VendedAzureSasCredentialProvider(ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid URI: null");

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, "invalid uri"))) {
      assertThatThrownBy(() -> provider.getCredential(STORAGE_ACCOUNT))
          .isInstanceOf(RESTException.class)
          .hasMessageStartingWith("Failed to create request URI from base invalid uri");
    }
  }

  @Test
  public void noADLSCredentials() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());

    HttpResponse mockResponse =
        response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      assertThatThrownBy(() -> provider.getCredential(STORAGE_ACCOUNT))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid ADLS Credentials for storage-account account1: empty");
    }
  }

  @Test
  public void expirationNotSet() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT, "randomSasToken"))
                    .build())
            .build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      assertThatThrownBy(() -> provider.getCredential(STORAGE_ACCOUNT))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid ADLS Credentials: adls.sas-token-expire-at-ms.account1 not set");
    }
  }

  @Test
  public void nonExpiredSasToken() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credential =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      AzureSasCredential azureSasCredential = provider.getCredential(STORAGE_ACCOUNT);
      assertThat(azureSasCredential.getSignature())
          .isEqualTo(credential.config().get(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT));

      for (int i = 0; i < 5; i++) {
        // resolving credentials multiple times should not hit the credentials endpoint again
        assertThat(provider.getCredential(STORAGE_ACCOUNT)).isSameAs(azureSasCredential);
      }
    }
    mockServer.verify(mockRequest, VerificationTimes.once());
  }

  @Test
  public void expiredSasToken() throws InterruptedException {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credential =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      AzureSasCredential azureSasCredential = provider.getCredential(STORAGE_ACCOUNT);
      assertThat(azureSasCredential.getSignature())
          .isEqualTo(credential.config().get(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT));

      Thread.sleep(10);
      // Since expiration time past to current time, the refresh will fall back at minimum 10ms
      // interval
    }
    mockServer.verify(mockRequest, VerificationTimes.atLeast(2));
  }

  @Test
  public void multipleADLSCredentialsPerStorageAccount() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credential1 =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken1",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    Credential credential2 =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX + "/dir2")
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken2",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential1, credential2).build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      assertThatThrownBy(() -> provider.getCredential(STORAGE_ACCOUNT))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "Invalid ADLS Credentials: only one ADLS credential should exist per storage-account");
    }
  }

  @Test
  public void multipleStorageAccounts() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credential1 =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken1",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    Credential credential2 =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX_2)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT_2,
                    "randomSasToken2",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT_2,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential1, credential2).build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      AzureSasCredential azureSasCredential1 = provider.getCredential(STORAGE_ACCOUNT);
      AzureSasCredential azureSasCredential2 = provider.getCredential(STORAGE_ACCOUNT_2);
      assertThat(azureSasCredential1).isNotSameAs(azureSasCredential2);
      assertThat(azureSasCredential1.getSignature())
          .isEqualTo(credential1.config().get(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT));
      assertThat(azureSasCredential2.getSignature())
          .isEqualTo(credential2.config().get(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT_2));
    }
  }

  @Test
  public void serializableTest() throws IOException, ClassNotFoundException {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credential =
        ImmutableCredential.builder()
            .prefix(CREDENTIAL_PREFIX)
            .config(
                ImmutableMap.of(
                    ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                    "randomSasToken",
                    ADLS_SAS_TOKEN_EXPIRE_AT_MS_PREFIX + STORAGE_ACCOUNT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedAzureSasCredentialProvider provider =
        new VendedAzureSasCredentialProvider(
            ImmutableMap.of(VendedAzureSasCredentialProvider.URI, URI))) {
      AzureSasCredential azureSasCredential = provider.getCredential(STORAGE_ACCOUNT);
      assertThat(azureSasCredential.getSignature())
          .isEqualTo(credential.config().get(ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT));

      VendedAzureSasCredentialProvider deserializedProvider =
          TestHelpers.roundTripSerialize(provider);
      AzureSasCredential reGeneratedAzureSasCredential =
          deserializedProvider.getCredential(STORAGE_ACCOUNT);

      assertThat(azureSasCredential).isNotSameAs(reGeneratedAzureSasCredential);
    }
    mockServer.verify(mockRequest, VerificationTimes.exactly(2));
  }
}
