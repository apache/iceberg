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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.azure.AzureProperties;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

class TestADLSFileIOCredentialRefresh {

  private static final String STORAGE_ACCOUNT = "account1";
  private static final String CREDENTIAL_PREFIX =
      "abfss://container@account1.dfs.core.windows.net/dir";

  private static ClientAndServer mockServer;
  private static String credentialsUri;
  private static String catalogUri;

  @BeforeAll
  static void beforeAll() {
    mockServer = startClientAndServer(0);
    int port = mockServer.getPort();
    credentialsUri = String.format("http://127.0.0.1:%d/v1/credentials", port);
    catalogUri = String.format("http://127.0.0.1:%d/v1", port);
  }

  @AfterAll
  static void stopServer() {
    mockServer.stop();
  }

  @BeforeEach
  void before() {
    mockServer.reset();
  }

  @Test
  void credentialRefreshWithinFiveMinuteWindow() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            CREDENTIAL_PREFIX,
            ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                "initialSasToken",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                nearExpiryMs));

    String refreshedExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());
    LoadCredentialsResponse refreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(
                            AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                            "refreshedSasToken",
                            AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                            refreshedExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod("GET");
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(refreshResponse)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (ADLSFileIO fileIO = new ADLSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      // trigger client cache initialization which schedules the credential refresh
      fileIO.client("abfss://container@account1.dfs.core.windows.net/file");

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> mockServer.verify(mockRequest, VerificationTimes.atLeast(1)));

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                List<StorageCredential> credentials = fileIO.credentials();
                assertThat(credentials).hasSize(1);
                assertThat(credentials.get(0).config())
                    .containsEntry(
                        AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                        "refreshedSasToken")
                    .containsEntry(
                        AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                        refreshedExpiryMs);
              });
    }
  }

  @Test
  void credentialRefreshSchedulesNextRefresh() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            CREDENTIAL_PREFIX,
            ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                "initialSasToken",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                nearExpiryMs));

    // return credentials that also expire within 5 minutes so the next refresh fires immediately
    String firstRefreshExpiryMs =
        Long.toString(Instant.now().plus(2, ChronoUnit.MINUTES).toEpochMilli());
    String secondRefreshExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());

    LoadCredentialsResponse firstRefreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(
                            AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                            "firstRefreshedSasToken",
                            AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                            firstRefreshExpiryMs))
                    .build())
            .build();

    LoadCredentialsResponse secondRefreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(
                            AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                            "secondRefreshedSasToken",
                            AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                            secondRefreshExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod("GET");
    mockServer
        .when(mockRequest, org.mockserver.matchers.Times.once())
        .respond(
            response(LoadCredentialsResponseParser.toJson(firstRefreshResponse))
                .withStatusCode(200));
    mockServer
        .when(mockRequest, org.mockserver.matchers.Times.unlimited())
        .respond(
            response(LoadCredentialsResponseParser.toJson(secondRefreshResponse))
                .withStatusCode(200));

    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (ADLSFileIO fileIO = new ADLSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      fileIO.client("abfss://container@account1.dfs.core.windows.net/file");

      // the first refresh returns near-expiry credentials, which should schedule a second refresh
      Awaitility.await()
          .atMost(30, TimeUnit.SECONDS)
          .untilAsserted(() -> mockServer.verify(mockRequest, VerificationTimes.atLeast(2)));

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                List<StorageCredential> credentials = fileIO.credentials();
                assertThat(credentials).hasSize(1);
                assertThat(credentials.get(0).config())
                    .containsEntry(
                        AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                        "secondRefreshedSasToken")
                    .containsEntry(
                        AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                        secondRefreshExpiryMs);
              });
    }
  }

  @Test
  void noDuplicateRefreshAfterClientCacheRebuild() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            CREDENTIAL_PREFIX,
            ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                "initialSasToken",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                nearExpiryMs));

    String refreshedExpiryMs =
        Long.toString(Instant.now().plus(30, ChronoUnit.MINUTES).toEpochMilli());
    LoadCredentialsResponse refreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(
                            AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                            "refreshedSasToken",
                            AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                            refreshedExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod("GET");
    mockServer
        .when(mockRequest)
        .respond(
            response(LoadCredentialsResponseParser.toJson(refreshResponse)).withStatusCode(200));

    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (ADLSFileIO fileIO = new ADLSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      fileIO.client("abfss://container@account1.dfs.core.windows.net/file");

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertThat(fileIO.credentials().get(0).config())
                      .containsEntry(
                          AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                          "refreshedSasToken"));

      // rebuilding the client cache should not schedule a duplicate refresh
      fileIO.client("abfss://container@account1.dfs.core.windows.net/file");

      Thread.sleep(3_000);
      mockServer.verify(mockRequest, VerificationTimes.exactly(1));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Test
  void clientCacheInvalidatedAfterBackgroundRefresh() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            CREDENTIAL_PREFIX,
            ImmutableMap.of(
                AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                "initialSasToken",
                AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                nearExpiryMs));

    String refreshedExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());
    LoadCredentialsResponse refreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix(CREDENTIAL_PREFIX)
                    .config(
                        ImmutableMap.of(
                            AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                            "refreshedSasToken",
                            AzureProperties.ADLS_SAS_TOKEN_EXPIRES_AT_MS_PREFIX + STORAGE_ACCOUNT,
                            refreshedExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod("GET");
    mockServer
        .when(mockRequest)
        .respond(
            response(LoadCredentialsResponseParser.toJson(refreshResponse)).withStatusCode(200));

    Map<String, String> properties =
        ImmutableMap.of(
            AzureProperties.ADLS_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (ADLSFileIO fileIO = new ADLSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      String path = "abfss://container@account1.dfs.core.windows.net/file";
      DataLakeFileSystemClient clientBeforeRefresh = fileIO.client(path);

      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertThat(fileIO.credentials().get(0).config())
                      .containsEntry(
                          AzureProperties.ADLS_SAS_TOKEN_PREFIX + STORAGE_ACCOUNT,
                          "refreshedSasToken"));

      DataLakeFileSystemClient clientAfterRefresh = fileIO.client(path);
      assertThat(clientAfterRefresh).isNotSameAs(clientBeforeRefresh);
    }
  }
}
