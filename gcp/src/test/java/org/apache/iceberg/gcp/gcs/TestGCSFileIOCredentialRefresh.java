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
package org.apache.iceberg.gcp.gcs;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.io.StorageCredential;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HttpMethod;
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

class TestGCSFileIOCredentialRefresh {

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
  void credentialRefreshSchedulesNextRefresh() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            "gs://bucket/path",
            ImmutableMap.of(
                GCPProperties.GCS_OAUTH2_TOKEN,
                "initialToken",
                GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
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
                    .prefix("gs://bucket/path")
                    .config(
                        ImmutableMap.of(
                            GCPProperties.GCS_OAUTH2_TOKEN,
                            "firstRefreshedToken",
                            GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                            firstRefreshExpiryMs))
                    .build())
            .build();

    LoadCredentialsResponse secondRefreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("gs://bucket/path")
                    .config(
                        ImmutableMap.of(
                            GCPProperties.GCS_OAUTH2_TOKEN,
                            "secondRefreshedToken",
                            GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                            secondRefreshExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
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
            GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      fileIO.client();

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
                    .containsEntry(GCPProperties.GCS_OAUTH2_TOKEN, "secondRefreshedToken")
                    .containsEntry(
                        GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, secondRefreshExpiryMs);
              });
    }
  }

  @Test
  void credentialRefreshWithinFiveMinuteWindow() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            "gs://bucket/path",
            ImmutableMap.of(
                GCPProperties.GCS_OAUTH2_TOKEN,
                "initialToken",
                GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                nearExpiryMs));

    String refreshedExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());
    LoadCredentialsResponse refreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("gs://bucket/path")
                    .config(
                        ImmutableMap.of(
                            GCPProperties.GCS_OAUTH2_TOKEN,
                            "refreshedToken",
                            GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                            refreshedExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(refreshResponse)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
            credentialsUri,
            CatalogProperties.URI,
            catalogUri);

    try (GCSFileIO fileIO = new GCSFileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      // trigger storageByPrefix() to build the client map and schedule the refresh
      fileIO.client();

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
                    .containsEntry(GCPProperties.GCS_OAUTH2_TOKEN, "refreshedToken")
                    .containsEntry(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, refreshedExpiryMs);
              });
    }
  }
}
