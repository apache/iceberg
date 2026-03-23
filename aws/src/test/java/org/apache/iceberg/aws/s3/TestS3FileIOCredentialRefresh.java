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
package org.apache.iceberg.aws.s3;

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
import org.apache.iceberg.aws.AwsProperties;
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

public class TestS3FileIOCredentialRefresh {

  private static final int PORT = 3233;
  private static final String CREDENTIALS_URI =
      String.format("http://127.0.0.1:%d/v1/credentials", PORT);
  private static final String CATALOG_URI = String.format("http://127.0.0.1:%d/v1", PORT);
  private static ClientAndServer mockServer;

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
  public void credentialRefreshSchedulesNextRefresh() {
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            "s3://bucket/path",
            ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID,
                "initialAccessKey",
                S3FileIOProperties.SECRET_ACCESS_KEY,
                "initialSecretKey",
                S3FileIOProperties.SESSION_TOKEN,
                "initialToken",
                S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                nearExpiryMs));

    String firstRefreshExpiryMs =
        Long.toString(Instant.now().plus(2, ChronoUnit.MINUTES).toEpochMilli());
    String secondRefreshExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());

    LoadCredentialsResponse firstRefreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3://bucket/path")
                    .config(
                        ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID,
                            "firstRefreshedAccessKey",
                            S3FileIOProperties.SECRET_ACCESS_KEY,
                            "firstRefreshedSecretKey",
                            S3FileIOProperties.SESSION_TOKEN,
                            "firstRefreshedToken",
                            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                            firstRefreshExpiryMs))
                    .build())
            .build();

    LoadCredentialsResponse secondRefreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3://bucket/path")
                    .config(
                        ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID,
                            "secondRefreshedAccessKey",
                            S3FileIOProperties.SECRET_ACCESS_KEY,
                            "secondRefreshedSecretKey",
                            S3FileIOProperties.SESSION_TOKEN,
                            "secondRefreshedToken",
                            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
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
            AwsProperties.CLIENT_FACTORY,
            StaticClientFactory.class.getName(),
            VendedCredentialsProvider.URI,
            CREDENTIALS_URI,
            CatalogProperties.URI,
            CATALOG_URI,
            "init-creation-stacktrace",
            "false");

    StaticClientFactory.client = null;
    try (S3FileIO fileIO = new S3FileIO()) {
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
                    .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "secondRefreshedAccessKey")
                    .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "secondRefreshedSecretKey")
                    .containsEntry(S3FileIOProperties.SESSION_TOKEN, "secondRefreshedToken")
                    .containsEntry(
                        S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, secondRefreshExpiryMs);
              });
    }
  }

  @Test
  public void credentialRefreshWithinFiveMinuteWindow() {
    // Set up credentials expiring within the next 5 minutes so the refresh triggers immediately
    String nearExpiryMs = Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli());

    StorageCredential initialCredential =
        StorageCredential.create(
            "s3://bucket/path",
            ImmutableMap.of(
                S3FileIOProperties.ACCESS_KEY_ID,
                "initialAccessKey",
                S3FileIOProperties.SECRET_ACCESS_KEY,
                "initialSecretKey",
                S3FileIOProperties.SESSION_TOKEN,
                "initialToken",
                S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                nearExpiryMs));

    // Mock the credentials endpoint to return refreshed credentials
    String refreshedExpiryMs =
        Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli());
    LoadCredentialsResponse refreshResponse =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3://bucket/path")
                    .config(
                        ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID,
                            "refreshedAccessKey",
                            S3FileIOProperties.SECRET_ACCESS_KEY,
                            "refreshedSecretKey",
                            S3FileIOProperties.SESSION_TOKEN,
                            "refreshedToken",
                            S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                            refreshedExpiryMs))
                    .build())
            .build();

    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(refreshResponse)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    Map<String, String> properties =
        ImmutableMap.of(
            AwsProperties.CLIENT_FACTORY,
            StaticClientFactory.class.getName(),
            VendedCredentialsProvider.URI,
            CREDENTIALS_URI,
            CatalogProperties.URI,
            CATALOG_URI,
            "init-creation-stacktrace",
            "false");

    StaticClientFactory.client = null;
    try (S3FileIO fileIO = new S3FileIO()) {
      fileIO.initialize(properties);
      fileIO.setCredentials(List.of(initialCredential));

      // Trigger clientByPrefix() to build the client map and schedule the refresh.
      // Since the credential expires within 5 minutes, the delay is negative/zero
      // and the refresh fires immediately.
      fileIO.client();

      // Wait for the scheduled refresh to call the credentials endpoint
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> mockServer.verify(mockRequest, VerificationTimes.atLeast(1)));

      // Verify the credentials were updated with the refreshed values
      Awaitility.await()
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () -> {
                List<StorageCredential> credentials = fileIO.credentials();
                assertThat(credentials).hasSize(1);
                assertThat(credentials.get(0).config())
                    .containsEntry(S3FileIOProperties.ACCESS_KEY_ID, "refreshedAccessKey")
                    .containsEntry(S3FileIOProperties.SECRET_ACCESS_KEY, "refreshedSecretKey")
                    .containsEntry(S3FileIOProperties.SESSION_TOKEN, "refreshedToken")
                    .containsEntry(
                        S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, refreshedExpiryMs);
              });
    }
  }
}
