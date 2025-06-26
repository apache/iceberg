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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;

import com.google.auth.oauth2.AccessToken;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HttpMethod;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponseParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;

public class TestOAuth2RefreshCredentialsHandler {
  private static final int PORT = 3333;
  private static final String CREDENTIALS_URI =
      String.format("http://127.0.0.1:%d/v1/credentials", PORT);
  private static final String CATALOG_URI = String.format("http://127.0.0.1:%d/v1/", PORT);
  private static final Map<String, String> PROPERTIES =
      ImmutableMap.of(
          GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
          CREDENTIALS_URI,
          CatalogProperties.URI,
          CATALOG_URI);
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
  public void invalidOrMissingUri() {
    assertThatThrownBy(
            () ->
                OAuth2RefreshCredentialsHandler.create(
                    ImmutableMap.of(CatalogProperties.URI, CATALOG_URI)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid credentials endpoint: null");

    assertThatThrownBy(
            () ->
                OAuth2RefreshCredentialsHandler.create(
                    ImmutableMap.of(
                        GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT, CREDENTIALS_URI)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid catalog endpoint: null");

    assertThatThrownBy(
            () ->
                OAuth2RefreshCredentialsHandler.create(
                        ImmutableMap.of(
                            GCPProperties.GCS_OAUTH2_REFRESH_CREDENTIALS_ENDPOINT,
                            "invalid uri",
                            CatalogProperties.URI,
                            CATALOG_URI))
                    .refreshAccessToken())
        .isInstanceOf(RESTException.class)
        .hasMessageStartingWith(
            "Failed to create request URI from base %sinvalid uri", CATALOG_URI);
  }

  @Test
  public void badRequest() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    HttpResponse mockResponse = HttpResponse.response().withStatusCode(400);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    assertThatThrownBy(handler::refreshAccessToken)
        .isInstanceOf(BadRequestException.class)
        .hasMessageStartingWith("Malformed request");
  }

  @Test
  public void noGcsCredentialInResponse() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    HttpResponse mockResponse =
        HttpResponse.response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    assertThatThrownBy(handler::refreshAccessToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid GCS Credentials: empty");
  }

  @Test
  public void noGcsToken() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    Credential credential =
        ImmutableCredential.builder()
            .prefix("gs")
            .config(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT, "1000"))
            .build();
    HttpResponse mockResponse =
        HttpResponse.response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    assertThatThrownBy(handler::refreshAccessToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid GCS Credentials: gcs.oauth2.token not set");
  }

  @Test
  public void tokenWithoutExpiration() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    Credential credential =
        ImmutableCredential.builder()
            .prefix("gs")
            .config(ImmutableMap.of(GCPProperties.GCS_OAUTH2_TOKEN, "gcsToken"))
            .build();
    HttpResponse mockResponse =
        HttpResponse.response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    assertThatThrownBy(handler::refreshAccessToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid GCS Credentials: gcs.oauth2.token-expires-at not set");
  }

  @Test
  public void tokenWithExpiration() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    Credential credential =
        ImmutableCredential.builder()
            .prefix("gs")
            .config(
                ImmutableMap.of(
                    GCPProperties.GCS_OAUTH2_TOKEN,
                    "gcsToken",
                    GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                    Long.toString(Instant.now().plus(5, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    HttpResponse mockResponse =
        HttpResponse.response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    AccessToken accessToken = handler.refreshAccessToken();
    assertThat(accessToken.getTokenValue())
        .isEqualTo(credential.config().get(GCPProperties.GCS_OAUTH2_TOKEN));
    assertThat(accessToken.getExpirationTime().toInstant().toEpochMilli())
        .isEqualTo(
            Long.parseLong(credential.config().get(GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT)));

    // refresh always fetches a new token
    AccessToken refreshedToken = handler.refreshAccessToken();
    assertThat(refreshedToken).isNotSameAs(accessToken);

    mockServer.verify(mockRequest, VerificationTimes.exactly(2));
  }

  @Test
  public void multipleGcsCredentials() {
    HttpRequest mockRequest =
        HttpRequest.request("/v1/credentials").withMethod(HttpMethod.GET.name());

    Credential credentialOne =
        ImmutableCredential.builder()
            .prefix("gs")
            .config(
                ImmutableMap.of(
                    GCPProperties.GCS_OAUTH2_TOKEN,
                    "gcsToken1",
                    GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                    Long.toString(Instant.now().plus(1, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    Credential credentialTwo =
        ImmutableCredential.builder()
            .prefix("gs://my-custom-prefix/xyz/long-prefix")
            .config(
                ImmutableMap.of(
                    GCPProperties.GCS_OAUTH2_TOKEN,
                    "gcsToken2",
                    GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                    Long.toString(Instant.now().plus(2, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    Credential credentialThree =
        ImmutableCredential.builder()
            .prefix("gs://my-custom-prefix/xyz")
            .config(
                ImmutableMap.of(
                    GCPProperties.GCS_OAUTH2_TOKEN,
                    "gcsToken3",
                    GCPProperties.GCS_OAUTH2_TOKEN_EXPIRES_AT,
                    Long.toString(Instant.now().plus(3, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    HttpResponse mockResponse =
        HttpResponse.response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder()
                        .addCredentials(credentialOne, credentialTwo, credentialThree)
                        .build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    OAuth2RefreshCredentialsHandler handler = OAuth2RefreshCredentialsHandler.create(PROPERTIES);

    assertThatThrownBy(handler::refreshAccessToken)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Invalid GCS Credentials: only one GCS credential should exist");
  }
}
