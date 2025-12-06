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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.HttpMethod;
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
import org.mockserver.model.Header;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class TestVendedCredentialsProvider {

  private static final int PORT = 3232;
  private static final String CREDENTIALS_URI =
      String.format("http://127.0.0.1:%d/v1/credentials", PORT);
  private static final String CATALOG_URI = String.format("http://127.0.0.1:%d/v1", PORT);
  private static final String HEADER_NAME = "test-header";
  private static final String HEADER_VALUE = "test-value";
  private static final Header TEST_HEADER = Header.header(HEADER_NAME, HEADER_VALUE);
  private static final ImmutableMap<String, String> PROPERTIES =
      ImmutableMap.of(
          VendedCredentialsProvider.URI,
          CREDENTIALS_URI,
          CatalogProperties.URI,
          CATALOG_URI,
          "header." + HEADER_NAME,
          HEADER_VALUE);
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
    assertThatThrownBy(() -> VendedCredentialsProvider.create(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid properties: null");
    assertThatThrownBy(() -> VendedCredentialsProvider.create(ImmutableMap.of()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid credentials endpoint: null");
    assertThatThrownBy(
            () ->
                VendedCredentialsProvider.create(
                    ImmutableMap.of("credentials.uri", "/credentials/uri")))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid catalog endpoint: null");

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(
            ImmutableMap.of(
                VendedCredentialsProvider.URI,
                "/credentials/uri",
                CatalogProperties.URI,
                "invalid catalog uri"))) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(RESTException.class)
          .hasMessageStartingWith("Failed to create request URI from base invalid catalog uri");
    }
  }

  @Test
  public void noS3Credentials() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);

    HttpResponse mockResponse =
        response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: empty");
    }
  }

  @Test
  public void accessKeyIdAndSecretAccessKeyWithoutToken() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3")
                    .config(
                        ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID,
                            "randomAccessKey",
                            S3FileIOProperties.SECRET_ACCESS_KEY,
                            "randomSecretAccessKey"))
                    .build())
            .build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: s3.session-token not set");
    }
  }

  @Test
  public void expirationNotSet() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(
                ImmutableCredential.builder()
                    .prefix("s3")
                    .config(
                        ImmutableMap.of(
                            S3FileIOProperties.ACCESS_KEY_ID,
                            "randomAccessKey",
                            S3FileIOProperties.SECRET_ACCESS_KEY,
                            "randomSecretAccessKey",
                            S3FileIOProperties.SESSION_TOKEN,
                            "sessionToken"))
                    .build())
            .build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: s3.session-token-expires-at-ms not set");
    }
  }

  @Test
  public void nonExpiredToken() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);
    Credential credential =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      AwsCredentials awsCredentials = provider.resolveCredentials();

      verifyCredentials(awsCredentials, credential);

      for (int i = 0; i < 5; i++) {
        // resolving credentials multiple times should not hit the credentials endpoint again
        assertThat(provider.resolveCredentials()).isSameAs(awsCredentials);
      }
    }

    mockServer.verify(mockRequest, VerificationTimes.once());
  }

  @Test
  public void expiredToken() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);
    Credential credential =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().minus(1, ChronoUnit.MINUTES).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      AwsCredentials awsCredentials = provider.resolveCredentials();
      verifyCredentials(awsCredentials, credential);

      // resolving credentials multiple times should hit the credentials endpoint again
      AwsCredentials refreshedCredentials = provider.resolveCredentials();
      assertThat(refreshedCredentials).isNotSameAs(awsCredentials);
      verifyCredentials(refreshedCredentials, credential);
    }

    mockServer.verify(mockRequest, VerificationTimes.exactly(2));
  }

  @Test
  public void multipleS3Credentials() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);
    Credential credentialOne =
        ImmutableCredential.builder()
            .prefix("gcs")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey1",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey1",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken1",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    Credential credentialTwo =
        ImmutableCredential.builder()
            .prefix("s3://custom-uri/longest-prefix")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey2",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey2",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken2",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(2, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    Credential credentialThree =
        ImmutableCredential.builder()
            .prefix("s3://custom-uri/long")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey3",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey3",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken3",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(3, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder()
            .addCredentials(credentialOne, credentialTwo, credentialThree)
            .build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider = VendedCredentialsProvider.create(PROPERTIES)) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: only one S3 credential should exist");
    }
  }

  @Test
  public void nonExpiredTokenInProperties() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    String expiresAt = Long.toString(Instant.now().plus(10, ChronoUnit.HOURS).toEpochMilli());
    Credential credentialFromProperties =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKeyFromProperties",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKeyFromProperties",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionTokenFromProperties",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    expiresAt))
            .build();

    Credential credential =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(
            ImmutableMap.of(
                CatalogProperties.URI,
                CATALOG_URI,
                VendedCredentialsProvider.URI,
                CREDENTIALS_URI,
                S3FileIOProperties.ACCESS_KEY_ID,
                "randomAccessKeyFromProperties",
                S3FileIOProperties.SECRET_ACCESS_KEY,
                "randomSecretAccessKeyFromProperties",
                S3FileIOProperties.SESSION_TOKEN,
                "sessionTokenFromProperties",
                S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                expiresAt))) {
      AwsCredentials awsCredentials = provider.resolveCredentials();

      verifyCredentials(awsCredentials, credentialFromProperties);

      for (int i = 0; i < 5; i++) {
        // resolving credentials multiple times should not hit the credentials endpoint again
        assertThat(provider.resolveCredentials()).isSameAs(awsCredentials);
      }
    }

    // token endpoint isn't hit, because the credentials are extracted from the properties
    mockServer.verify(mockRequest, VerificationTimes.never());
  }

  @Test
  public void expiredTokenInProperties() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);

    Credential credential =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(
            ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, CATALOG_URI)
                .put(VendedCredentialsProvider.URI, CREDENTIALS_URI)
                .put("header." + HEADER_NAME, HEADER_VALUE)
                .put(S3FileIOProperties.ACCESS_KEY_ID, "randomAccessKeyFromProperties")
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, "randomSecretAccessKeyFromProperties")
                .put(S3FileIOProperties.SESSION_TOKEN, "sessionTokenFromProperties")
                .put(
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().minus(1, ChronoUnit.HOURS).toEpochMilli()))
                .build())) {
      AwsCredentials awsCredentials = provider.resolveCredentials();

      verifyCredentials(awsCredentials, credential);

      for (int i = 0; i < 5; i++) {
        // resolving credentials multiple times should not hit the credentials endpoint again
        assertThat(provider.resolveCredentials()).isSameAs(awsCredentials);
      }
    }

    // token endpoint is hit once due to the properties containing an expired token
    mockServer.verify(mockRequest, VerificationTimes.once());
  }

  @Test
  public void invalidTokenInProperties() {
    HttpRequest mockRequest =
        request("/v1/credentials").withMethod(HttpMethod.GET.name()).withHeader(TEST_HEADER);

    Credential credential =
        ImmutableCredential.builder()
            .prefix("s3")
            .config(
                ImmutableMap.of(
                    S3FileIOProperties.ACCESS_KEY_ID,
                    "randomAccessKey",
                    S3FileIOProperties.SECRET_ACCESS_KEY,
                    "randomSecretAccessKey",
                    S3FileIOProperties.SESSION_TOKEN,
                    "sessionToken",
                    S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS,
                    Long.toString(Instant.now().plus(1, ChronoUnit.HOURS).toEpochMilli())))
            .build();
    LoadCredentialsResponse response =
        ImmutableLoadCredentialsResponse.builder().addCredentials(credential).build();

    HttpResponse mockResponse =
        response(LoadCredentialsResponseParser.toJson(response)).withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    // token expiration is missing from the properties
    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(
            ImmutableMap.<String, String>builder()
                .put(CatalogProperties.URI, CATALOG_URI)
                .put(VendedCredentialsProvider.URI, CREDENTIALS_URI)
                .put("header." + HEADER_NAME, HEADER_VALUE)
                .put(S3FileIOProperties.ACCESS_KEY_ID, "randomAccessKeyFromProperties")
                .put(S3FileIOProperties.SECRET_ACCESS_KEY, "randomSecretAccessKeyFromProperties")
                .put(S3FileIOProperties.SESSION_TOKEN, "sessionTokenFromProperties")
                .build())) {
      AwsCredentials awsCredentials = provider.resolveCredentials();

      verifyCredentials(awsCredentials, credential);

      for (int i = 0; i < 5; i++) {
        // resolving credentials multiple times should not hit the credentials endpoint again
        assertThat(provider.resolveCredentials()).isSameAs(awsCredentials);
      }
    }

    // token endpoint is hit once due to the properties not containing the token's expiration
    mockServer.verify(mockRequest, VerificationTimes.once());
  }

  private void verifyCredentials(AwsCredentials awsCredentials, Credential credential) {
    assertThat(awsCredentials).isInstanceOf(AwsSessionCredentials.class);
    AwsSessionCredentials creds = (AwsSessionCredentials) awsCredentials;

    assertThat(creds.accessKeyId())
        .isEqualTo(credential.config().get(S3FileIOProperties.ACCESS_KEY_ID));
    assertThat(creds.secretAccessKey())
        .isEqualTo(credential.config().get(S3FileIOProperties.SECRET_ACCESS_KEY));
    assertThat(creds.sessionToken())
        .isEqualTo(credential.config().get(S3FileIOProperties.SESSION_TOKEN));
    assertThat(creds.expirationTime())
        .isPresent()
        .get()
        .extracting(Instant::toEpochMilli)
        .isEqualTo(
            Long.parseLong(
                credential.config().get(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS)));
  }
}
