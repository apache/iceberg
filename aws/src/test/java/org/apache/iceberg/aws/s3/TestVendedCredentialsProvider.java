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
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;
import org.mockserver.verify.VerificationTimes;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

public class TestVendedCredentialsProvider {

  private static final int PORT = 3232;
  private static final String URI = String.format("http://127.0.0.1:%d/v1/credentials", PORT);
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
        .hasMessage("Invalid URI: null");

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(
            ImmutableMap.of(VendedCredentialsProvider.URI, "invalid uri"))) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(RESTException.class)
          .hasMessageStartingWith("Failed to create request URI from base invalid uri");
    }
  }

  @Test
  public void noS3CredentialsInResponse() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());

    HttpResponse mockResponse =
        response(
                LoadCredentialsResponseParser.toJson(
                    ImmutableLoadCredentialsResponse.builder().build()))
            .withStatusCode(200);
    mockServer.when(mockRequest).respond(mockResponse);

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: empty");
    }
  }

  @Test
  public void accessKeyIdAndSecretAccessKeyWithoutToken() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
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

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: s3.session-token not set");
    }
  }

  @Test
  public void expirationNotSet() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
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

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
      assertThatThrownBy(provider::resolveCredentials)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("Invalid S3 Credentials: s3.session-token-expires-at-ms not set");
    }
  }

  @Test
  public void nonExpiredToken() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
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
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
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
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
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

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
      AwsCredentials awsCredentials = provider.resolveCredentials();
      verifyCredentials(awsCredentials, credential);

      // resolving credentials multiple times should hit the credentials endpoint again
      AwsCredentials refreshedCredentials = provider.resolveCredentials();
      assertThat(refreshedCredentials).isNotSameAs(awsCredentials);
      // TODO
      verifyCredentials(refreshedCredentials, credential);
    }

    mockServer.verify(mockRequest, VerificationTimes.exactly(2));
  }

  @Test
  public void credentialWithLongestPrefixIsUsed() {
    HttpRequest mockRequest = request("/v1/credentials").withMethod(HttpMethod.GET.name());
    Credential credentialOne =
        ImmutableCredential.builder()
            .prefix("s3")
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

    try (VendedCredentialsProvider provider =
        VendedCredentialsProvider.create(ImmutableMap.of(VendedCredentialsProvider.URI, URI))) {
      AwsCredentials awsCredentials = provider.resolveCredentials();
      verifyCredentials(awsCredentials, credentialTwo);
    }
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
