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
package org.apache.iceberg.aws.s3.signer;

import static org.apache.iceberg.aws.s3.signer.S3V4RestSignerClient.S3_SIGNER_URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import software.amazon.awssdk.utils.IoUtils;

class TestS3V4RestSignerClient {

  @BeforeAll
  static void beforeAll() {
    S3V4RestSignerClient.authManager = null;
    S3V4RestSignerClient.httpClient = Mockito.mock(RESTClient.class);
    when(S3V4RestSignerClient.httpClient.withAuthSession(Mockito.any()))
        .thenReturn(S3V4RestSignerClient.httpClient);
    when(S3V4RestSignerClient.httpClient.postForm(
            Mockito.anyString(),
            Mockito.eq(
                Map.of(
                    "grant_type",
                    "client_credentials",
                    "client_id",
                    "user",
                    "client_secret",
                    "12345",
                    "scope",
                    "sign")),
            Mockito.eq(OAuthTokenResponse.class),
            Mockito.anyMap(),
            Mockito.any()))
        .thenReturn(
            OAuthTokenResponse.builder().withToken("token").withTokenType("Bearer").build());
    when(S3V4RestSignerClient.httpClient.postForm(
            Mockito.anyString(),
            Mockito.eq(
                Map.of(
                    "grant_type",
                    "client_credentials",
                    "client_id",
                    "user",
                    "client_secret",
                    "12345",
                    "scope",
                    "custom")),
            Mockito.eq(OAuthTokenResponse.class),
            Mockito.anyMap(),
            Mockito.any()))
        .thenReturn(
            OAuthTokenResponse.builder().withToken("token").withTokenType("Bearer").build());
  }

  @AfterAll
  static void afterAll() {
    S3V4RestSignerClient.httpClient = null;
  }

  @AfterEach
  void afterEach() {
    IoUtils.closeQuietlyV2(S3V4RestSignerClient.authManager, null);
    S3V4RestSignerClient.authManager = null;
  }

  @ParameterizedTest
  @MethodSource("validOAuth2Properties")
  void authSessionOAuth2(Map<String, String> properties, String expectedScope, String expectedToken)
      throws Exception {
    try (S3V4RestSignerClient client =
            ImmutableS3V4RestSignerClient.builder().properties(properties).build();
        AuthSession authSession = client.authSession()) {
      assertThat(client.optionalOAuthParams()).containsEntry(OAuth2Properties.SCOPE, expectedScope);
      if (expectedToken == null) {
        assertThat(authSession).isInstanceOf(AuthSession.class);
      } else {
        assertThat(authSession)
            .asInstanceOf(type(OAuth2Util.AuthSession.class))
            .extracting(OAuth2Util.AuthSession::headers)
            .satisfies(
                headers ->
                    assertThat(headers).containsEntry("Authorization", "Bearer " + expectedToken));
      }
    }
  }

  public static Stream<Arguments> validOAuth2Properties() {
    return Stream.of(
        // No OAuth2 data
        Arguments.of(Map.of(S3_SIGNER_URI, "https://signer.com"), "sign", null),
        // Token only
        Arguments.of(
            Map.of(
                S3_SIGNER_URI,
                "https://signer.com",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.TOKEN,
                "token"),
            "sign",
            "token"),
        // Credential only: expect a token to be fetched
        Arguments.of(
            Map.of(
                S3_SIGNER_URI,
                "https://signer.com",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.CREDENTIAL,
                "user:12345"),
            "sign",
            "token"),
        // Token and credential: should use token as is, not fetch a new one
        Arguments.of(
            Map.of(
                S3_SIGNER_URI,
                "https://signer.com",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.TOKEN,
                "token",
                OAuth2Properties.CREDENTIAL,
                "user:12345"),
            "sign",
            "token"),
        // Custom scope
        Arguments.of(
            Map.of(
                S3_SIGNER_URI,
                "https://signer.com",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.CREDENTIAL,
                "user:12345",
                OAuth2Properties.SCOPE,
                "custom"),
            "custom",
            "token"));
  }
}
