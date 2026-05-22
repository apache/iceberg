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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Manager;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;

class TestS3V4RestSignerClient {

  private static final Map<String, String> SIGNER_PROPERTIES =
      Map.of(
          RESTCatalogProperties.SIGNER_URI,
          "https://signer.com",
          RESTCatalogProperties.SIGNER_ENDPOINT,
          "v1/sign/s3");

  @ParameterizedTest
  @MethodSource("validOAuth2Properties")
  void authSessionOAuth2(
      Map<String, String> properties, String expectedScope, String expectedToken) {
    RESTClient mockHttpClient = mockHttpClient();
    try (S3V4RestSignerClient client =
            ImmutableS3V4RestSignerClient.builder()
                .properties(properties)
                .httpClientSupplier(() -> mockHttpClient)
                .build();
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
        Arguments.of(
            Map.of(
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3"),
            "sign",
            null),
        // Token only
        Arguments.of(
            Map.of(
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.TOKEN,
                "token"),
            "sign",
            "token"),
        // Credential only: expect a token to be fetched
        Arguments.of(
            Map.of(
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.CREDENTIAL,
                "user:12345"),
            "sign",
            "token"),
        // Token and credential: should use token as is, not fetch a new one
        Arguments.of(
            Map.of(
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3",
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
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.CREDENTIAL,
                "user:12345",
                OAuth2Properties.SCOPE,
                "custom"),
            "custom",
            "token"));
  }

  @ParameterizedTest
  @MethodSource("legacySignerProperties")
  void legacySignerProperties(
      Map<String, String> properties, String expectedBaseSignerUri, String expectedEndpoint) {
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder().properties(properties).build()) {
      assertThat(client.baseSignerUri()).isEqualTo(expectedBaseSignerUri);
      assertThat(client.endpoint()).isEqualTo(expectedEndpoint);
    }
  }

  @SuppressWarnings("deprecation")
  public static Stream<Arguments> legacySignerProperties() {
    return Stream.of(
        // Only legacy properties
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://catalog.com",
                S3V4RestSignerClient.S3_SIGNER_URI,
                "https://legacy-signer.com",
                S3V4RestSignerClient.S3_SIGNER_ENDPOINT,
                "v1/legacy/sign"),
            "https://legacy-signer.com",
            "https://legacy-signer.com/v1/legacy/sign"),
        // Only new properties
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://catalog.com",
                RESTCatalogProperties.SIGNER_URI,
                "https://new-signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/new/sign"),
            "https://new-signer.com",
            "https://new-signer.com/v1/new/sign"),
        // Mixed properties: legacy properties take precedence
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://catalog.com",
                RESTCatalogProperties.SIGNER_URI,
                "https://new-signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/new/sign",
                S3V4RestSignerClient.S3_SIGNER_URI,
                "https://legacy-signer.com",
                S3V4RestSignerClient.S3_SIGNER_ENDPOINT,
                "v1/legacy/sign"),
            "https://legacy-signer.com",
            "https://legacy-signer.com/v1/legacy/sign"),
        // No signer properties: the catalog URI and the deprecated default endpoint are used
        Arguments.of(
            Map.of(CatalogProperties.URI, "https://catalog.com"),
            "https://catalog.com",
            "https://catalog.com/" + S3V4RestSignerClient.S3_SIGNER_DEFAULT_ENDPOINT));
  }

  @Test
  void httpClientNotSharedAcrossInstances() {
    RESTClient firstHttpClient = Mockito.mock(RESTClient.class);
    RESTClient secondHttpClient = Mockito.mock(RESTClient.class);
    try (S3V4RestSignerClient first =
            ImmutableS3V4RestSignerClient.builder()
                .properties(SIGNER_PROPERTIES)
                .httpClientSupplier(() -> firstHttpClient)
                .build();
        S3V4RestSignerClient second =
            ImmutableS3V4RestSignerClient.builder()
                .properties(SIGNER_PROPERTIES)
                .httpClientSupplier(() -> secondHttpClient)
                .build()) {
      // each signer must use its own client, not the first one initialized in the JVM
      assertThat(first.httpClient()).isSameAs(firstHttpClient);
      assertThat(second.httpClient()).isSameAs(secondHttpClient);
      assertThat(first.httpClient()).isNotSameAs(second.httpClient());
    }
  }

  @Test
  void authManagerNotSharedAcrossInstances() {
    Map<String, String> oauth2Properties =
        Map.of(
            RESTCatalogProperties.SIGNER_URI,
            "https://signer.com",
            RESTCatalogProperties.SIGNER_ENDPOINT,
            "v1/sign/s3",
            AuthProperties.AUTH_TYPE,
            AuthProperties.AUTH_TYPE_OAUTH2,
            OAuth2Properties.TOKEN,
            "token");
    try (S3V4RestSignerClient noAuth =
            ImmutableS3V4RestSignerClient.builder().properties(SIGNER_PROPERTIES).build();
        S3V4RestSignerClient oauth2 =
            ImmutableS3V4RestSignerClient.builder().properties(oauth2Properties).build()) {
      AuthManager noAuthManager = noAuth.authManager();
      AuthManager oauth2Manager = oauth2.authManager();
      // a catalog configured for OAuth2 must not leak its auth manager into a catalog that
      // configured no auth (and vice versa)
      assertThat(noAuthManager).isNotSameAs(oauth2Manager);
      assertThat(oauth2Manager).isInstanceOf(OAuth2Manager.class);
      assertThat(noAuthManager).isNotInstanceOf(OAuth2Manager.class);
    }
  }

  @Test
  void closeReleasesHttpClient() throws Exception {
    RESTClient mockHttpClient = Mockito.mock(RESTClient.class);
    S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(SIGNER_PROPERTIES)
            .httpClientSupplier(() -> mockHttpClient)
            .build();
    assertThat(client.httpClient()).isSameAs(mockHttpClient);
    client.close();
    Mockito.verify(mockHttpClient).close();
  }

  private static RESTClient mockHttpClient() {
    RESTClient mock = Mockito.mock(RESTClient.class);
    when(mock.withAuthSession(Mockito.any())).thenReturn(mock);
    when(mock.postForm(
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
    when(mock.postForm(
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
    return mock;
  }
}
