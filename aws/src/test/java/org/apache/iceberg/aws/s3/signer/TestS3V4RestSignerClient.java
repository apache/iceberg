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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalogInternalProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.signing.ImmutableRemoteSigningConfig;
import org.apache.iceberg.rest.signing.RemoteSigningConfig;
import org.apache.iceberg.rest.signing.RemoteSigningConfigParser;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import software.amazon.awssdk.utils.IoUtils;

class TestS3V4RestSignerClient {

  // A valid encoded table identifier (ns.t with %1F separator) used in test properties
  private static final String TABLE_ID = "ns%1Ft";

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
        Arguments.of(
            Map.of(
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
                RESTCatalogProperties.SIGNER_URI,
                "https://signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/sign/s3"),
            "sign",
            null),
        // Token only
        Arguments.of(
            Map.of(
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
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
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
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
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
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
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
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
  @MethodSource("signerUriResolution")
  void signerUriResolution(
      Map<String, String> properties, String expectedBaseSignerUri, String expectedEndpoint)
      throws Exception {
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder().properties(properties).build()) {
      assertThat(client.baseSignerUri()).isEqualTo(expectedBaseSignerUri);
      assertThat(client.endpoint()).isEqualTo(expectedEndpoint);
    }
  }

  public static Stream<Arguments> signerUriResolution() {
    return Stream.of(
        // Signer URI + explicit endpoint
        Arguments.of(
            Map.of(
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
                CatalogProperties.URI,
                "https://catalog.com",
                RESTCatalogProperties.SIGNER_URI,
                "https://new-signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/new/sign"),
            "https://new-signer.com",
            "https://new-signer.com/v1/new/sign"),
        // No signer URI: the catalog URI is used as base
        Arguments.of(
            Map.of(
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
                CatalogProperties.URI,
                "https://catalog.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/tables/t/sign"),
            "https://catalog.com",
            "https://catalog.com/v1/tables/t/sign"),
        // No explicit endpoint: derived from TABLE_IDENTIFIER (ns.t →
        // v1/namespaces/ns/tables/t/sign)
        Arguments.of(
            Map.of(
                RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                TABLE_ID,
                CatalogProperties.URI,
                "https://catalog.com"),
            "https://catalog.com",
            "https://catalog.com/v1/namespaces/ns/tables/t/sign"));
  }

  @Test
  void tableIdentifierIsRequired() {
    assertThatThrownBy(
            () ->
                ImmutableS3V4RestSignerClient.builder()
                    .properties(Map.of(CatalogProperties.URI, "https://catalog.com"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Table identifier is required");
  }

  @Test
  void signerUriIsRequired() {
    assertThatThrownBy(
            () ->
                ImmutableS3V4RestSignerClient.builder()
                    .properties(Map.of(RESTCatalogInternalProperties.TABLE_IDENTIFIER, TABLE_ID))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("S3 signer service URI is required");
  }

  @Test
  void remoteSigningConfigDefaultEmpty() throws Exception {
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com"))
            .build()) {
      assertThat(client.remoteSigningConfig()).isEqualTo(RemoteSigningConfig.EMPTY);
      assertThat(client.requestPropertiesSupplier().get()).isEmpty();
    }
  }

  @Test
  void remoteSigningConfigFromProperty() throws Exception {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("prop1", "val1")
            .putHeaders("Authorization", List.of("Bearer token123"))
            .build();

    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com",
                    RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG,
                    RemoteSigningConfigParser.toJson(config)))
            .build()) {
      assertThat(client.remoteSigningConfig().properties()).containsEntry("prop1", "val1");
      assertThat(client.remoteSigningConfig().headers().get("Authorization"))
          .containsExactly("Bearer token123");
      // requestPropertiesSupplier returns config.properties()
      assertThat(client.requestPropertiesSupplier().get()).containsEntry("prop1", "val1");
    }
  }

  @Test
  void signingEndpointHeadersEmptyWhenNoHeaders() throws Exception {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().putProperties("k", "v").build();

    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com",
                    RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG,
                    RemoteSigningConfigParser.toJson(config)))
            .build()) {
      // headers are empty in config, so signing endpoint headers should be empty
      assertThat(client.remoteSigningConfig().headers()).isEmpty();
    }
  }

  @Test
  void signingEndpointHeadersFromConfig() throws Exception {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putHeaders("Authorization", List.of("Bearer tok"))
            .putHeaders("X-Multi", List.of("a", "b"))
            .build();

    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com",
                    RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG,
                    RemoteSigningConfigParser.toJson(config)))
            .build()) {
      Map<String, List<String>> headers = client.remoteSigningConfig().headers();
      assertThat(headers.get("Authorization")).containsExactly("Bearer tok");
      // multi-value headers are comma-joined when sent to signing endpoint
      assertThat(String.join(", ", headers.get("X-Multi"))).isEqualTo("a, b");
    }
  }

  @Test
  void requestPropertiesSupplierOverride() throws Exception {
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com"))
            .requestPropertiesSupplier(() -> Map.of("custom", "override"))
            .build()) {
      assertThat(client.requestPropertiesSupplier().get()).containsEntry("custom", "override");
    }
  }

  @Test
  void deprecatedSignerUriWarns() throws Exception {
    // SIGNER_URI is deprecated — client still constructs, just logs a warning
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com",
                    RESTCatalogProperties.SIGNER_URI,
                    "https://custom-signer.com"))
            .build()) {
      assertThat(client.baseSignerUri()).isEqualTo("https://custom-signer.com");
    }
  }

  @Test
  void requestPropertiesSupplierDefaultUsesConfigProperties() throws Exception {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().putProperties("signer-prop", "signer-val").build();
    try (S3V4RestSignerClient client =
        ImmutableS3V4RestSignerClient.builder()
            .properties(
                Map.of(
                    RESTCatalogInternalProperties.TABLE_IDENTIFIER,
                    TABLE_ID,
                    CatalogProperties.URI,
                    "https://catalog.com",
                    RESTCatalogInternalProperties.REMOTE_SIGNING_CONFIG,
                    RemoteSigningConfigParser.toJson(config)))
            .build()) {
      assertThat(client.requestPropertiesSupplier().get())
          .containsExactlyEntriesOf(Collections.singletonMap("signer-prop", "signer-val"));
    }
  }
}
