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

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthProperties;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.OAuth2Util;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mockito;
import software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider;
import software.amazon.awssdk.auth.signer.AwsSignerExecutionAttribute;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.http.SdkHttpFullRequest;
import software.amazon.awssdk.http.SdkHttpMethod;
import software.amazon.awssdk.regions.Region;
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
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/namespaces/ns1/tables/t1/sign"),
            "sign",
            null),
        // Token only
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/namespaces/ns1/tables/t1/sign",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.TOKEN,
                "token"),
            "sign",
            "token"),
        // Credential only: expect a token to be fetched
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/namespaces/ns1/tables/t1/sign",
                AuthProperties.AUTH_TYPE,
                AuthProperties.AUTH_TYPE_OAUTH2,
                OAuth2Properties.CREDENTIAL,
                "user:12345"),
            "sign",
            "token"),
        // Token and credential: should use token as is, not fetch a new one
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/namespaces/ns1/tables/t1/sign",
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
                CatalogProperties.URI,
                "https://signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/namespaces/ns1/tables/t1/sign",
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
      Map<String, String> properties, String expectedBaseSignerUri, String expectedEndpoint)
      throws Exception {
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
                RESTCatalogProperties.SIGNER_URI,
                "https://legacy-signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/legacy/sign"),
            "https://legacy-signer.com",
            "https://legacy-signer.com/v1/legacy/sign"),
        // Only new properties
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://new-signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/new/sign"),
            "https://new-signer.com",
            "https://new-signer.com/v1/new/sign"),
        // Mixed properties: legacy properties take precedence
        Arguments.of(
            Map.of(
                CatalogProperties.URI,
                "https://new-signer.com",
                RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
                "v1/new/sign",
                RESTCatalogProperties.SIGNER_URI,
                "https://legacy-signer.com",
                RESTCatalogProperties.SIGNER_ENDPOINT,
                "v1/legacy/sign"),
            "https://legacy-signer.com",
            "https://legacy-signer.com/v1/legacy/sign"));
  }

  @Test
  void doesNotReuseSignedComponentAcrossSignerClients() throws Exception {
    Map<String, String> properties1 =
        Map.of(
            CatalogProperties.URI,
            "https://signer.com",
            RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
            "v1/sign",
            OAuth2Properties.TOKEN,
            "token1");
    Map<String, String> properties2 =
        Map.of(
            CatalogProperties.URI,
            "https://signer.com",
            RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
            "v1/sign",
            OAuth2Properties.TOKEN,
            "token2");

    URI requestUri = URI.create("https://bucket.s3.us-west-2.amazonaws.com/object-key");
    SdkHttpFullRequest request =
        SdkHttpFullRequest.builder()
            .method(SdkHttpMethod.GET)
            .uri(requestUri)
            .protocol("https")
            .host("bucket.s3.us-west-2.amazonaws.com")
            .encodedPath("/object-key")
            .build();

    ExecutionAttributes executionAttributes = new ExecutionAttributes();
    executionAttributes.putAttribute(AwsSignerExecutionAttribute.SERVICE_SIGNING_NAME, "s3");
    executionAttributes.putAttribute(AwsSignerExecutionAttribute.SIGNING_REGION, Region.US_WEST_2);
    executionAttributes.putAttribute(
        AwsSignerExecutionAttribute.AWS_CREDENTIALS,
        AnonymousCredentialsProvider.create().resolveCredentials());

    AtomicInteger callCount = new AtomicInteger();
    when(S3V4RestSignerClient.httpClient.post(
            Mockito.eq("https://signer.com/v1/sign"),
            Mockito.any(RemoteSignRequest.class),
            Mockito.eq(RemoteSignResponse.class),
            Mockito.anyMap(),
            Mockito.any(),
            Mockito.any()))
        .thenAnswer(
            invocation -> {
              int count = callCount.incrementAndGet();
              Consumer<Map<String, String>> headersConsumer = invocation.getArgument(5);
              if (headersConsumer != null) {
                headersConsumer.accept(Map.of("Cache-Control", "private"));
              }
              return ImmutableRemoteSignResponse.builder()
                  .uri(requestUri)
                  .headers(Map.of("Authorization", List.of("AWS4-HMAC-SHA256 sig-client-" + count)))
                  .build();
            });

    try (S3V4RestSignerClient client1 =
            ImmutableS3V4RestSignerClient.builder().properties(properties1).build();
        S3V4RestSignerClient client2 =
            ImmutableS3V4RestSignerClient.builder().properties(properties2).build()) {

      // Client 1 makes request: cache miss, calls signer, response is cached
      SdkHttpFullRequest signed1 = client1.sign(request, executionAttributes);
      assertThat(signed1.headers().get("Authorization"))
          .containsExactly("AWS4-HMAC-SHA256 sig-client-1");
      assertThat(callCount.get()).isEqualTo(1);

      // Client 1 repeated request: cache hit, does not call signer again
      SdkHttpFullRequest signed1Repeat = client1.sign(request, executionAttributes);
      assertThat(signed1Repeat.headers().get("Authorization"))
          .containsExactly("AWS4-HMAC-SHA256 sig-client-1");
      assertThat(callCount.get()).isEqualTo(1);

      // Client 2 makes the same request: must NOT reuse Client 1's cached signature.
      // It must call signer and receive Client 2's own signature.
      SdkHttpFullRequest signed2 = client2.sign(request, executionAttributes);
      assertThat(signed2.headers().get("Authorization"))
          .containsExactly("AWS4-HMAC-SHA256 sig-client-2");
      assertThat(callCount.get()).isEqualTo(2);

      // Client 2 repeated request: cache hit on client 2's cache
      SdkHttpFullRequest signed2Repeat = client2.sign(request, executionAttributes);
      assertThat(signed2Repeat.headers().get("Authorization"))
          .containsExactly("AWS4-HMAC-SHA256 sig-client-2");
      assertThat(callCount.get()).isEqualTo(2);

      // Verify structural isolation of the cache instances
      assertThat(client1.signedComponentCache()).isNotSameAs(client2.signedComponentCache());
    }
  }
}
