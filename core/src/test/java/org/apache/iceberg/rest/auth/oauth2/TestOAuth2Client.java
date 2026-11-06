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
package org.apache.iceberg.rest.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockserver.matchers.Times;
import org.mockserver.model.Parameter;

class TestOAuth2Client {

  private static MockAuthorizationServer server;

  @BeforeAll
  static void startServer() {
    server = new MockAuthorizationServer();
  }

  @AfterAll
  static void stopServer() {
    server.close();
  }

  @BeforeEach
  void reset() {
    server.reset();
  }

  @ParameterizedTest
  @ValueSource(strings = {"client_secret_basic", "client_secret_post"})
  void clientCredentials(String clientAuth) {
    ClientAuthenticationMethod method = ClientAuthenticationMethod.parse(clientAuth);
    server.expectClientCredentials(method);
    OAuth2Config config = config(Map.of(OAuth2Config.CLIENT_AUTH, clientAuth));
    try (OAuth2Client client = new OAuth2Client(config, server.runtime())) {
      TokensResult tokens = client.currentTokens();
      assertThat(tokens.accessToken().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
      assertThat(tokens.expirationTime())
          .isEqualTo(tokens.receivedAt().plus(MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN));
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
    }

    server.verifyTokenRequests(1);
  }

  @Test
  void clientCredentialsWithDiscovery() {
    server.expectOidcDiscovery();
    server.expectClientCredentials();
    OAuth2Config config = OAuth2Config.fromProperties(server.issuerProperties());
    try (OAuth2Client client = new OAuth2Client(config, server.runtime())) {
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
    }
  }

  @Test
  void resourcesAndAudiences() {
    server.expectClientCredentials(
        MockAuthorizationServer.CLIENT_ID,
        MockAuthorizationServer.CLIENT_SECRET,
        MockAuthorizationServer.SCOPE,
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        MockAuthorizationServer.ACCESS_TOKEN,
        MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN,
        Times.unlimited(),
        Parameter.param("resource", "https://catalog.example.com/api", "https://other.example.com"),
        Parameter.param("audience", "catalog", "storage"),
        Parameter.param("extra", "value"));
    OAuth2Config config =
        config(
            Map.of(
                OAuth2Config.RESOURCE,
                "https://catalog.example.com/api, https://other.example.com",
                OAuth2Config.AUDIENCE,
                "catalog,storage",
                OAuth2Config.EXTRA_PARAMS_PREFIX + "extra",
                "value"));
    try (OAuth2Client client = new OAuth2Client(config, server.runtime())) {
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
    }

    server.verifyTokenRequests(1);
  }

  @Test
  @SuppressWarnings("resource")
  void invalidClientIsNotRetried() {
    server.expectInvalidClient();
    assertThatThrownBy(() -> new OAuth2Client(config(Map.of()), server.runtime()))
        .isInstanceOf(OAuth2Exception.class)
        .hasMessage("OAuth2 token request failed with status 401: Invalid client credentials")
        .satisfies(
            e -> {
              OAuth2Exception oauth2Exception = (OAuth2Exception) e;
              assertThat(oauth2Exception.statusCode()).isEqualTo(401);
              assertThat(oauth2Exception.errorCode()).isEqualTo("invalid_client");
            });
    server.verifyTokenRequests(1);
  }

  @Test
  void serverErrorIsRetried() {
    server.expectServerErrorOnce();
    server.expectClientCredentials();
    try (OAuth2Client client = new OAuth2Client(config(Map.of()), server.runtime())) {
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
    }

    server.verifyTokenRequests(2);
  }

  @Test
  void renewTokens() {
    server.expectClientCredentialsOnce(
        MockAuthorizationServer.ACCESS_TOKEN, MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN);
    server.expectClientCredentials(
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        MockAuthorizationServer.RENEWED_ACCESS_TOKEN,
        MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN);
    try (OAuth2Client client = new OAuth2Client(config(Map.of()), server.runtime())) {
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
      client.renewTokens();
      assertThat(client.authenticate().getValue())
          .isEqualTo(MockAuthorizationServer.RENEWED_ACCESS_TOKEN);
    }

    server.verifyTokenRequests(2);
  }

  @Test
  void renewTokensFailureKeepsCurrentTokens() {
    server.expectClientCredentialsOnce(
        MockAuthorizationServer.ACCESS_TOKEN, MockAuthorizationServer.ACCESS_TOKEN_LIFESPAN);
    server.expectInvalidClient();
    try (OAuth2Client client = new OAuth2Client(config(Map.of()), server.runtime())) {
      TokensResult initialTokens = client.currentTokens();
      client.renewTokens();
      assertThat(client.currentTokens()).isSameAs(initialTokens);
    }

    server.verifyTokenRequests(2);
  }

  @Test
  void scheduledRenewal() {
    server.expectClientCredentialsOnce(MockAuthorizationServer.ACCESS_TOKEN, Duration.ofSeconds(1));
    server.expectClientCredentials(
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        MockAuthorizationServer.RENEWED_ACCESS_TOKEN,
        Duration.ofHours(1));
    OAuth2Config config =
        config(
            Map.of(
                OAuth2Config.TOKEN_REFRESH_PREFETCH, "PT0S",
                OAuth2Config.TOKEN_REFRESH_JITTER, "PT0S"));
    try (OAuth2Client client = new OAuth2Client(config, server.runtime())) {
      assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertThat(client.authenticate().getValue())
                      .isEqualTo(MockAuthorizationServer.RENEWED_ACCESS_TOKEN));
    }
  }

  @Test
  void scheduledRenewalWithoutServerLifetime() {
    server.expectClientCredentialsOnce(MockAuthorizationServer.ACCESS_TOKEN, null);
    server.expectClientCredentials(
        ClientAuthenticationMethod.CLIENT_SECRET_BASIC,
        MockAuthorizationServer.RENEWED_ACCESS_TOKEN,
        Duration.ofHours(1));
    OAuth2Config config =
        config(
            Map.of(
                OAuth2Config.TOKEN_REFRESH_ACCESS_TOKEN_LIFESPAN, "PT1S",
                OAuth2Config.TOKEN_REFRESH_PREFETCH, "PT0S",
                OAuth2Config.TOKEN_REFRESH_JITTER, "PT0S"));
    try (OAuth2Client client = new OAuth2Client(config, server.runtime())) {
      assertThat(client.currentTokens().expirationTime()).isNull();
      await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () ->
                  assertThat(client.authenticate().getValue())
                      .isEqualTo(MockAuthorizationServer.RENEWED_ACCESS_TOKEN));
    }
  }

  @Test
  void close() {
    server.expectClientCredentials();
    OAuth2Client client = new OAuth2Client(config(Map.of()), server.runtime());
    assertThat(client.authenticate().getValue()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
    client.close();
    client.close(); // idempotent
    assertThatThrownBy(client::authenticate)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("OAuth2 client is closed");
    client.renewTokens(); // no-op
    server.verifyTokenRequests(1);
  }

  @Test
  void closeDuringRenewal() throws Exception {
    server.expectClientCredentials();
    OAuth2Runtime runtime = server.runtime();
    CountDownLatch renewalStarted = new CountDownLatch(1);
    CountDownLatch releaseRenewal = new CountDownLatch(1);
    AtomicInteger requests = new AtomicInteger();
    OAuth2Runtime blockingRuntime =
        new OAuth2Runtime(
            request -> {
              if (requests.incrementAndGet() > 1) {
                renewalStarted.countDown();
                try {
                  assertThat(releaseRenewal.await(30, TimeUnit.SECONDS)).isTrue();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  throw new IOException(e);
                }
              }

              return runtime.httpClient().send(request);
            },
            runtime.executor(),
            runtime.clock());
    try (OAuth2Client client = new OAuth2Client(config(Map.of()), blockingRuntime)) {
      CompletableFuture<Void> renewal = CompletableFuture.runAsync(client::renewTokens);
      try {
        assertThat(renewalStarted.await(30, TimeUnit.SECONDS)).isTrue();
        client.close();
      } finally {
        releaseRenewal.countDown();
      }

      renewal.get(30, TimeUnit.SECONDS);
      assertThatThrownBy(client::authenticate)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("OAuth2 client is closed");
    }
  }

  @Test
  void parallelAuthentication() throws Exception {
    server.expectClientCredentials();
    try (OAuth2Client client = new OAuth2Client(config(Map.of()), server.runtime())) {
      List<CompletableFuture<String>> futures =
          IntStream.range(0, 10)
              .mapToObj(i -> CompletableFuture.supplyAsync(() -> client.authenticate().getValue()))
              .toList();
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);
      for (CompletableFuture<String> future : futures) {
        assertThat(future.get()).isEqualTo(MockAuthorizationServer.ACCESS_TOKEN);
      }
    }

    server.verifyTokenRequests(1);
  }

  private static OAuth2Config config(Map<String, String> overrides) {
    return OAuth2Config.fromProperties(
        ImmutableMap.<String, String>builder()
            .putAll(server.clientProperties())
            .putAll(overrides)
            .buildKeepingLast());
  }
}
