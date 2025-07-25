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
package org.apache.iceberg.rest.auth.oauth2.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.throwable;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.apache.iceberg.rest.auth.oauth2.test.TestCertificates;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ErrorExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.expectation.ImmutableRefreshTokenExpectation;
import org.apache.iceberg.rest.auth.oauth2.test.junit.EnumLike;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

class TestOAuth2Client {

  @CartesianTest
  void testClientCredentials(
      @EnumLike(excludes = "none") ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens) {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult currentTokens = client.authenticateInternal();
      env.assertTokensResult(
          currentTokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
    }
  }

  @Test
  void testClientCredentialsUnauthorized() {
    try (TestEnvironment env =
            TestEnvironment.builder().clientId(new ClientID("WrongClient")).build();
        OAuth2Client client = env.newOAuth2Client()) {
      assertThatThrownBy(client::authenticate)
          .asInstanceOf(throwable(OAuth2Exception.class))
          .extracting(OAuth2Exception::error)
          .isEqualTo(ErrorExpectation.OAUTH2_ERROR);
    }
  }

  @CartesianTest
  void testRefreshToken(
      @EnumLike(excludes = "none") ClientAuthenticationMethod authenticationMethod)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(true)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult firstTokens = client.authenticateInternal();
      TokensResult refreshedTokens =
          client.refreshCurrentTokens(firstTokens).toCompletableFuture().get();
      env.assertTokensResult(refreshedTokens, "access_refreshed", "refresh_refreshed");
    }
  }

  @CartesianTest
  void testRefreshTokenWithTokenExchange(
      @EnumLike(excludes = "none") ClientAuthenticationMethod authenticationMethod)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(false)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult firstTokens = client.authenticateInternal();
      TokensResult refreshedTokens =
          client.refreshCurrentTokens(firstTokens).toCompletableFuture().get();
      env.assertTokensResult(refreshedTokens, "access_refreshed", null);
    }
  }

  @Test
  void testRefreshTokenMustFetchNewTokens() {
    try (TestEnvironment env =
            TestEnvironment.builder().tokenRefreshWithTokenExchangeEnabled(false).build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult currentTokens =
          TokensResult.of(new Tokens(new BearerAccessToken("access_initial"), null), env.clock());
      assertThat(client.refreshCurrentTokens(currentTokens))
          .completesExceptionallyWithin(Duration.ofSeconds(10))
          .withThrowableOfType(ExecutionException.class)
          .withCauseInstanceOf(OAuth2Client.MustFetchNewTokensException.class);
    }
  }

  @CartesianTest
  void testTokenExchangeStaticSubjectAndActorTokens(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .clientAuthenticationMethod(authenticationMethod)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult currentTokens = client.authenticateInternal();
      env.assertTokensResult(
          currentTokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
      if (returnRefreshTokens) {
        currentTokens = client.refreshCurrentTokens(currentTokens).toCompletableFuture().get();
        env.assertTokensResult(currentTokens, "access_refreshed", "refresh_refreshed");
      }
    }
  }

  @CartesianTest
  void testTokenExchangeParentSubjectAndActorTokens(
      @EnumLike ClientAuthenticationMethod authenticationMethod,
      @Values(booleans = {true, false}) boolean returnRefreshTokens)
      throws InterruptedException, ExecutionException {
    try (TestEnvironment parent = TestEnvironment.builder().build();
        OAuth2Client parentClient = parent.newOAuth2Client();
        TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .clientAuthenticationMethod(authenticationMethod)
                .subjectTokenString(ConfigUtil.PARENT_TOKEN)
                .actorTokenString(ConfigUtil.PARENT_TOKEN)
                .parentClient(parentClient)
                .returnRefreshTokens(returnRefreshTokens)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult tokens = client.authenticateInternal();
      env.assertTokensResult(
          tokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
      if (returnRefreshTokens) {
        tokens = client.refreshCurrentTokens(tokens).toCompletableFuture().get();
        env.assertTokensResult(tokens, "access_refreshed", "refresh_refreshed");
      }
    }
  }

  @Test
  void testTokenExchangeSubjectUnauthorized() {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .subjectTokenString("WrongSubjectToken")
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      assertThatThrownBy(client::authenticate)
          .hasMessageContaining("OAuth2 request failed: Invalid request")
          .asInstanceOf(throwable(OAuth2Exception.class))
          .extracting(OAuth2Exception::error)
          .isEqualTo(ErrorExpectation.OAUTH2_ERROR);
    }
  }

  @Test
  void testTokenExchangeActorUnauthorized() {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .grantType(GrantType.TOKEN_EXCHANGE)
                .actorTokenString("WrongActorToken")
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      assertThatThrownBy(client::authenticate)
          .asInstanceOf(throwable(OAuth2Exception.class))
          .hasMessageContaining("OAuth2 request failed: Invalid request")
          .extracting(OAuth2Exception::error)
          .isEqualTo(ErrorExpectation.OAUTH2_ERROR);
    }
  }

  @Test
  void testStaticTokenNoRefreshToken() {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .token(new TypelessAccessToken("access_initial"))
                .returnRefreshTokens(false)
                .tokenRefreshWithTokenExchangeEnabled(false)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult tokens = client.authenticateInternal();
      env.assertAccessToken(tokens.tokens().getAccessToken(), "access_initial", Duration.ZERO);
      // Cannot refresh a static token using the refresh_token grant,
      // as there is no initial refresh token
      assertThat(client.refreshCurrentTokens(tokens))
          .completesExceptionallyWithin(Duration.ofSeconds(10))
          .withThrowableOfType(ExecutionException.class)
          .withCauseInstanceOf(OAuth2Client.MustFetchNewTokensException.class);
    }
  }

  @Test
  void testStaticTokenWithTokenExchangeRefreshNoClientId() {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .token(new TypelessAccessToken("access_initial"))
                .returnRefreshTokens(false)
                .tokenRefreshWithTokenExchangeEnabled(true)
                .clientId(Optional.empty())
                .clientSecret(Optional.empty())
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult tokens = client.authenticateInternal();
      env.assertAccessToken(tokens.tokens().getAccessToken(), "access_initial", Duration.ZERO);
      // Cannot refresh a static token using the token exchange grant,
      // when there is no configured client id / secret
      assertThat(client.refreshCurrentTokens(tokens))
          .completesExceptionallyWithin(Duration.ofSeconds(10))
          .withThrowableOfType(ExecutionException.class)
          .havingCause()
          .isInstanceOf(IllegalStateException.class)
          .withMessage("Client ID is required");
    }
  }

  @CartesianTest
  void testStaticTokenWithTokenExchangeRefreshWithClientId()
      throws ExecutionException, InterruptedException {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .token(new TypelessAccessToken("access_initial"))
                .returnRefreshTokens(false)
                .tokenRefreshWithTokenExchangeEnabled(true)
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      TokensResult tokens = client.authenticateInternal();
      env.assertAccessToken(tokens.tokens().getAccessToken(), "access_initial", Duration.ZERO);
      // Can refresh a static token using the token exchange grant,
      // when there is a configured client id / secret
      tokens = client.refreshCurrentTokens(tokens).toCompletableFuture().get();
      env.assertTokensResult(tokens, "access_refreshed", null);
    }
  }

  @Test
  void testSsl() {
    TestCertificates certs = TestCertificates.instance();

    try (TestEnvironment env =
            TestEnvironment.builder()
                .ssl(true)
                .sslTrustStorePath(certs.mockServerKeyStore())
                .sslTrustStorePassword(certs.keyStorePassword())
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      assertThatCode(client::authenticate).doesNotThrowAnyException();
    }
  }

  // Token renewal tests

  /**
   * Tests the whole token renewal cycle, by manually triggering the token renewal task (which is
   * normally scheduled).
   */
  @CartesianTest
  void testTokenRenewal(@Values(booleans = {true, false}) boolean returnRefreshTokens) {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .createDefaultExpectations(false)
            .returnRefreshTokens(returnRefreshTokens)
            .tokenRefreshEnabled(false) // will be triggered manually below
            .build()) {
      env.createMetadataDiscoveryExpectations();
      env.createInitialGrantExpectations();
      ImmutableRefreshTokenExpectation.of(env).create();
      try (OAuth2Client client = env.newOAuth2Client()) {
        TokensResult initialTokens = client.authenticateInternal();
        env.assertTokensResult(
            initialTokens, "access_initial", returnRefreshTokens ? "refresh_initial" : null);
        client.renewTokens();
        TokensResult refreshedTokens = client.authenticateInternal();
        env.assertTokensResult(
            refreshedTokens, "access_refreshed", returnRefreshTokens ? "refresh_refreshed" : null);
      }
    }
  }

  /**
   * Tests the whole token renewal cycle, but in the edge case where the refreshed access token is
   * too short and is discarded.
   */
  @Test
  void testTokenRenewalRefreshedAccessTokenTooShort() {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .returnRefreshTokens(true)
            .accessTokenLifespan(Duration.ofSeconds(1))
            .tokenRefreshEnabled(false) // will be triggered manually below
            .build()) {
      try (OAuth2Client client = env.newOAuth2Client()) {
        TokensResult initialTokens = client.authenticateInternal();
        env.assertTokensResult(initialTokens, "access_initial", "refresh_initial");
        client.renewTokens();
        TokensResult nextTokens = client.authenticateInternal();
        // Refreshed access token should have been discarded and a new one fetched
        assertThat(nextTokens).isNotSameAs(initialTokens);
        env.assertTokensResult(nextTokens, "access_initial", "refresh_initial");
      }
    }
  }

  // Client copy tests

  /**
   * Tests copying a client before and after closing the original client. The typical use case for
   * copying a client is when reusing an init session as a catalog session, so the init session is
   * already closed when it's copied. But we also want to test the case where the init session is
   * not yet closed when it's copied, since nothing in the API prevents that.
   */
  @Test
  void testCopyAfterSuccessfulAuth() throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
            TestEnvironment.builder().grantType(GrantType.TOKEN_EXCHANGE).build();
        OAuth2Client client1 = env.newOAuth2Client()) {
      TokensResult tokens = client1.authenticateInternal();
      env.assertTokensResult(tokens, "access_initial", "refresh_initial");
      // 1) Test copy before close
      try (OAuth2Client client2 = client1.copy()) {
        // Should have the same tokens instance
        assertThat(client2.authenticateInternal()).isSameAs(tokens);
        // Now close client1
        client1.close();
        // Should still have the same tokens instance, and not throw
        assertThat(client2.authenticateInternal()).isSameAs(tokens);
        // Should have a token refresh future
        assertThat(client2).extracting("tokenRefreshFuture").isNotNull();
        // Should be able to refresh tokens
        TokensResult refreshedTokens =
            client2.refreshCurrentTokens(tokens).toCompletableFuture().get();
        env.assertTokensResult(refreshedTokens, "access_refreshed", "refresh_refreshed");
        // Should be able to fetch new tokens
        TokensResult newTokens = client2.fetchNewTokens().toCompletableFuture().get();
        env.assertTokensResult(newTokens, "access_initial", "refresh_initial");
      }

      // 2) Test copy after close
      try (OAuth2Client client3 = client1.copy()) {
        // Should have the same tokens instance
        assertThat(client3.authenticateInternal()).isSameAs(tokens);
        // Should have a token refresh future
        assertThat(client3).extracting("tokenRefreshFuture").isNotNull();
        // Should be able to refresh tokens
        TokensResult refreshedTokens =
            client3.refreshCurrentTokens(tokens).toCompletableFuture().get();
        env.assertTokensResult(refreshedTokens, "access_refreshed", "refresh_refreshed");
        // Should be able to fetch new tokens
        TokensResult newTokens = client3.fetchNewTokens().toCompletableFuture().get();
        env.assertTokensResult(newTokens, "access_initial", "refresh_initial");
      }
    }
  }

  /**
   * Tests copying a client before and after closing the original client, when the original client
   * failed to authenticate. This is a rather contrived scenario since in practice, a failed init
   * session would cause the catalog initialization to fail; but it's possible in theory, so we
   * should add tests for it.
   */
  @Test
  @SuppressWarnings("NestedTryDepth")
  void testCopyAfterFailedAuth() throws InterruptedException, ExecutionException {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .grantType(GrantType.TOKEN_EXCHANGE)
            .createDefaultExpectations(false)
            .build()) {
      // Emulate success fetching metadata, but failure on initial token fetch
      env.createMetadataDiscoveryExpectations();
      env.createErrorExpectations();
      try (OAuth2Client client1 = env.newOAuth2Client()) {
        assertThatThrownBy(client1::authenticateInternal)
            .isInstanceOf(OAuth2Exception.class)
            .hasMessageContaining("Invalid request");
        // Restore expectations so that copied clients can fetch tokens
        env.reset();
        env.createExpectations();
        // 1) Test copy before close
        try (OAuth2Client client2 = client1.copy()) {
          // Should be able to fetch tokens even if the original client failed
          assertThat(client2.authenticateInternal()).isNotNull();
          // Now close client1
          client1.close();
          // Should still have tokens
          TokensResult tokens = client2.authenticateInternal();
          env.assertTokensResult(tokens, "access_initial", "refresh_initial");
          assertThat(tokens).isNotNull();
          // Should have a token refresh future
          assertThat(client2).extracting("tokenRefreshFuture").isNotNull();
          // Should be able to refresh tokens
          TokensResult refreshedTokens =
              client2.refreshCurrentTokens(tokens).toCompletableFuture().get();
          env.assertTokensResult(refreshedTokens, "access_refreshed", "refresh_refreshed");
          // Should be able to fetch new tokens
          TokensResult newTokens = client2.fetchNewTokens().toCompletableFuture().get();
          env.assertTokensResult(newTokens, "access_initial", "refresh_initial");
        }

        // 2) Test copy after close
        try (OAuth2Client client3 = client1.copy()) {
          // Should be able to fetch tokens even if the original client failed
          TokensResult tokens = client3.authenticateInternal();
          env.assertTokensResult(tokens, "access_initial", "refresh_initial");
          assertThat(tokens).isNotNull();
          // Should be able to refresh tokens
          TokensResult refreshedTokens =
              client3.refreshCurrentTokens(tokens).toCompletableFuture().get();
          env.assertTokensResult(refreshedTokens, "access_refreshed", "refresh_refreshed");
          // Should be able to fetch new tokens
          TokensResult newTokens = client3.fetchNewTokens().toCompletableFuture().get();
          env.assertTokensResult(newTokens, "access_initial", "refresh_initial");
        }
      }
    }
  }

  // Concurrency tests

  /**
   * Multiple threads calling authenticate() concurrently should all get a valid, non-null access
   * token. This exercises the volatile currentTokensFuture field under contention.
   */
  @RepeatedTest(100)
  void testParallelAuthenticate() throws Exception {
    try (TestEnvironment env = TestEnvironment.builder().build();
        OAuth2Client client = env.newOAuth2Client()) {

      CyclicBarrier barrier = new CyclicBarrier(10);
      List<AccessToken> tokens = new CopyOnWriteArrayList<>();
      List<Throwable> errors = new CopyOnWriteArrayList<>();

      Thread[] threads = new Thread[10];
      for (int i = 0; i < 10; i++) {
        threads[i] =
            new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    AccessToken token = client.authenticate();
                    tokens.add(token);
                  } catch (Throwable t) {
                    errors.add(t);
                  }
                });
        threads[i].start();
      }

      for (Thread t : threads) {
        t.join(10_000);
      }

      assertThat(errors).as("No thread should have thrown an exception").isEmpty();
      assertThat(tokens)
          .hasSize(10)
          .allSatisfy(token -> assertThat(token.getValue()).isEqualTo("access_initial"));
    }
  }

  /**
   * Calling close() while multiple threads are authenticating should not cause unhandled
   * exceptions. Threads may get a CancellationException or succeed — either is acceptable.
   */
  @RepeatedTest(100)
  void testCloseWhileAuthenticating() throws Exception {
    try (TestEnvironment env = TestEnvironment.builder().build();
        OAuth2Client client = env.newOAuth2Client()) {

      CyclicBarrier barrier = new CyclicBarrier(11);
      List<Throwable> errors = new CopyOnWriteArrayList<>();

      // Repeatedly authenticate while the client may be closing
      Runnable action =
          () -> {
            for (int i = 0; i < 100; i++) {
              try {
                client.authenticate();
              } catch (CancellationException ignored) {
                // Cancellation is expected when client closing
              }
            }
          };

      Thread[] threads = new Thread[10];

      for (int i = 0; i < 10; i++) {
        threads[i] =
            new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    action.run();
                  } catch (Throwable t) {
                    errors.add(t);
                  }
                });
        threads[i].start();
      }

      // Release all threads, then close the client mid-flight
      barrier.await(5, TimeUnit.SECONDS);
      client.close();

      for (Thread t : threads) {
        t.join(10_000);
      }

      assertThat(errors)
          .as("No thread should have thrown an exception other than CancellationException")
          .isEmpty();

      // close() is idempotent
      assertThatCode(client::close).doesNotThrowAnyException();
    }
  }

  /**
   * Multiple threads calling authenticate() and renewTokens() concurrently should not corrupt the
   * client's internal state.
   */
  @RepeatedTest(100)
  void testConcurrentAuthenticateAndRenew() throws Exception {
    try (TestEnvironment env =
            TestEnvironment.builder()
                .tokenRefreshEnabled(false) // renewals will be triggered manually
                .build();
        OAuth2Client client = env.newOAuth2Client()) {

      // Ensure initial auth completes
      client.authenticate();

      CyclicBarrier barrier = new CyclicBarrier(10);
      List<Throwable> errors = new CopyOnWriteArrayList<>();

      Thread[] threads = new Thread[10];
      for (int i = 0; i < 10; i++) {
        boolean renew = i == 0;
        threads[i] =
            new Thread(
                () -> {
                  try {
                    barrier.await(5, TimeUnit.SECONDS);
                    for (int j = 0; j < 50; j++) {
                      if (renew) {
                        client.renewTokens();
                      } else {
                        AccessToken token = client.authenticate();
                        assertThat(token).isNotNull();
                        assertThat(token.getValue()).isIn("access_initial", "access_refreshed");
                      }
                    }
                  } catch (Throwable t) {
                    errors.add(t);
                  }
                });
        threads[i].start();
      }

      for (Thread t : threads) {
        t.join(10_000);
      }

      assertThat(errors).as("No thread should have thrown an exception").isEmpty();
    }
  }
}
