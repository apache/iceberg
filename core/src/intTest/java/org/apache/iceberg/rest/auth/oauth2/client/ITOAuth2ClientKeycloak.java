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

import static com.nimbusds.oauth2.sdk.GrantType.CLIENT_CREDENTIALS;
import static com.nimbusds.oauth2.sdk.GrantType.TOKEN_EXCHANGE;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_BASIC;
import static com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod.CLIENT_SECRET_POST;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID1;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID2;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_ID3;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_SECRET2;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.CLIENT_SECRET3;
import static org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension.SCOPE1;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.type;

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.text.ParseException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Exception;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigUtil;
import org.apache.iceberg.rest.auth.oauth2.flow.TokensResult;
import org.apache.iceberg.rest.auth.oauth2.test.ImmutableTestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.apache.iceberg.rest.auth.oauth2.test.container.KeycloakContainer;
import org.apache.iceberg.rest.auth.oauth2.test.junit.KeycloakExtension;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(KeycloakExtension.class)
public class ITOAuth2ClientKeycloak {

  private static KeycloakContainer keycloak;

  @BeforeAll
  static void setKeycloakContainer(KeycloakContainer keycloakContainer) {
    keycloak = keycloakContainer;
  }

  @Test
  void clientSecretBasic(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.clientAuthenticationMethod(CLIENT_SECRET_BASIC).build();
        OAuth2Client client = env.newOAuth2Client()) {
      testClient(client, true);
    }
  }

  @Test
  void clientSecretPost(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.clientAuthenticationMethod(CLIENT_SECRET_POST).build();
        OAuth2Client client = env.newOAuth2Client()) {
      testClient(client, true);
    }
  }

  /**
   * Tests a token exchange scenario with a fixed subject token obtained off-band, and no actor
   * token.
   */
  @Test
  void impersonation(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws Exception {
    AccessToken subjectToken;
    try (TestEnvironment env =
            envBuilder1
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID2))
                .clientSecret(new Secret(CLIENT_SECRET2))
                .build();
        OAuth2Client subjectClient = env.newOAuth2Client()) {
      subjectToken = subjectClient.authenticate();
    }

    try (TestEnvironment env =
            envBuilder2
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(TokenTypeURI.ACCESS_TOKEN)
                .subjectTokenString(subjectToken.getValue())
                .actorTokenString(Optional.empty())
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      testClient(client, false);
    }
  }

  /**
   * Tests a token exchange scenario with fixed subject and actor tokens, both obtained off-band.
   */
  @Test
  void delegation(
      ImmutableTestEnvironment.Builder envBuilder1,
      ImmutableTestEnvironment.Builder envBuilder2,
      ImmutableTestEnvironment.Builder envBuilder3)
      throws Exception {
    AccessToken subjectToken;
    try (TestEnvironment env =
            envBuilder1
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID2))
                .clientSecret(new Secret(CLIENT_SECRET2))
                .build();
        OAuth2Client subjectClient = env.newOAuth2Client()) {
      subjectToken = subjectClient.authenticate();
    }

    AccessToken actorToken;
    try (TestEnvironment env =
            envBuilder2
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID3))
                .clientSecret(new Secret(CLIENT_SECRET3))
                .build();
        OAuth2Client actorClient = env.newOAuth2Client()) {
      actorToken = actorClient.authenticate();
    }

    try (TestEnvironment env =
            envBuilder3
                .grantType(TOKEN_EXCHANGE)
                .requestedTokenType(TokenTypeURI.ACCESS_TOKEN)
                .subjectTokenString(subjectToken.getValue())
                .actorTokenString(actorToken.getValue())
                .build();
        OAuth2Client client = env.newOAuth2Client()) {
      testClient(client, false);
    }
  }

  /**
   * Tests a token exchange scenario with the subject token inherited from a separate, parent
   * client, and no actor token.
   */
  @Test
  void parentSubject(
      ImmutableTestEnvironment.Builder envBuilder1, ImmutableTestEnvironment.Builder envBuilder2)
      throws Exception {
    try (TestEnvironment parent =
            envBuilder1
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID2))
                .clientSecret(new Secret(CLIENT_SECRET2))
                .build();
        OAuth2Client parentClient = parent.newOAuth2Client()) {
      try (TestEnvironment env =
              envBuilder2
                  .grantType(TOKEN_EXCHANGE)
                  .requestedTokenType(TokenTypeURI.ACCESS_TOKEN)
                  .parentClient(parentClient)
                  .subjectTokenString(ConfigUtil.PARENT_TOKEN)
                  .actorTokenString(Optional.empty())
                  .build();
          OAuth2Client client = env.newOAuth2Client()) {
        testClient(client, false);
      }
    }
  }

  /**
   * Tests a token exchange scenario with a fixed subject token obtained off-band, and the actor
   * token inherited from a separate, parent client.
   */
  @Test
  void parentActor(
      ImmutableTestEnvironment.Builder envBuilder1,
      ImmutableTestEnvironment.Builder envBuilder2,
      ImmutableTestEnvironment.Builder envBuilder3)
      throws Exception {
    AccessToken subjectToken;
    try (TestEnvironment env =
            envBuilder1
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID2))
                .clientSecret(new Secret(CLIENT_SECRET2))
                .build();
        OAuth2Client subjectClient = env.newOAuth2Client()) {
      subjectToken = subjectClient.authenticate();
    }

    try (TestEnvironment parent =
            envBuilder2
                .grantType(CLIENT_CREDENTIALS)
                .clientId(new ClientID(CLIENT_ID3))
                .clientSecret(new Secret(CLIENT_SECRET3))
                .build();
        OAuth2Client parentClient = parent.newOAuth2Client()) {
      try (TestEnvironment env =
              envBuilder3
                  .grantType(TOKEN_EXCHANGE)
                  .requestedTokenType(TokenTypeURI.ACCESS_TOKEN)
                  .parentClient(parentClient)
                  .subjectTokenString(subjectToken.getValue())
                  .actorTokenString(ConfigUtil.PARENT_TOKEN)
                  .build();
          OAuth2Client client = env.newOAuth2Client()) {
        testClient(client, false);
      }
    }
  }

  @Test
  void parallelAuthenticate(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.build()) {

      CyclicBarrier barrier = new CyclicBarrier(10);
      List<Throwable> errors = new CopyOnWriteArrayList<>();

      Thread[] threads = new Thread[10];
      for (int i = 0; i < 10; i++) {
        threads[i] =
            new Thread(
                () -> {
                  try (OAuth2Client client = env.newOAuth2Client()) {
                    barrier.await(5, TimeUnit.SECONDS);
                    testClient(client, true);
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

  /** Tests dynamically-obtained tokens with refresh forcibly disabled. */
  @Test
  void refreshDisabled(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env =
            envBuilder.grantType(CLIENT_CREDENTIALS).tokenRefreshEnabled(false).build();
        OAuth2Client client = env.newOAuth2Client()) {
      // initial grant
      TokensResult firstTokens = client.authenticateInternal();
      introspectToken(firstTokens.tokens().getAccessToken());
      assertThat(client).extracting("tokenRefreshFuture").isNull();
    }
  }

  @Test
  void unauthorizedBadClientSecret(ImmutableTestEnvironment.Builder envBuilder) {
    try (TestEnvironment env = envBuilder.clientSecret(new Secret("BAD SECRET")).build();
        OAuth2Client client = env.newOAuth2Client()) {
      assertThatThrownBy(client::authenticate)
          .asInstanceOf(type(OAuth2Exception.class))
          .extracting(OAuth2Exception::statusCode, e -> e.error().code())
          .containsExactly(401, "unauthorized_client");
    }
  }

  /** Tests copying a client before and after closing the original client. */
  @Test
  void clientCopy(ImmutableTestEnvironment.Builder envBuilder) throws Exception {
    try (TestEnvironment env = envBuilder.build();
        OAuth2Client client = env.newOAuth2Client()) {
      // copy before close
      try (OAuth2Client client2 = client.copy()) {
        testClient(client2, true);
      }

      client.close();
      // copy after close
      try (OAuth2Client client3 = client.copy()) {
        testClient(client3, true);
      }
    }
  }

  private static void testClient(OAuth2Client client, boolean refreshPossible) throws Exception {
    // fetch initial tokens
    TokensResult initial = client.authenticateInternal();
    introspectToken(initial.tokens().getAccessToken());
    if (refreshPossible) {
      // Note: the client is configured to use token exchange when the initial grant is
      // client_credentials, and refresh_token otherwise. Keycloak is configured to support both.
      TokensResult refreshed = client.refreshCurrentTokens(initial).toCompletableFuture().get();
      introspectToken(refreshed.tokens().getAccessToken());
    } else {
      assertThat(initial.tokens().getRefreshToken()).isNull();
    }
    // fetch new tokens
    TokensResult renewed = client.fetchNewTokens().toCompletableFuture().get();
    introspectToken(renewed.tokens().getAccessToken());
  }

  private static void introspectToken(AccessToken accessToken) throws ParseException {
    assertThat(accessToken).isNotNull();
    JWTClaimsSet claims = keycloak.verifyToken(accessToken.getValue());
    assertThat(claims.getStringClaim("azp")).isEqualTo(CLIENT_ID1);
    assertThat(claims.getStringClaim("scope")).contains(SCOPE1);
  }
}
