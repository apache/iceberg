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
package org.apache.iceberg.rest.auth.oauth2.flow;

import static org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment.NOW;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import com.nimbusds.oauth2.sdk.token.RefreshToken;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import org.junit.jupiter.api.Test;

class TestTokensResult {

  private final Clock clock = Clock.fixed(NOW, ZoneOffset.UTC);

  @Test
  void testCreateFromOpaqueAccessToken() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token");
    TokensResult result = TokensResult.of(accessToken, clock);
    assertThat(result.tokens().getAccessToken()).isEqualTo(accessToken);
    assertThat(result.tokens().getRefreshToken()).isNull();
    assertThat(result.receivedAt()).isEqualTo(NOW);
    assertThat(result.accessTokenExpirationTime()).isEmpty();
  }

  @Test
  void testCreateFromJwtAccessToken() throws JOSEException {
    JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .expirationTime(Date.from(NOW.plusSeconds(100)))
            .build();
    BearerAccessToken accessToken = new BearerAccessToken(createJwtToken(claimsSet));
    TokensResult result = TokensResult.of(accessToken, clock);
    assertThat(result.tokens().getAccessToken()).isEqualTo(accessToken);
    assertThat(result.tokens().getRefreshToken()).isNull();
    assertThat(result.receivedAt()).isEqualTo(NOW);
    assertThat(result.accessTokenExpirationTime()).hasValue(NOW.plusSeconds(100));
  }

  @Test
  void testCreateFromAccessTokenResponse() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token");
    RefreshToken refreshToken = new RefreshToken("test-refresh-token");
    Tokens tokens = new Tokens(accessToken, refreshToken);
    AccessTokenResponse accessTokenResponse = new AccessTokenResponse(tokens);
    TokensResult result = TokensResult.of(accessTokenResponse, clock);
    assertThat(result.tokens()).isEqualTo(tokens);
    assertThat(result.receivedAt()).isEqualTo(NOW);
  }

  // Access token expiration tests

  @Test
  void testAccessTokenNoExpirationTime() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token");
    TokensResult result = TokensResult.of(accessToken, clock);
    assertThat(result.accessTokenExpirationTime()).isEmpty();
  }

  @Test
  void testAccessTokenExpirationTimeFromResponse() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token", 7200, null);
    TokensResult result = TokensResult.of(accessToken, clock);
    assertThat(result.accessTokenExpirationTime()).hasValue(NOW.plusSeconds(7200));
  }

  @Test
  void testAccessTokenExpirationTimeFromJwt() throws JOSEException {
    Instant jwtExpiration = NOW.plusSeconds(7200);
    String jwtToken = createJwtToken(jwtExpiration);
    BearerAccessToken accessToken = new BearerAccessToken(jwtToken);
    TokensResult result = TokensResult.of(accessToken, clock);
    assertThat(result.accessTokenExpirationTime()).hasValue(jwtExpiration);
  }

  @Test
  void testAccessTokenExpirationTimeResponseTakesPrecedenceOverJwt() throws JOSEException {
    Instant jwtExpiration = NOW.plusSeconds(7200);
    String jwtToken = createJwtToken(jwtExpiration);
    BearerAccessToken accessToken = new BearerAccessToken(jwtToken, 3600, null);
    TokensResult result = TokensResult.of(accessToken, clock);
    Instant expectedExpiration = NOW.plusSeconds(3600);
    assertThat(result.accessTokenExpirationTime()).hasValue(expectedExpiration);
  }

  @Test
  void testAccessTokenExpirationTimeFromResponseUsesReceivedAtNotJwtIat() throws JOSEException {
    // JWT iat is 10 seconds ahead of local clock; expires_in should be relative to local clock
    JWTClaimsSet claimsSet =
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .issueTime(Date.from(NOW.plusSeconds(10)))
            .expirationTime(Date.from(NOW.plusSeconds(7200)))
            .build();
    BearerAccessToken accessToken = new BearerAccessToken(createJwtToken(claimsSet), 3600, null);
    TokensResult result = TokensResult.of(accessToken, clock);
    // expires_in uses receivedAt() (NOW), not the JWT iat (NOW+10)
    assertThat(result.receivedAt()).isEqualTo(NOW);
    assertThat(result.accessTokenExpirationTime()).hasValue(NOW.plusSeconds(3600));
  }

  @Test
  void testAccessTokenExpirationTimeNullWhenZeroLifetime() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token", 0, null);
    Tokens tokens = new Tokens(accessToken, null);
    TokensResult result = TokensResult.of(tokens, clock);
    assertThat(result.accessTokenExpirationTime()).isEmpty();
  }

  @Test
  void testAccessTokenExpirationTimeNullWhenNegativeLifetime() {
    BearerAccessToken accessToken = new BearerAccessToken("test-access-token", -1, null);
    Tokens tokens = new Tokens(accessToken, null);
    TokensResult result = TokensResult.of(tokens, clock);
    assertThat(result.accessTokenExpirationTime()).isEmpty();
  }

  private static String createJwtToken(Instant expiration) throws JOSEException {
    return createJwtToken(
        new JWTClaimsSet.Builder()
            .subject("test-subject")
            .expirationTime(Date.from(expiration))
            .build());
  }

  private static String createJwtToken(JWTClaimsSet claimsSet) throws JOSEException {
    SignedJWT signedJWT = new SignedJWT(new JWSHeader(JWSAlgorithm.HS256), claimsSet);
    signedJWT.sign(new MACSigner("a-secret-key-with-at-least-256-bits"));
    return signedJWT.serialize();
  }
}
