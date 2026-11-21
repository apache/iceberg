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

import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.PlainJWT;
import com.nimbusds.oauth2.sdk.token.BearerAccessToken;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import org.junit.jupiter.api.Test;

class TestTokensResult {

  private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

  @Test
  void opaqueTokenWithoutLifetime() {
    TokensResult result = new TokensResult(new BearerAccessToken("opaque"), NOW);
    assertThat(result.accessToken().getValue()).isEqualTo("opaque");
    assertThat(result.receivedAt()).isEqualTo(NOW);
    assertThat(result.expirationTime()).isNull();
  }

  @Test
  void opaqueTokenWithLifetime() {
    TokensResult result = new TokensResult(new BearerAccessToken("opaque", 3600, null), NOW);
    assertThat(result.expirationTime()).isEqualTo(NOW.plusSeconds(3600));
  }

  @Test
  void jwtTokenWithoutLifetime() {
    Instant exp = NOW.plus(2, ChronoUnit.HOURS);
    TokensResult result = new TokensResult(new BearerAccessToken(jwt(exp)), NOW);
    assertThat(result.expirationTime()).isEqualTo(exp);
  }

  @Test
  void jwtTokenWithoutExpClaim() {
    TokensResult result = new TokensResult(new BearerAccessToken(jwt(null)), NOW);
    assertThat(result.expirationTime()).isNull();
  }

  @Test
  void lifetimeTakesPrecedenceOverJwt() {
    Instant exp = NOW.plus(2, ChronoUnit.HOURS);
    TokensResult result = new TokensResult(new BearerAccessToken(jwt(exp), 60, null), NOW);
    assertThat(result.expirationTime()).isEqualTo(NOW.plusSeconds(60));
  }

  private static String jwt(Instant exp) {
    JWTClaimsSet.Builder claims = new JWTClaimsSet.Builder().subject("alice");
    if (exp != null) {
      claims.expirationTime(Date.from(exp));
    }

    return new PlainJWT(claims.build()).serialize();
  }
}
