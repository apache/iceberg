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

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.AccessTokenResponse;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import com.nimbusds.oauth2.sdk.token.Token;
import com.nimbusds.oauth2.sdk.token.Tokens;
import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * The result of a successful token request, capturing the {@linkplain Tokens issued tokens} and the
 * time they were received, thus allowing the calculation of their expiration time.
 */
@Value.Immutable
public abstract class TokensResult {

  /** Creates a new {@link TokensResult} from a static access token and a clock. */
  public static TokensResult of(AccessToken token, Clock clock) {
    return of(new Tokens(token, null), clock);
  }

  /** Creates a new {@link TokensResult} from an {@link AccessTokenResponse} and a clock. */
  public static TokensResult of(AccessTokenResponse response, Clock clock) {
    return of(response.toSuccessResponse().getTokens(), clock);
  }

  /**
   * Creates a new {@link TokensResult} from a {@link Tokens} object, custom parameters, and a
   * clock.
   */
  public static TokensResult of(Tokens tokens, Clock clock) {
    return ImmutableTokensResult.builder().tokens(tokens).clock(clock).build();
  }

  /** The issued tokens. */
  public abstract Tokens tokens();

  /**
   * The time when this result was received (i.e. the local clock time at construction). This is
   * used for computing expiration from the OAuth2 {@code expires_in} field, which is relative to
   * the response time, not the JWT {@code iat} claim.
   */
  @Value.Derived
  public Instant receivedAt() {
    return clock().instant();
  }

  /**
   * The resolved expiration time of the access token, taking into account the response's {@code
   * expires_in} field and the JWT claims, if applicable.
   */
  @Value.Lazy
  public Optional<Instant> accessTokenExpirationTime() {
    return accessTokenResponseExpirationTime().or(this::accessTokenJwtExpirationTime);
  }

  /** The clock used to determine the current time. Not exposed publicly. */
  @Value.Auxiliary
  abstract Clock clock();

  /** The access token expiration time as reported in the token response, if any. */
  @Value.Lazy
  Optional<Instant> accessTokenResponseExpirationTime() {
    long lifetimeSeconds = tokens().getAccessToken().getLifetime();
    // Note: we don't rely on the JWT's "iat" claim because it was set
    // by the server's clock and hence may be off from our own clock.
    return lifetimeSeconds > 0
        ? Optional.of(receivedAt().plusSeconds(lifetimeSeconds))
        : Optional.empty();
  }

  /**
   * The access token JWT token expiration time, if the token is a JWT token and contains an
   * expiration ("exp") claim.
   */
  @Value.Lazy
  Optional<Instant> accessTokenJwtExpirationTime() {
    Token token = tokens().getAccessToken();
    try {
      Date expirationTime = JWTParser.parse(token.getValue()).getJWTClaimsSet().getExpirationTime();
      return Optional.ofNullable(expirationTime).map(Date::toInstant);
    } catch (Exception ignored) {
      return Optional.empty();
    }
  }
}
