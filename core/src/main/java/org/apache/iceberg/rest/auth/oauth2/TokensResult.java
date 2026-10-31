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

import com.nimbusds.jwt.JWTParser;
import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.time.Instant;
import java.util.Date;
import javax.annotation.Nullable;

/**
 * The result of a successful token request: the issued access token, the time it was received, and
 * its expiration time, if known.
 */
record TokensResult(AccessToken accessToken, Instant receivedAt, @Nullable Instant expirationTime) {

  /**
   * Creates a result for a token received at the given time. The expiration time is derived from
   * the response's {@code expires_in} field if present, or from the JWT {@code exp} claim if the
   * token is a JWT. The response field takes precedence because it is relative to the local clock,
   * whereas JWT claims are set by the server's clock.
   */
  TokensResult(AccessToken accessToken, Instant receivedAt) {
    this(
        accessToken,
        receivedAt,
        accessToken.getLifetime() > 0
            ? receivedAt.plusSeconds(accessToken.getLifetime())
            : jwtExpirationTime(accessToken));
  }

  @Nullable
  private static Instant jwtExpirationTime(AccessToken accessToken) {
    try {
      Date expirationTime =
          JWTParser.parse(accessToken.getValue()).getJWTClaimsSet().getExpirationTime();
      return expirationTime == null ? null : expirationTime.toInstant();
    } catch (Exception ignored) {
      // not a JWT, or the claims cannot be parsed
      return null;
    }
  }
}
