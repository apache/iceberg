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

import com.nimbusds.oauth2.sdk.GrantType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

/** Configuration properties for the token refresh feature. */
@Value.Immutable
interface TokenRefreshConfig {

  String PREFIX = OAuth2Config.PREFIX + "token-refresh.";

  String ENABLED = PREFIX + "enabled";
  String TOKEN_EXCHANGE_ENABLED = PREFIX + "token-exchange-enabled";
  String ACCESS_TOKEN_LIFESPAN = PREFIX + "access-token-lifespan";
  String PREFETCH = PREFIX + "prefetch";
  String JITTER = PREFIX + "jitter";

  Duration DEFAULT_ACCESS_TOKEN_LIFESPAN = Duration.parse("PT1H");
  Duration DEFAULT_PREFETCH = Duration.parse("PT10S");
  Duration DEFAULT_JITTER = Duration.parse("PT5S");

  Duration MIN_ACCESS_TOKEN_LIFESPAN = Duration.parse("PT15S");
  Duration MIN_PREFETCH = Duration.parse("PT10S");

  /**
   * Whether to enable token refresh. If enabled, the OAuth2 client will automatically refresh its
   * access token when it expires. If disabled, the OAuth2 client will only fetch the initial access
   * token, but won't refresh it. Defaults to {@code true}.
   */
  @Value.Default
  default boolean enabled() {
    return true;
  }

  /**
   * Whether to use the token exchange grant to refresh tokens.
   *
   * <p>When enabled, the token exchange grant will be used to refresh the access token, if no
   * refresh token is available.
   *
   * <p>Optional. If absent, defaults to {@code true} when the initial grant is {@link
   * GrantType#CLIENT_CREDENTIALS}, and to {@code false} otherwise.
   */
  Optional<Boolean> tokenExchangeEnabled();

  /**
   * Default access token lifespan; if the OAuth2 server returns an access token without specifying
   * its expiration time, this value will be used.
   *
   * <p>Defaults to {@link #DEFAULT_ACCESS_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @Value.Default
  default Duration accessTokenLifespan() {
    return DEFAULT_ACCESS_TOKEN_LIFESPAN;
  }

  /**
   * Prefetch duration to use; a new token will be fetched when the current token's remaining
   * lifespan is less than this value. Defaults to {@link #DEFAULT_PREFETCH}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @Value.Default
  default Duration prefetch() {
    return DEFAULT_PREFETCH;
  }

  /**
   * Maximum jitter to subtract from the computed token renewal delay, to avoid thundering-herd
   * effects when many clients refresh at the same time.
   *
   * <p>A random value within {@code [0, jitter)} is subtracted from the delay, so renewal is spread
   * over the window {@code [expirationTime - prefetch - jitter, expirationTime - prefetch]}, and
   * thus the prefetch duration is always respected.
   *
   * <p>Defaults to {@link #DEFAULT_JITTER}. Setting this property to zero disables jitter. Must be
   * a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @Value.Default
  default Duration jitter() {
    return DEFAULT_JITTER;
  }

  @Value.Check
  default void validate() {
    if (enabled()) {
      ConfigValidator validator = new ConfigValidator();
      validator.check(
          accessTokenLifespan().compareTo(MIN_ACCESS_TOKEN_LIFESPAN) >= 0,
          ACCESS_TOKEN_LIFESPAN,
          "access token lifespan must be greater than or equal to %s",
          MIN_ACCESS_TOKEN_LIFESPAN);
      validator.check(
          prefetch().compareTo(MIN_PREFETCH) >= 0,
          PREFETCH,
          "refresh prefetch must be greater than or equal to %s",
          MIN_PREFETCH);
      validator.check(
          prefetch().compareTo(accessTokenLifespan()) < 0,
          List.of(PREFETCH, ACCESS_TOKEN_LIFESPAN),
          "refresh prefetch must be less than the access token lifespan");
      validator.check(!jitter().isNegative(), JITTER, "jitter must not be negative");
      validator.check(
          jitter().plus(prefetch()).compareTo(accessTokenLifespan()) < 0,
          List.of(JITTER, PREFETCH, ACCESS_TOKEN_LIFESPAN),
          "jitter plus prefetch must be less than the access token lifespan");
      validator.validate();
    }
  }

  static ImmutableTokenRefreshConfig.Builder from(Map<String, String> properties) {
    return ImmutableTokenRefreshConfig.builder()
        .enabled(ConfigUtil.parseOptional(properties, ENABLED, Boolean::parseBoolean).orElse(true))
        .tokenExchangeEnabled(
            ConfigUtil.parseOptional(properties, TOKEN_EXCHANGE_ENABLED, Boolean::parseBoolean))
        .accessTokenLifespan(
            ConfigUtil.parseOptional(properties, ACCESS_TOKEN_LIFESPAN, Duration::parse)
                .orElse(DEFAULT_ACCESS_TOKEN_LIFESPAN))
        .prefetch(
            ConfigUtil.parseOptional(properties, PREFETCH, Duration::parse)
                .orElse(DEFAULT_PREFETCH))
        .jitter(
            ConfigUtil.parseOptional(properties, JITTER, Duration::parse).orElse(DEFAULT_JITTER));
  }
}
