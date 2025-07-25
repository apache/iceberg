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
package org.apache.iceberg.rest.auth.oauth2.config;

import com.nimbusds.oauth2.sdk.GrantType;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/** Configuration properties for the token refresh feature. */
@Value.Immutable
public interface TokenRefreshConfig {

  String PREFIX = OAuth2Config.PREFIX + "token-refresh.";

  String ENABLED = PREFIX + "enabled";
  String TOKEN_EXCHANGE_ENABLED = PREFIX + "token-exchange-enabled";
  String ACCESS_TOKEN_LIFESPAN = PREFIX + "access-token-lifespan";
  String SAFETY_MARGIN = PREFIX + "safety-margin";

  Duration DEFAULT_ACCESS_TOKEN_LIFESPAN = Duration.parse("PT1H");
  Duration DEFAULT_SAFETY_MARGIN = Duration.parse("PT10S");

  Duration MIN_ACCESS_TOKEN_LIFESPAN = Duration.parse("PT15S");
  Duration MIN_SAFETY_MARGIN = Duration.parse("PT10S");

  /**
   * Whether to enable token refresh. If enabled, the OAuth2 client will automatically refresh its
   * access token when it expires. If disabled, the OAuth2 client will only fetch the initial access
   * token, but won't refresh it. Defaults to {@code true}.
   */
  @ConfigOption(ENABLED)
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
   * <p>Optional, defaults to {@code true} if the initial grant is {@link
   * GrantType#CLIENT_CREDENTIALS}.
   */
  @ConfigOption(TOKEN_EXCHANGE_ENABLED)
  Optional<Boolean> tokenExchangeEnabled();

  /**
   * Default access token lifespan; if the OAuth2 server returns an access token without specifying
   * its expiration time, this value will be used.
   *
   * <p>Optional, defaults to {@link #DEFAULT_ACCESS_TOKEN_LIFESPAN}. Must be a valid <a
   * href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigOption(ACCESS_TOKEN_LIFESPAN)
  @Value.Default
  default Duration accessTokenLifespan() {
    return DEFAULT_ACCESS_TOKEN_LIFESPAN;
  }

  /**
   * Refresh safety margin to use; a new token will be fetched when the current token's remaining
   * lifespan is less than this value. Optional, defaults to {@link #DEFAULT_SAFETY_MARGIN}. Must be
   * a valid <a href="https://en.wikipedia.org/wiki/ISO_8601#Durations">ISO-8601 duration</a>.
   */
  @ConfigOption(SAFETY_MARGIN)
  @Value.Default
  default Duration safetyMargin() {
    return DEFAULT_SAFETY_MARGIN;
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
          safetyMargin().compareTo(MIN_SAFETY_MARGIN) >= 0,
          SAFETY_MARGIN,
          "refresh safety margin must be greater than or equal to %s",
          MIN_SAFETY_MARGIN);
      validator.check(
          safetyMargin().compareTo(accessTokenLifespan()) < 0,
          List.of(SAFETY_MARGIN, ACCESS_TOKEN_LIFESPAN),
          "refresh safety margin must be less than the access token lifespan");
      validator.validate();
    }
  }

  static ImmutableTokenRefreshConfig.Builder parse(Map<String, String> properties) {
    return ImmutableTokenRefreshConfig.builder()
        .enabled(ConfigUtil.parseOptional(properties, ENABLED, Boolean::parseBoolean).orElse(true))
        .tokenExchangeEnabled(
            ConfigUtil.parseOptional(properties, TOKEN_EXCHANGE_ENABLED, Boolean::parseBoolean))
        .accessTokenLifespan(
            ConfigUtil.parseOptional(properties, ACCESS_TOKEN_LIFESPAN, Duration::parse)
                .orElse(DEFAULT_ACCESS_TOKEN_LIFESPAN))
        .safetyMargin(
            ConfigUtil.parseOptional(properties, SAFETY_MARGIN, Duration::parse)
                .orElse(DEFAULT_SAFETY_MARGIN));
  }
}
