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

import static org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig.ENABLED;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig.SAFETY_MARGIN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTokenRefreshConfig {

  @ParameterizedTest
  @MethodSource
  @SuppressWarnings("ResultOfMethodCallIgnored")
  void testValidate(Map<String, String> properties, List<String> expected) {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> TokenRefreshConfig.parse(properties).build())
        .withMessage(ConfigValidator.buildDescription(expected.stream()));
  }

  static Stream<Arguments> testValidate() {
    return Stream.of(
        Arguments.of(
            Map.of(ACCESS_TOKEN_LIFESPAN, "PT2S"),
            List.of(
                "access token lifespan must be greater than or equal to PT15S (rest.auth.oauth2.token-refresh.access-token-lifespan)",
                "refresh safety margin must be less than the access token lifespan (rest.auth.oauth2.token-refresh.safety-margin / rest.auth.oauth2.token-refresh.access-token-lifespan)")),
        Arguments.of(
            Map.of(SAFETY_MARGIN, "PT0.1S"),
            List.of(
                "refresh safety margin must be greater than or equal to PT10S (rest.auth.oauth2.token-refresh.safety-margin)")),
        Arguments.of(
            Map.of(SAFETY_MARGIN, "PT10M", ACCESS_TOKEN_LIFESPAN, "PT5M"),
            List.of(
                "refresh safety margin must be less than the access token lifespan (rest.auth.oauth2.token-refresh.safety-margin / rest.auth.oauth2.token-refresh.access-token-lifespan)")));
  }

  @ParameterizedTest
  @MethodSource
  void testParse(Map<String, String> properties, TokenRefreshConfig expected) {
    TokenRefreshConfig actual = TokenRefreshConfig.parse(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testParse() {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableTokenRefreshConfig.builder().build()),
        Arguments.of(
            Map.of(ENABLED, "false"), ImmutableTokenRefreshConfig.builder().enabled(false).build()),
        Arguments.of(
            Map.of(TOKEN_EXCHANGE_ENABLED, "false"),
            ImmutableTokenRefreshConfig.builder().tokenExchangeEnabled(false).build()),
        Arguments.of(
            Map.of(ACCESS_TOKEN_LIFESPAN, "PT10M"),
            ImmutableTokenRefreshConfig.builder()
                .accessTokenLifespan(Duration.ofMinutes(10))
                .build()),
        Arguments.of(
            Map.of(SAFETY_MARGIN, "PT30S"),
            ImmutableTokenRefreshConfig.builder().safetyMargin(Duration.ofSeconds(30)).build()));
  }
}
