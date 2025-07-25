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

import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.ACTOR_TOKEN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.ACTOR_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.AUDIENCES;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.REQUESTED_TOKEN_TYPE;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.RESOURCES;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.SUBJECT_TOKEN;
import static org.apache.iceberg.rest.auth.oauth2.config.TokenExchangeConfig.SUBJECT_TOKEN_TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestTokenExchangeConfig {

  @ParameterizedTest
  @MethodSource
  void testParse(Map<String, String> properties, TokenExchangeConfig expected) {
    TokenExchangeConfig actual = TokenExchangeConfig.parse(properties).build();
    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> testParse() throws ParseException {
    return Stream.of(
        Arguments.of(Map.of(), ImmutableTokenExchangeConfig.builder().build()),
        Arguments.of(
            Map.of(
                SUBJECT_TOKEN,
                "my-subject-token",
                SUBJECT_TOKEN_TYPE,
                "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .subjectTokenString("my-subject-token")
                .subjectTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(
                ACTOR_TOKEN,
                "my-actor-token",
                ACTOR_TOKEN_TYPE,
                "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .actorTokenString("my-actor-token")
                .actorTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(REQUESTED_TOKEN_TYPE, "urn:ietf:params:oauth:token-type:jwt"),
            ImmutableTokenExchangeConfig.builder()
                .requestedTokenType(TokenTypeURI.parse("urn:ietf:params:oauth:token-type:jwt"))
                .build()),
        Arguments.of(
            Map.of(RESOURCES, "https://example.com/api"),
            ImmutableTokenExchangeConfig.builder()
                .addResources(URI.create("https://example.com/api"))
                .build()),
        Arguments.of(
            Map.of(AUDIENCES, "https://example.com/resource"),
            ImmutableTokenExchangeConfig.builder()
                .addAudiences(new Audience("https://example.com/resource"))
                .build()),
        Arguments.of(
            Map.of(AUDIENCES, "https://example.com/resource1,https://example.com/resource2"),
            ImmutableTokenExchangeConfig.builder()
                .addAudiences(
                    new Audience("https://example.com/resource1"),
                    new Audience("https://example.com/resource2"))
                .build()));
  }
}
