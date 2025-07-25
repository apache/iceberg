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

import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.immutables.value.Value;

/**
 * Configuration properties for the <a href="https://datatracker.ietf.org/doc/html/rfc8693">Token
 * Exchange</a> flow.
 *
 * <p>This flow allows a client to exchange one token for another, typically to obtain a token that
 * is more suitable for the target resource or service.
 */
@Value.Immutable
@Value.Style(redactedMask = "****")
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface TokenExchangeConfig {

  String PREFIX = OAuth2Config.PREFIX + "token-exchange.";

  String SUBJECT_TOKEN = PREFIX + "subject-token";
  String SUBJECT_TOKEN_TYPE = PREFIX + "subject-token-type";
  String ACTOR_TOKEN = PREFIX + "actor-token";
  String ACTOR_TOKEN_TYPE = PREFIX + "actor-token-type";
  String REQUESTED_TOKEN_TYPE = PREFIX + "requested-token-type";
  String RESOURCES = PREFIX + "resources";
  String AUDIENCES = PREFIX + "audiences";

  /**
   * The subject token to exchange. Required.
   *
   * <p>The special value {@code ::parent::} can be used to indicate that the subject token should
   * be obtained from the parent OAuth2 session.
   */
  @ConfigOption(SUBJECT_TOKEN)
  @Value.Redacted
  Optional<String> subjectTokenString();

  /**
   * The type of the subject token. Must be a valid URN. Required. If not set, the default is {@code
   * urn:ietf:params:oauth:token-type:access_token}.
   *
   * @see TokenExchangeConfig#SUBJECT_TOKEN_TYPE
   */
  @ConfigOption(SUBJECT_TOKEN_TYPE)
  Optional<TokenTypeURI> subjectTokenType();

  /**
   * The actor token to exchange. Optional.
   *
   * <p>The special value {@code ::parent::} can be used to indicate that the actor token should be
   * obtained from the parent OAuth2 session.
   */
  @ConfigOption(ACTOR_TOKEN)
  @Value.Redacted
  Optional<String> actorTokenString();

  /**
   * The type of the actor token. Must be a valid URN. Required if an actor token is used. If not
   * set, the default is {@code urn:ietf:params:oauth:token-type:access_token}.
   *
   * @see TokenExchangeConfig#ACTOR_TOKEN_TYPE
   */
  @ConfigOption(ACTOR_TOKEN_TYPE)
  Optional<TokenTypeURI> actorTokenType();

  /** The type of the requested token. Must be a valid URN. Optional. */
  @ConfigOption(REQUESTED_TOKEN_TYPE)
  Optional<TokenTypeURI> requestedTokenType();

  /**
   * One or more URIs that indicate the target service(s) or resource(s) where the client intends to
   * use the requested token.
   *
   * <p>Optional. Can be a single value or a comma-separated list of values.
   */
  @ConfigOption(RESOURCES)
  List<URI> resources();

  /**
   * The logical name(s) of the target service where the client intends to use the requested token.
   * This serves a purpose similar to the resource parameter but with the client providing a logical
   * name for the target service.
   *
   * <p>Optional. Can be a single value or a comma-separated list of values.
   */
  @ConfigOption(AUDIENCES)
  List<Audience> audiences();

  static ImmutableTokenExchangeConfig.Builder parse(Map<String, String> properties) {
    return ImmutableTokenExchangeConfig.builder()
        .subjectTokenString(ConfigUtil.parseOptional(properties, SUBJECT_TOKEN))
        .subjectTokenType(
            ConfigUtil.parseOptional(properties, SUBJECT_TOKEN_TYPE, TokenTypeURI::parse))
        .actorTokenString(ConfigUtil.parseOptional(properties, ACTOR_TOKEN))
        .actorTokenType(ConfigUtil.parseOptional(properties, ACTOR_TOKEN_TYPE, TokenTypeURI::parse))
        .requestedTokenType(
            ConfigUtil.parseOptional(properties, REQUESTED_TOKEN_TYPE, TokenTypeURI::parse))
        .resources(ConfigUtil.parseList(properties, RESOURCES, ",", URI::create))
        .audiences(ConfigUtil.parseList(properties, AUDIENCES, ",", Audience::new));
  }
}
