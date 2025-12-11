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

import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.DEFAULT_CLIENT_ID;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_LEGACY_OPTION;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_MISSING_TOKEN_ENDPOINT;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_NO_CLIENT_ID;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_RELATIVE_TOKEN_ENDPOINT;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED;
import static org.apache.iceberg.rest.auth.oauth2.ConfigMigrator.MESSAGE_TEMPLATE_VENDED_TOKEN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.list;

import com.nimbusds.oauth2.sdk.GrantType;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.Scope;
import com.nimbusds.oauth2.sdk.auth.Secret;
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("deprecation")
class TestConfigMigrator {

  private List<Pair<String, List<String>>> messages;
  private BiConsumer<String, String[]> consumer;

  @BeforeEach
  void before() {
    messages = Lists.newArrayList();
    consumer = (msg, args) -> messages.add(Pair.of(msg, List.of(args)));
  }

  @AfterEach
  void after() {
    messages.clear();
  }

  // Legacy properties migration tests

  @Test
  void noLegacyProperties() {
    Map<String, String> input =
        Map.of(
            BasicConfig.TOKEN_ENDPOINT,
            "https://example.com/token",
            BasicConfig.CLIENT_ID,
            "client1",
            BasicConfig.CLIENT_SECRET,
            "secret",
            "non.oauth2.property",
            "value");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    // Only OAuth2 properties should be included
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.TOKEN_ENDPOINT,
                "https://example.com/token",
                BasicConfig.CLIENT_ID,
                "client1",
                BasicConfig.CLIENT_SECRET,
                "secret"));
    assertThat(messages).isEmpty();
  }

  @Test
  void credentialValid() {
    Map<String, String> input = Map.of(OAuth2Properties.CREDENTIAL, "client1:secret1");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual)
        .isEqualTo(Map.of(BasicConfig.CLIENT_ID, "client1", BasicConfig.CLIENT_SECRET, "secret1"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.CREDENTIAL,
            "s",
            BasicConfig.CLIENT_ID + " and " + BasicConfig.CLIENT_SECRET);
  }

  @Test
  void credentialNoClientId() {
    Map<String, String> input = Map.of(OAuth2Properties.CREDENTIAL, "secret1");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.CLIENT_ID,
                DEFAULT_CLIENT_ID.getValue(),
                BasicConfig.CLIENT_SECRET,
                "secret1"));
    assertThat(messages).hasSize(2);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.CREDENTIAL,
            "s",
            BasicConfig.CLIENT_ID + " and " + BasicConfig.CLIENT_SECRET);
    assertThatMessage(messages.get(1), MESSAGE_TEMPLATE_NO_CLIENT_ID)
        .containsExactly(DEFAULT_CLIENT_ID.getValue());
  }

  @Test
  void token() {
    Map<String, String> input = Map.of(OAuth2Properties.TOKEN, "access-token-123");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(BasicConfig.TOKEN, "access-token-123"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(OAuth2Properties.TOKEN, "", BasicConfig.TOKEN);
  }

  @Test
  void tokenExpiresInMs() {
    Map<String, String> input = Map.of(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "300000");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN, Duration.ofMillis(300000).toString()));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.TOKEN_EXPIRES_IN_MS, "", TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tokenRefreshEnabled(boolean enabled) {
    Map<String, String> input =
        Map.of(OAuth2Properties.TOKEN_REFRESH_ENABLED, String.valueOf(enabled));
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(TokenRefreshConfig.ENABLED, String.valueOf(enabled)));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(OAuth2Properties.TOKEN_REFRESH_ENABLED, "", TokenRefreshConfig.ENABLED);
  }

  @Test
  void oauth2ServerUri() {
    Map<String, String> input =
        Map.of(OAuth2Properties.OAUTH2_SERVER_URI, "https://example.com/token");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(BasicConfig.TOKEN_ENDPOINT, "https://example.com/token"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.OAUTH2_SERVER_URI,
            "s",
            BasicConfig.ISSUER_URL + " or " + BasicConfig.TOKEN_ENDPOINT);
  }

  @Test
  void scope() {
    Map<String, String> input = Map.of(OAuth2Properties.SCOPE, "read write admin");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(BasicConfig.SCOPE, "read write admin"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(OAuth2Properties.SCOPE, "", BasicConfig.SCOPE);
  }

  @Test
  void audience() {
    Map<String, String> input = Map.of(OAuth2Properties.AUDIENCE, "https://api.example.com");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(TokenExchangeConfig.AUDIENCES, "https://api.example.com"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(OAuth2Properties.AUDIENCE, "", TokenExchangeConfig.AUDIENCES);
  }

  @Test
  void resource() {
    Map<String, String> input = Map.of(OAuth2Properties.RESOURCE, "urn:example:resource");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(Map.of(TokenExchangeConfig.RESOURCES, "urn:example:resource"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(OAuth2Properties.RESOURCE, "", TokenExchangeConfig.RESOURCES);
  }

  @ParameterizedTest
  @MethodSource
  void vendedTokenExchange(String tokenTypeProperty) {
    Map<String, String> input = Map.of(tokenTypeProperty, "some-value");
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.GRANT_TYPE,
                GrantType.TOKEN_EXCHANGE.getValue(),
                TokenExchangeConfig.SUBJECT_TOKEN,
                "some-value",
                TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
                tokenTypeProperty,
                TokenExchangeConfig.ACTOR_TOKEN,
                ConfigUtil.PARENT_TOKEN));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            tokenTypeProperty,
            "s",
            TokenExchangeConfig.SUBJECT_TOKEN
                + ", "
                + TokenExchangeConfig.SUBJECT_TOKEN_TYPE
                + " and "
                + TokenExchangeConfig.ACTOR_TOKEN);
  }

  static Stream<String> vendedTokenExchange() {
    return Stream.of(
        OAuth2Properties.ACCESS_TOKEN_TYPE,
        OAuth2Properties.ID_TOKEN_TYPE,
        OAuth2Properties.SAML1_TOKEN_TYPE,
        OAuth2Properties.SAML2_TOKEN_TYPE,
        OAuth2Properties.JWT_TOKEN_TYPE,
        OAuth2Properties.REFRESH_TOKEN_TYPE);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void tokenExchangeEnabled(boolean enabled) {
    Map<String, String> input =
        Map.of(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, String.valueOf(enabled));
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual)
        .isEqualTo(Map.of(TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED, String.valueOf(enabled)));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "", TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED);
  }

  @Test
  void fullLegacyMigrationScenario() {
    Map<String, String> input =
        ImmutableMap.<String, String>builder()
            .put(OAuth2Properties.CREDENTIAL, "client1:secret1")
            .put(OAuth2Properties.TOKEN_EXPIRES_IN_MS, "300000")
            .put(OAuth2Properties.TOKEN_REFRESH_ENABLED, "true")
            .put(OAuth2Properties.OAUTH2_SERVER_URI, "custom/path/to/token")
            .put(OAuth2Properties.SCOPE, "read write")
            .put(OAuth2Properties.AUDIENCE, "https://api.example.com")
            .put(OAuth2Properties.RESOURCE, "urn:example:resource")
            .put(OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "false")
            .put(
                BasicConfig.ISSUER_URL,
                "https://idp.example.com") // New property should be preserved
            .put("non.oauth2.property", "ignored") // Non-OAuth2 property should be filtered out
            .build();

    Map<String, String> expected =
        ImmutableMap.<String, String>builder()
            .put(BasicConfig.CLIENT_ID, "client1")
            .put(BasicConfig.CLIENT_SECRET, "secret1")
            .put(BasicConfig.TOKEN_ENDPOINT, "custom/path/to/token")
            .put(BasicConfig.ISSUER_URL, "https://idp.example.com")
            .put(BasicConfig.SCOPE, "read write")
            .put(TokenRefreshConfig.ENABLED, "true")
            .put(TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED, "false")
            .put(TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN, Duration.ofMillis(300000).toString())
            .put(TokenExchangeConfig.RESOURCES, "urn:example:resource")
            .put(TokenExchangeConfig.AUDIENCES, "https://api.example.com")
            .build();

    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(expected);

    assertThat(messages).hasSize(8);
    List<String> legacyProperties =
        messages.stream().map(Pair::second).map(args -> args.get(0)).collect(Collectors.toList());

    assertThat(legacyProperties)
        .containsExactlyInAnyOrder(
            OAuth2Properties.CREDENTIAL,
            OAuth2Properties.TOKEN_EXPIRES_IN_MS,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.OAUTH2_SERVER_URI,
            OAuth2Properties.SCOPE,
            OAuth2Properties.AUDIENCE,
            OAuth2Properties.RESOURCE,
            OAuth2Properties.TOKEN_EXCHANGE_ENABLED);
  }

  @ParameterizedTest
  @MethodSource
  void newPropertyOverridesLegacy(
      Map<String, String> input, Map<String, String> expectedOutput, String[] expectedWarningArgs) {
    Map<String, String> actual = new ConfigMigrator(consumer).migrateProperties(input);
    assertThat(actual).isEqualTo(expectedOutput);
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(expectedWarningArgs);
  }

  static Stream<Arguments> newPropertyOverridesLegacy() {
    return Stream.of(
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.CREDENTIAL,
                "legacy-client:legacy-secret",
                BasicConfig.CLIENT_ID,
                "new-client",
                BasicConfig.CLIENT_SECRET,
                "new-secret"),
            Map.of(BasicConfig.CLIENT_ID, "new-client", BasicConfig.CLIENT_SECRET, "new-secret"),
            new String[] {
              OAuth2Properties.CREDENTIAL,
              "s",
              BasicConfig.CLIENT_ID + " and " + BasicConfig.CLIENT_SECRET
            }),
        Arguments.of(
            ImmutableMap.of(OAuth2Properties.TOKEN, "legacy-token", BasicConfig.TOKEN, "new-token"),
            Map.of(BasicConfig.TOKEN, "new-token"),
            new String[] {OAuth2Properties.TOKEN, "", BasicConfig.TOKEN}),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.OAUTH2_SERVER_URI,
                "https://legacy.example.com/token",
                BasicConfig.TOKEN_ENDPOINT,
                "https://new.example.com/token"),
            Map.of(BasicConfig.TOKEN_ENDPOINT, "https://new.example.com/token"),
            new String[] {
              OAuth2Properties.OAUTH2_SERVER_URI,
              "s",
              BasicConfig.ISSUER_URL + " or " + BasicConfig.TOKEN_ENDPOINT
            }),
        Arguments.of(
            ImmutableMap.of(OAuth2Properties.SCOPE, "legacy-scope", BasicConfig.SCOPE, "new-scope"),
            Map.of(BasicConfig.SCOPE, "new-scope"),
            new String[] {OAuth2Properties.SCOPE, "", BasicConfig.SCOPE}),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.AUDIENCE,
                "https://legacy.example.com",
                TokenExchangeConfig.AUDIENCES,
                "https://new.example.com"),
            Map.of(TokenExchangeConfig.AUDIENCES, "https://new.example.com"),
            new String[] {OAuth2Properties.AUDIENCE, "", TokenExchangeConfig.AUDIENCES}),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.RESOURCE,
                "urn:legacy:resource",
                TokenExchangeConfig.RESOURCES,
                "urn:new:resource"),
            Map.of(TokenExchangeConfig.RESOURCES, "urn:new:resource"),
            new String[] {OAuth2Properties.RESOURCE, "", TokenExchangeConfig.RESOURCES}),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.TOKEN_REFRESH_ENABLED,
                "false",
                TokenRefreshConfig.ENABLED,
                "true"),
            Map.of(TokenRefreshConfig.ENABLED, "true"),
            new String[] {OAuth2Properties.TOKEN_REFRESH_ENABLED, "", TokenRefreshConfig.ENABLED}),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
                "false",
                TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED,
                "true"),
            Map.of(TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED, "true"),
            new String[] {
              OAuth2Properties.TOKEN_EXCHANGE_ENABLED, "", TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED
            }),
        Arguments.of(
            ImmutableMap.of(
                OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                "300000",
                TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN,
                "PT10M"),
            Map.of(TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN, "PT10M"),
            new String[] {
              OAuth2Properties.TOKEN_EXPIRES_IN_MS, "", TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN
            }));
  }

  // Token endpoint URL handling tests

  @ParameterizedTest
  @CsvSource({
    "https://example.com      , /oauth2/token , https://example.com/oauth2/token",
    "https://example.com      ,  oauth2/token , https://example.com/oauth2/token",
    "https://example.com/     , /oauth2/token , https://example.com/oauth2/token",
    "https://example.com/     ,  oauth2/token , https://example.com/oauth2/token",
    "https://example.com/api  , /oauth2/token , https://example.com/api/oauth2/token",
    "https://example.com/api/ ,  oauth2/token , https://example.com/api/oauth2/token"
  })
  void legacyTokenEndpoint(String catalogUri, String oauth2ServerUri, String expected) {
    Map<String, String> input = Map.of(OAuth2Properties.OAUTH2_SERVER_URI, oauth2ServerUri);
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, catalogUri);
    assertThat(actual).isEqualTo(Map.of(BasicConfig.TOKEN_ENDPOINT, expected));
    assertThat(messages).hasSize(2);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_LEGACY_OPTION)
        .containsExactly(
            OAuth2Properties.OAUTH2_SERVER_URI,
            "s",
            BasicConfig.ISSUER_URL + " or " + BasicConfig.TOKEN_ENDPOINT);
    assertThatMessage(messages.get(1), MESSAGE_TEMPLATE_RELATIVE_TOKEN_ENDPOINT)
        .containsExactly(expected);
  }

  @ParameterizedTest
  @CsvSource({
    "https://example.com      , https://example.com/v1/oauth/tokens",
    "https://example.com/     , https://example.com/v1/oauth/tokens",
    "https://example.com/api  , https://example.com/api/v1/oauth/tokens",
    "https://example.com/api/ , https://example.com/api/v1/oauth/tokens"
  })
  void tokenEndpointMissing(String catalogUri, String expected) {
    Map<String, String> input =
        Map.of(BasicConfig.CLIENT_ID, "client-id", BasicConfig.CLIENT_SECRET, "client-secret");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, catalogUri);
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.CLIENT_ID,
                "client-id",
                BasicConfig.CLIENT_SECRET,
                "client-secret",
                BasicConfig.TOKEN_ENDPOINT,
                expected));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_MISSING_TOKEN_ENDPOINT)
        .containsExactly(expected, BasicConfig.TOKEN_ENDPOINT, BasicConfig.ISSUER_URL);
  }

  @Test
  void tokenEndpointMissingWithIssuerUrl() {
    Map<String, String> input =
        Map.of(
            BasicConfig.ISSUER_URL,
            "https://issuer.com",
            BasicConfig.CLIENT_ID,
            "client-id",
            BasicConfig.CLIENT_SECRET,
            "client-secret");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, "https://catalog.com");
    assertThat(actual).isEqualTo(input);
    assertThat(messages).hasSize(0);
  }

  @Test
  void tokenEndpointMissingWithStaticToken() {
    Map<String, String> input = Map.of(BasicConfig.TOKEN, "static-token");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, "https://catalog.com");
    assertThat(actual).isEqualTo(input);
    assertThat(messages).hasSize(0);
  }

  @Test
  void tokenEndpointRelative() {
    Map<String, String> input =
        Map.of(
            BasicConfig.TOKEN_ENDPOINT,
            "/relative/token/endpoint",
            BasicConfig.CLIENT_ID,
            "client-id",
            BasicConfig.CLIENT_SECRET,
            "client-secret");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, "https://catalog.com");
    assertThat(actual)
        .isEqualTo(
            Map.of(
                BasicConfig.TOKEN_ENDPOINT,
                "https://catalog.com/relative/token/endpoint",
                BasicConfig.CLIENT_ID,
                "client-id",
                BasicConfig.CLIENT_SECRET,
                "client-secret"));
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_RELATIVE_TOKEN_ENDPOINT)
        .containsExactly("https://catalog.com/relative/token/endpoint");
  }

  @Test
  void tokenEndpointAbsolute() {
    Map<String, String> input =
        Map.of(
            BasicConfig.TOKEN_ENDPOINT,
            "https://token-endpoint.com/token",
            BasicConfig.CLIENT_ID,
            "client-id",
            BasicConfig.CLIENT_SECRET,
            "client-secret");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    Map<String, String> actual = migrator.migrateProperties(input);
    migrator.handleTokenEndpoint(actual, "https://catalog.com");
    assertThat(actual).isEqualTo(input);
    assertThat(messages).hasSize(0);
  }

  // Full config migration tests

  @Test
  void migrateCatalogConfig() {
    Map<String, String> input =
        ImmutableMap.<String, String>builder()
            .put(BasicConfig.TOKEN_ENDPOINT, "https://example.com/token")
            .put(BasicConfig.CLIENT_ID, "client-id")
            .put(BasicConfig.CLIENT_SECRET, "client-secret")
            .build();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateCatalogConfig(input, "https://example.com");
    assertThat(actual)
        .isEqualTo(
            ImmutableOAuth2Config.builder()
                .basicConfig(
                    ImmutableBasicConfig.builder()
                        .tokenEndpoint(URI.create("https://example.com/token"))
                        .clientId(new ClientID("client-id"))
                        .clientSecret(new Secret("client-secret"))
                        .build())
                .build());
    assertThat(messages).hasSize(0);
  }

  @Test
  void migrateContextualConfigFromEmptyInputEmptyParent() {
    // minimal parent config with just a token
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .token(new TypelessAccessToken("access-token-123"))
                    .build())
            .build();
    Map<String, String> input = Map.of();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateContextualConfig(parent, input, "https://example.com");
    assertThat(actual).isSameAs(parent);
    assertThat(messages).hasSize(0);
  }

  @Test
  void migrateContextualConfigFromEmptyInputNonEmptyParent() {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .tokenExchangeConfig(
                ImmutableTokenExchangeConfig.builder()
                    .addAudiences(new Audience("parent-audience"))
                    .addResources(URI.create("parent-resource"))
                    .build())
            .build();
    Map<String, String> input = Map.of();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateContextualConfig(parent, input, "https://example.com");
    assertThat(actual).isEqualTo(parent);
    assertThat(messages).hasSize(6);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(BasicConfig.CLIENT_ID);
    assertThatMessage(messages.get(1), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(BasicConfig.CLIENT_SECRET);
    assertThatMessage(messages.get(2), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(BasicConfig.TOKEN_ENDPOINT);
    assertThatMessage(messages.get(3), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(BasicConfig.SCOPE);
    assertThatMessage(messages.get(4), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(TokenExchangeConfig.RESOURCES);
    assertThatMessage(messages.get(5), MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG)
        .containsExactly(TokenExchangeConfig.AUDIENCES);
  }

  @Test
  void migrateContextualConfigFromNonEmptyInputEmptyParent() {
    // minimal parent config with just a token
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .token(new TypelessAccessToken("access-token-123"))
                    .build())
            .build();
    Map<String, String> input =
        Map.of(
            BasicConfig.CLIENT_ID, "child-client-id",
            BasicConfig.CLIENT_SECRET, "child-client-secret",
            BasicConfig.TOKEN_ENDPOINT, "https://example.com/token/child",
            BasicConfig.SCOPE, "child-scope",
            TokenExchangeConfig.RESOURCES, "child-resource",
            TokenExchangeConfig.AUDIENCES, "child-audience");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateContextualConfig(parent, input, "https://example.com");
    assertThat(actual)
        .isEqualTo(
            ImmutableOAuth2Config.builder()
                .basicConfig(
                    ImmutableBasicConfig.builder()
                        .tokenEndpoint(URI.create("https://example.com/token/child"))
                        .clientId(new ClientID("child-client-id"))
                        .clientSecret(new Secret("child-client-secret"))
                        .scope(Scope.parse("child-scope"))
                        .build())
                .tokenExchangeConfig(
                    ImmutableTokenExchangeConfig.builder()
                        .addAudiences(new Audience("child-audience"))
                        .addResources(URI.create("child-resource"))
                        .build())
                .build());
    assertThat(messages).hasSize(0);
  }

  @Test
  void migrateContextualConfigFromNonEmptyInputNonEmptyParent() {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token/parent"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .tokenExchangeConfig(
                ImmutableTokenExchangeConfig.builder()
                    .addAudiences(new Audience("parent-audience"))
                    .addResources(URI.create("parent-resource"))
                    .build())
            .build();
    Map<String, String> input =
        Map.of(
            BasicConfig.CLIENT_ID, "child-client-id",
            BasicConfig.CLIENT_SECRET, "child-client-secret",
            BasicConfig.TOKEN_ENDPOINT, "https://example.com/token/child",
            BasicConfig.SCOPE, "child-scope",
            TokenExchangeConfig.RESOURCES, "child-resource",
            TokenExchangeConfig.AUDIENCES, "child-audience");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateContextualConfig(parent, input, "https://example.com");
    assertThat(actual)
        .isEqualTo(
            ImmutableOAuth2Config.builder()
                .basicConfig(
                    ImmutableBasicConfig.builder()
                        .tokenEndpoint(URI.create("https://example.com/token/child"))
                        .clientId(new ClientID("child-client-id"))
                        .clientSecret(new Secret("child-client-secret"))
                        .scope(Scope.parse("child-scope"))
                        .build())
                .tokenExchangeConfig(
                    ImmutableTokenExchangeConfig.builder()
                        .addAudiences(new Audience("child-audience"))
                        .addResources(URI.create("child-resource"))
                        .build())
                .build());
    assertThat(messages).hasSize(0);
  }

  @Test
  void migrateTableConfigFromEmptyInput() {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token/parent"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .build();
    Map<String, String> input = Map.of();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateTableConfig(parent, input);
    assertThat(actual).isEqualTo(parent);
    assertThat(messages).hasSize(0);
  }

  @Test
  void migrateTableConfigFromDisallowedInput() {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token/parent"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .build();
    // should be filtered out
    Map<String, String> input =
        ImmutableMap.of(
            BasicConfig.CLIENT_ID, "child-client-id",
            BasicConfig.CLIENT_SECRET, "child-client-secret",
            BasicConfig.SCOPE, "table-scope");
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateTableConfig(parent, input);
    assertThat(actual).isEqualTo(parent);
    assertThat(messages).hasSize(3);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED)
        .containsExactly(BasicConfig.CLIENT_ID);
    assertThatMessage(messages.get(1), MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED)
        .containsExactly(BasicConfig.CLIENT_SECRET);
    assertThatMessage(messages.get(2), MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED)
        .containsExactly(BasicConfig.SCOPE);
  }

  @Test
  void migrateTableConfigFromNonEmptyInputWithVendedStaticToken() {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token/parent"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .build();
    Map<String, String> input =
        ImmutableMap.<String, String>builder().put(BasicConfig.TOKEN, "access-token-123").build();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateTableConfig(parent, input);
    assertThat(actual)
        .isEqualTo(
            ImmutableOAuth2Config.builder()
                .basicConfig(
                    ImmutableBasicConfig.builder()
                        // from table config
                        .token(new TypelessAccessToken("access-token-123"))
                        // from parent config
                        .tokenEndpoint(URI.create("https://example.com/token/parent"))
                        .clientId(new ClientID("parent-client-id"))
                        .clientSecret(new Secret("parent-client-secret"))
                        .scope(Scope.parse("parent-scope"))
                        .build())
                .build());
    assertThat(messages).hasSize(1);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_VENDED_TOKEN)
        .containsExactly(BasicConfig.TOKEN);
  }

  @Test
  void migrateTableConfigFromNonEmptyInputWithVendedTokenExchange() throws ParseException {
    OAuth2Config parent =
        ImmutableOAuth2Config.builder()
            .basicConfig(
                ImmutableBasicConfig.builder()
                    .tokenEndpoint(URI.create("https://example.com/token/parent"))
                    .clientId(new ClientID("parent-client-id"))
                    .clientSecret(new Secret("parent-client-secret"))
                    .scope(Scope.parse("parent-scope"))
                    .build())
            .build();
    Map<String, String> input =
        ImmutableMap.<String, String>builder()
            .put(TokenExchangeConfig.SUBJECT_TOKEN, "id-token-123")
            .put(TokenExchangeConfig.SUBJECT_TOKEN_TYPE, OAuth2Properties.ID_TOKEN_TYPE)
            .put(TokenExchangeConfig.ACTOR_TOKEN, ConfigUtil.PARENT_TOKEN)
            .put(TokenExchangeConfig.ACTOR_TOKEN_TYPE, OAuth2Properties.ACCESS_TOKEN_TYPE)
            .build();
    ConfigMigrator migrator = new ConfigMigrator(consumer);
    OAuth2Config actual = migrator.migrateTableConfig(parent, input);
    assertThat(actual)
        .isEqualTo(
            ImmutableOAuth2Config.builder()
                // from parent config
                .basicConfig(
                    ImmutableBasicConfig.builder()
                        .grantType(GrantType.TOKEN_EXCHANGE)
                        .tokenEndpoint(URI.create("https://example.com/token/parent"))
                        .clientId(new ClientID("parent-client-id"))
                        .clientSecret(new Secret("parent-client-secret"))
                        .scope(Scope.parse("parent-scope"))
                        .build())
                // from table config
                .tokenExchangeConfig(
                    ImmutableTokenExchangeConfig.builder()
                        .subjectTokenString("id-token-123")
                        .subjectTokenType(TokenTypeURI.parse(OAuth2Properties.ID_TOKEN_TYPE))
                        .actorTokenString(ConfigUtil.PARENT_TOKEN)
                        .actorTokenType(TokenTypeURI.parse(OAuth2Properties.ACCESS_TOKEN_TYPE))
                        .build())
                .build());
    assertThat(messages).hasSize(4);
    assertThatMessage(messages.get(0), MESSAGE_TEMPLATE_VENDED_TOKEN)
        .containsExactly(TokenExchangeConfig.SUBJECT_TOKEN);
    assertThatMessage(messages.get(1), MESSAGE_TEMPLATE_VENDED_TOKEN)
        .containsExactly(TokenExchangeConfig.SUBJECT_TOKEN_TYPE);
    assertThatMessage(messages.get(2), MESSAGE_TEMPLATE_VENDED_TOKEN)
        .containsExactly(TokenExchangeConfig.ACTOR_TOKEN);
    assertThatMessage(messages.get(3), MESSAGE_TEMPLATE_VENDED_TOKEN)
        .containsExactly(TokenExchangeConfig.ACTOR_TOKEN_TYPE);
  }

  private static ListAssert<String> assertThatMessage(
      Pair<String, List<String>> message, String template) {
    assertThat(message).extracting(Pair::first).isEqualTo(template);
    return assertThat(message).extracting(Pair::second).asInstanceOf(list(String.class));
  }
}
