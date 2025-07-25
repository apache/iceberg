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
import com.nimbusds.oauth2.sdk.id.Audience;
import com.nimbusds.oauth2.sdk.id.ClientID;
import com.nimbusds.oauth2.sdk.token.TokenTypeURI;
import com.nimbusds.oauth2.sdk.token.TypelessAccessToken;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.auth.oauth2.ImmutableOAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component for migrating from legacy OAuth2 properties (from {@link OAuth2Properties}) to the
 * new OAuth2 properties (as declared in {@link OAuth2Config}).
 */
@SuppressWarnings("deprecation")
public final class ConfigMigrator {

  /**
   * The default client ID to use when no client ID is provided in the legacy {@link
   * OAuth2Properties#CREDENTIAL} property.
   */
  public static final ClientID DEFAULT_CLIENT_ID = new ClientID("iceberg");

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigMigrator.class);

  private static final Splitter CREDENTIAL_SPLITTER = Splitter.on(":").limit(2).trimResults();

  private static final Set<String> TABLE_CONFIG_ALLOW_LIST =
      Set.of(
          BasicConfig.TOKEN,
          TokenExchangeConfig.SUBJECT_TOKEN,
          TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
          TokenExchangeConfig.ACTOR_TOKEN,
          TokenExchangeConfig.ACTOR_TOKEN_TYPE);

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_LEGACY_OPTION =
      "Detected legacy OAuth2 property '{}', please use option{} {} instead.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_NO_CLIENT_ID =
      "The legacy OAuth2 property 'credential' was provided, but it did not contain a client ID; assuming '{}'.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_MISSING_TOKEN_ENDPOINT =
      "The OAuth2 configuration does not specify a token endpoint nor an issuer URL: "
          + "the token endpoint URL will default to {}. "
          + "This automatic fallback will be removed in a future Iceberg release. "
          + "Please configure OAuth2 endpoints using the following properties: '{}' or '{}'. "
          + "This warning will disappear if OAuth2 endpoints are properly configured. "
          + "See https://github.com/apache/iceberg/issues/10537";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_RELATIVE_TOKEN_ENDPOINT =
      "The OAuth2 token endpoint URL is a relative URL. "
          + "It will be resolved against the catalog URI, resulting in the absolute URL: '{}'. "
          + "This automatic fallback will be removed in a future Iceberg release. "
          + "Please configure OAuth2 endpoints using absolute URLs.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG =
      "The OAuth2 configuration property '{}' was not found in the context session, "
          + "and will be inherited from the parent session. "
          + "This automatic fallback will be removed in a future Iceberg release.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_VENDED_TOKEN =
      "The OAuth2 configuration property '{}' was found in the table configuration "
          + "and indicates that the catalog server vended an OAuth2 token. "
          + "Vended OAuth2 tokens will be disallowed in a future Iceberg release.";

  @VisibleForTesting
  static final String MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED =
      "The OAuth2 configuration property '{}' is not allowed to be vended by catalog servers.";

  private final BiConsumer<String, String[]> logConsumer;

  public ConfigMigrator() {
    this(LOGGER);
  }

  public ConfigMigrator(Logger logger) {
    this(logger::warn);
  }

  @VisibleForTesting
  ConfigMigrator(BiConsumer<String, String[]> logConsumer) {
    this.logConsumer = logConsumer;
  }

  /**
   * Migrates catalog-level properties.
   *
   * <p>Legacy Iceberg OAuth2 properties are migrated, and warnings are logged for each detected
   * legacy property.
   *
   * <p>The migration will further check the token endpoint for the following legacy situations:
   *
   * <ol>
   *   <li>If no token endpoint is provided, a default one will be added to the migrated properties;
   *       its value is the catalog URI + {@link ResourcePaths#tokens()} – that is, it will point to
   *       the (deprecated) REST Catalog token endpoint.
   *   <li>If a token endpoint is provided, but is a relative path, it will be resolved against the
   *       catalog URI.
   * </ol>
   *
   * In both cases, a warning will be logged.
   *
   * @param properties The properties to migrate
   * @param catalogUri The catalog URI, for extended token endpoint checks
   */
  public OAuth2Config migrateCatalogConfig(Map<String, String> properties, String catalogUri) {
    Map<String, String> migrated = migrateProperties(properties);
    handleTokenEndpoint(migrated, catalogUri);
    return OAuth2Config.of(migrated);
  }

  /**
   * Migrates session context properties.
   *
   * <p>Legacy Iceberg OAuth2 properties are migrated, and warnings are logged for each detected
   * legacy property.
   *
   * <p>See {@link #migrateCatalogConfig(Map, String)} for details on token endpoint checks.
   *
   * <p>Contextual configs inherit some properties from their parent config. This is legacy
   * behavior, and will be removed after 1.13; a warning will be logged when this happens.
   *
   * @param parent The parent config
   * @param properties The properties to migrate
   * @param catalogUri The catalog URI, for extended token endpoint checks
   */
  @SuppressWarnings("CyclomaticComplexity")
  public OAuth2Config migrateContextualConfig(
      OAuth2Config parent, Map<String, String> properties, String catalogUri) {
    Map<String, String> migrated = migrateProperties(properties);

    if (!migrated.containsKey(BasicConfig.CLIENT_ID)
        && parent.basicConfig().clientId().isPresent()) {
      warnOnMergedContextualConfig(BasicConfig.CLIENT_ID);
      migrated.put(BasicConfig.CLIENT_ID, parent.basicConfig().clientId().get().getValue());
    }

    if (!migrated.containsKey(BasicConfig.CLIENT_SECRET)
        && !migrated.containsKey(BasicConfig.CLIENT_AUTH)
        && parent.basicConfig().clientSecret().isPresent()) {
      warnOnMergedContextualConfig(BasicConfig.CLIENT_SECRET);
      migrated.put(BasicConfig.CLIENT_SECRET, parent.basicConfig().clientSecret().get().getValue());
    }

    if (!migrated.containsKey(BasicConfig.TOKEN_ENDPOINT)
        && !migrated.containsKey(BasicConfig.ISSUER_URL)
        && parent.basicConfig().tokenEndpoint().isPresent()) {
      warnOnMergedContextualConfig(BasicConfig.TOKEN_ENDPOINT);
      migrated.put(
          BasicConfig.TOKEN_ENDPOINT, parent.basicConfig().tokenEndpoint().get().toString());
    }

    if (!migrated.containsKey(BasicConfig.SCOPE) && parent.basicConfig().scope().isPresent()) {
      warnOnMergedContextualConfig(BasicConfig.SCOPE);
      migrated.put(BasicConfig.SCOPE, parent.basicConfig().scope().get().toString());
    }

    if (!migrated.containsKey(TokenExchangeConfig.RESOURCES)
        && !parent.tokenExchangeConfig().resources().isEmpty()) {
      warnOnMergedContextualConfig(TokenExchangeConfig.RESOURCES);
      migrated.put(
          TokenExchangeConfig.RESOURCES,
          parent.tokenExchangeConfig().resources().stream()
              .map(URI::toString)
              .collect(Collectors.joining(",")));
    }

    if (!migrated.containsKey(TokenExchangeConfig.AUDIENCES)
        && !parent.tokenExchangeConfig().audiences().isEmpty()) {
      warnOnMergedContextualConfig(TokenExchangeConfig.AUDIENCES);
      migrated.put(
          TokenExchangeConfig.AUDIENCES,
          parent.tokenExchangeConfig().audiences().stream()
              .map(Audience::getValue)
              .collect(Collectors.joining(",")));
    }

    if (migrated.isEmpty()) {
      return parent;
    }

    handleTokenEndpoint(migrated, catalogUri);

    return OAuth2Config.of(migrated);
  }

  /**
   * Migrates table properties.
   *
   * <p>Legacy Iceberg OAuth2 properties are migrated, and warnings are logged for each detected
   * legacy property.
   *
   * <p>Table configs are not allowed to contain any property that is not in the allow list; a
   * warning will be logged for each detected disallowed property.
   *
   * <p>Moreover, table configs are considered deprecated, and a warning will be logged on any
   * OAuth2 property found, even if it is allowed.
   *
   * @param parent The parent config
   * @param properties The properties to migrate
   */
  public OAuth2Config migrateTableConfig(OAuth2Config parent, Map<String, String> properties) {

    Map<String, String> migrated = migrateProperties(properties);
    Map<String, String> filtered = Maps.newHashMap();

    for (Entry<String, String> entry : migrated.entrySet()) {
      if (TABLE_CONFIG_ALLOW_LIST.contains(entry.getKey())) {
        warnOnVendedToken(entry.getKey());
        filtered.put(entry.getKey(), entry.getValue());
      } else {
        warnOnForbiddenTableConfig(entry.getKey());
      }
    }

    ImmutableBasicConfig.Builder basicBuilder =
        ImmutableBasicConfig.builder().from(parent.basicConfig());
    ImmutableTokenExchangeConfig.Builder tokenExchangeBuilder =
        ImmutableTokenExchangeConfig.builder().from(parent.tokenExchangeConfig());

    if (filtered.containsKey(BasicConfig.TOKEN)) {
      // static vended token use case
      basicBuilder.token(
          ConfigUtil.parseOptional(filtered, BasicConfig.TOKEN, TypelessAccessToken::new));
    } else {
      // vended token exchange use cases
      if (filtered.containsKey(TokenExchangeConfig.SUBJECT_TOKEN)) {
        basicBuilder.grantType(GrantType.TOKEN_EXCHANGE);
        tokenExchangeBuilder.subjectTokenString(
            ConfigUtil.parseOptional(filtered, TokenExchangeConfig.SUBJECT_TOKEN));
      }

      if (filtered.containsKey(TokenExchangeConfig.SUBJECT_TOKEN_TYPE)) {
        tokenExchangeBuilder.subjectTokenType(
            ConfigUtil.parseOptional(
                    filtered, TokenExchangeConfig.SUBJECT_TOKEN_TYPE, TokenTypeURI::parse)
                .orElse(TokenTypeURI.ACCESS_TOKEN));
      }

      if (filtered.containsKey(TokenExchangeConfig.ACTOR_TOKEN)) {
        tokenExchangeBuilder.actorTokenString(
            ConfigUtil.parseOptional(filtered, TokenExchangeConfig.ACTOR_TOKEN));
      }

      if (filtered.containsKey(TokenExchangeConfig.ACTOR_TOKEN_TYPE)) {
        tokenExchangeBuilder.actorTokenType(
            ConfigUtil.parseOptional(
                    filtered, TokenExchangeConfig.ACTOR_TOKEN_TYPE, TokenTypeURI::parse)
                .orElse(TokenTypeURI.ACCESS_TOKEN));
      }
    }

    return ImmutableOAuth2Config.builder()
        .from(parent)
        .basicConfig(basicBuilder.build())
        .tokenExchangeConfig(tokenExchangeBuilder.build())
        .build();
  }

  @VisibleForTesting
  Map<String, String> migrateProperties(Map<String, String> properties) {
    Map<String, String> migrated = Maps.newLinkedHashMap();
    for (Entry<String, String> entry : properties.entrySet()) {
      switch (entry.getKey()) {
        case OAuth2Properties.TOKEN:
          warnOnLegacyOption(entry.getKey(), BasicConfig.TOKEN);
          migrated.put(BasicConfig.TOKEN, entry.getValue());
          break;
        case OAuth2Properties.CREDENTIAL:
          warnOnLegacyOption(
              entry.getKey(), true, BasicConfig.CLIENT_ID, BasicConfig.CLIENT_SECRET);
          List<String> parts = CREDENTIAL_SPLITTER.splitToList(entry.getValue());
          if (parts.size() == 2) {
            migrated.put(BasicConfig.CLIENT_ID, parts.get(0));
            migrated.put(BasicConfig.CLIENT_SECRET, parts.get(1));
          } else {
            logConsumer.accept(
                MESSAGE_TEMPLATE_NO_CLIENT_ID, new String[] {DEFAULT_CLIENT_ID.getValue()});
            migrated.put(BasicConfig.CLIENT_ID, DEFAULT_CLIENT_ID.getValue());
            migrated.put(BasicConfig.CLIENT_SECRET, parts.get(0));
          }

          break;
        case OAuth2Properties.TOKEN_EXPIRES_IN_MS:
          warnOnLegacyOption(entry.getKey(), TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN);
          Duration duration =
              Duration.ofMillis(
                  PropertyUtil.propertyAsLong(
                      properties,
                      OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                      OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT));
          migrated.put(TokenRefreshConfig.ACCESS_TOKEN_LIFESPAN, duration.toString());
          break;
        case OAuth2Properties.TOKEN_REFRESH_ENABLED:
          warnOnLegacyOption(entry.getKey(), TokenRefreshConfig.ENABLED);
          migrated.put(
              TokenRefreshConfig.ENABLED, String.valueOf(Boolean.parseBoolean(entry.getValue())));
          break;
        case OAuth2Properties.OAUTH2_SERVER_URI:
          warnOnLegacyOption(
              entry.getKey(), false, BasicConfig.ISSUER_URL, BasicConfig.TOKEN_ENDPOINT);
          migrated.put(BasicConfig.TOKEN_ENDPOINT, entry.getValue());
          break;
        case OAuth2Properties.SCOPE:
          warnOnLegacyOption(entry.getKey(), BasicConfig.SCOPE);
          migrated.put(BasicConfig.SCOPE, entry.getValue());
          break;
        case OAuth2Properties.AUDIENCE:
          warnOnLegacyOption(entry.getKey(), TokenExchangeConfig.AUDIENCES);
          migrated.put(TokenExchangeConfig.AUDIENCES, entry.getValue());
          break;
        case OAuth2Properties.RESOURCE:
          warnOnLegacyOption(entry.getKey(), TokenExchangeConfig.RESOURCES);
          migrated.put(TokenExchangeConfig.RESOURCES, entry.getValue());
          break;
        case OAuth2Properties.ACCESS_TOKEN_TYPE:
        case OAuth2Properties.ID_TOKEN_TYPE:
        case OAuth2Properties.SAML1_TOKEN_TYPE:
        case OAuth2Properties.SAML2_TOKEN_TYPE:
        case OAuth2Properties.JWT_TOKEN_TYPE:
        case OAuth2Properties.REFRESH_TOKEN_TYPE:
          warnOnLegacyOption(
              entry.getKey(),
              true,
              TokenExchangeConfig.SUBJECT_TOKEN,
              TokenExchangeConfig.SUBJECT_TOKEN_TYPE,
              TokenExchangeConfig.ACTOR_TOKEN);
          migrated.put(BasicConfig.GRANT_TYPE, GrantType.TOKEN_EXCHANGE.getValue());
          migrated.put(TokenExchangeConfig.SUBJECT_TOKEN, entry.getValue());
          migrated.put(TokenExchangeConfig.SUBJECT_TOKEN_TYPE, entry.getKey());
          migrated.put(TokenExchangeConfig.ACTOR_TOKEN, ConfigUtil.PARENT_TOKEN);
          break;
        case OAuth2Properties.TOKEN_EXCHANGE_ENABLED:
          warnOnLegacyOption(entry.getKey(), TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED);
          migrated.put(TokenRefreshConfig.TOKEN_EXCHANGE_ENABLED, entry.getValue());
          break;
      }
    }

    // preserve new properties, overriding legacy properties if any
    for (Entry<String, String> entry : properties.entrySet()) {
      if (entry.getKey().startsWith(OAuth2Config.PREFIX)) {
        migrated.put(entry.getKey(), entry.getValue());
      }
    }

    return migrated;
  }

  @VisibleForTesting
  void handleTokenEndpoint(Map<String, String> migrated, String catalogUri) {

    Preconditions.checkNotNull(catalogUri, "Catalog URI is required");

    String tokenEndpoint = migrated.get(BasicConfig.TOKEN_ENDPOINT);
    String issuerUrl = migrated.get(BasicConfig.ISSUER_URL);
    String token = migrated.get(BasicConfig.TOKEN);

    if (tokenEndpoint == null && issuerUrl == null && token == null) {

      // No token endpoint or issuer URL configured, and no static token:
      // default the token endpoint to catalog URI + ResourcePaths.tokens()
      tokenEndpoint = RESTUtil.resolveEndpoint(catalogUri, ResourcePaths.tokens());
      migrated.put(BasicConfig.TOKEN_ENDPOINT, tokenEndpoint);
      warnOnMissingTokenEndpoint(tokenEndpoint);

    } else if (tokenEndpoint != null && !URI.create(tokenEndpoint).isAbsolute()) {

      // If the token endpoint was provided, but is a relative path:
      // assume it's an endpoint internal to the catalog server
      // and resolve it against the catalog URI
      tokenEndpoint = RESTUtil.resolveEndpoint(catalogUri, tokenEndpoint);
      migrated.put(BasicConfig.TOKEN_ENDPOINT, tokenEndpoint);
      warnOnRelativeTokenEndpoint(tokenEndpoint);
    }
  }

  private void warnOnLegacyOption(String icebergOption, String authManagerOption) {
    warnOnLegacyOption(icebergOption, false, authManagerOption);
  }

  private void warnOnLegacyOption(String legacyOption, boolean and, String... newOptions) {
    List<String> options = Lists.newArrayList(newOptions);
    String joined =
        options.size() == 1
            ? options.get(0)
            : options.stream().limit(options.size() - 1).collect(Collectors.joining(", "))
                + (and ? " and " : " or ")
                + options.get(options.size() - 1);
    logConsumer.accept(
        MESSAGE_TEMPLATE_LEGACY_OPTION,
        new String[] {legacyOption, options.size() == 1 ? "" : "s", joined});
  }

  private void warnOnMissingTokenEndpoint(String tokenEndpoint) {
    logConsumer.accept(
        MESSAGE_TEMPLATE_MISSING_TOKEN_ENDPOINT,
        new String[] {
          tokenEndpoint, BasicConfig.TOKEN_ENDPOINT, BasicConfig.ISSUER_URL,
        });
  }

  private void warnOnRelativeTokenEndpoint(String tokenEndpoint) {
    logConsumer.accept(MESSAGE_TEMPLATE_RELATIVE_TOKEN_ENDPOINT, new String[] {tokenEndpoint});
  }

  private void warnOnMergedContextualConfig(String mergedOption) {
    logConsumer.accept(MESSAGE_TEMPLATE_MERGED_CONTEXTUAL_CONFIG, new String[] {mergedOption});
  }

  private void warnOnVendedToken(String vendedOption) {
    logConsumer.accept(MESSAGE_TEMPLATE_VENDED_TOKEN, new String[] {vendedOption});
  }

  private void warnOnForbiddenTableConfig(String tableOption) {
    logConsumer.accept(MESSAGE_TEMPLATE_TABLE_CONFIG_NOT_ALLOWED, new String[] {tableOption});
  }
}
