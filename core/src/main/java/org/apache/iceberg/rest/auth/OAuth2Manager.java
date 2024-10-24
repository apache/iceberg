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
package org.apache.iceberg.rest.auth;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unused") // loaded by reflection
public class OAuth2Manager extends RefreshingAuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2Manager.class);

  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  // Auth-related properties that are allowed to be passed to the table session
  private static final Set<String> TABLE_SESSION_ALLOW_LIST =
      ImmutableSet.<String>builder()
          .add(OAuth2Properties.TOKEN)
          .addAll(TOKEN_PREFERENCE_ORDER)
          .build();

  private RESTClient client;
  private long startTimeMillis;
  private OAuthTokenResponse authResponse;
  private AuthSessionCache sessionCache;
  private Map<String, String> extraHeaders;

  @Override
  public void setName(String name) {
    setExecutorNamePrefix(name + "-token-refresh");
  }

  @Override
  public AuthSession preConfigSession(RESTClient initClient, Map<String, String> properties) {
    warnIfTokenEndpointUsed(properties);
    // FIXME these headers are meant for the catalog server;
    // they should not be passed to the token endpoint
    // if the auth server is different from the catalog server.
    extraHeaders = RESTUtil.extractPrefixMap(properties, "header.");
    AuthConfig config = createConfig(properties);
    Map<String, String> headers = OAuth2Util.authHeaders(config.token());
    OAuth2Util.AuthSession session = new OAuth2Util.AuthSession(headers, config);
    if (config.credential() != null) {
      // keep track of the start time for token refresh
      this.startTimeMillis = System.currentTimeMillis();
      this.authResponse =
          OAuth2Util.fetchToken(
              initClient,
              extraHeaders,
              session,
              config.credential(),
              config.scope(),
              config.oauth2ServerUri(),
              config.optionalOAuthParams());
      return OAuth2Util.AuthSession.fromTokenResponse(
          initClient, null, authResponse, startTimeMillis, session, extraHeaders);
    } else if (config.token() != null) {
      return OAuth2Util.AuthSession.fromAccessToken(
          initClient, null, config.token(), null, session, extraHeaders);
    }
    return session;
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    this.client = sharedClient;
    this.sessionCache = new AuthSessionCache(sessionTimeout(properties));
    AuthConfig config = createConfig(properties);
    Map<String, String> headers = OAuth2Util.authHeaders(config.token());
    OAuth2Util.AuthSession session = new OAuth2Util.AuthSession(headers, config);
    setKeepRefreshed(config.keepRefreshed());
    if (authResponse != null /* from the pre-config phase */) {
      return OAuth2Util.AuthSession.fromTokenResponse(
          this.client, refreshExecutor(), authResponse, startTimeMillis, session, extraHeaders);
    } else if (config.token() != null) {
      return OAuth2Util.AuthSession.fromAccessToken(
          this.client,
          refreshExecutor(),
          config.token(),
          config.expiresAtMillis(),
          session,
          extraHeaders);
    }
    return session;
  }

  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    return maybeCreateChildSession(
        context.credentials(),
        context.properties(),
        ignored -> context.sessionId(),
        (OAuth2Util.AuthSession) parent);
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    return maybeCreateChildSession(
        Maps.filterKeys(properties, TABLE_SESSION_ALLOW_LIST::contains),
        properties,
        properties::get,
        (OAuth2Util.AuthSession) parent);
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      try {
        AuthSessionCache cache = sessionCache;
        if (cache != null) {
          cache.close();
        }
      } finally {
        sessionCache = null;
      }
    }
  }

  private AuthSession maybeCreateChildSession(
      Map<String, String> credentials,
      Map<String, String> properties,
      Function<String, String> cacheKeyFunc,
      OAuth2Util.AuthSession parent) {
    if (credentials != null) {
      // use the bearer token without exchanging
      if (credentials.containsKey(OAuth2Properties.TOKEN)) {
        String token = credentials.get(OAuth2Properties.TOKEN);
        return sessionCache.cachedSession(
            cacheKeyFunc.apply(OAuth2Properties.TOKEN),
            () ->
                OAuth2Util.AuthSession.fromAccessToken(
                    client,
                    refreshExecutor(),
                    token,
                    expiresAtMillis(properties),
                    parent,
                    extraHeaders));
      }

      if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
        // fetch a token using the client credentials flow
        String credential = credentials.get(OAuth2Properties.CREDENTIAL);
        return sessionCache.cachedSession(
            cacheKeyFunc.apply(OAuth2Properties.CREDENTIAL),
            () ->
                OAuth2Util.AuthSession.fromCredential(
                    client, refreshExecutor(), credential, parent, extraHeaders));
      }

      for (String tokenType : TOKEN_PREFERENCE_ORDER) {
        if (credentials.containsKey(tokenType)) {
          // exchange the token for an access token using the token exchange flow
          String token = credentials.get(tokenType);
          return sessionCache.cachedSession(
              cacheKeyFunc.apply(tokenType),
              () ->
                  OAuth2Util.AuthSession.fromTokenExchange(
                      client, refreshExecutor(), token, tokenType, parent, extraHeaders));
        }
      }
    }
    return parent;
  }

  private static void warnIfTokenEndpointUsed(Map<String, String> properties) {
    if (usesDeprecatedTokenEndpoint(properties)) {
      String credential = properties.get(OAuth2Properties.CREDENTIAL);
      String initToken = properties.get(OAuth2Properties.TOKEN);
      boolean hasCredential = credential != null && !credential.isEmpty();
      boolean hasInitToken = initToken != null;
      if (hasInitToken || hasCredential) {
        LOG.warn(
            "Iceberg REST client is missing the OAuth2 server URI configuration and defaults to {}{}. "
                + "This automatic fallback will be removed in a future Iceberg release."
                + "It is recommended to configure the OAuth2 endpoint using the '{}' property to be prepared. "
                + "This warning will disappear if the OAuth2 endpoint is explicitly configured. "
                + "See https://github.com/apache/iceberg/issues/10537",
            properties.get(CatalogProperties.URI),
            ResourcePaths.tokens(),
            OAuth2Properties.OAUTH2_SERVER_URI);
      }
    }
  }

  private static boolean usesDeprecatedTokenEndpoint(Map<String, String> properties) {
    if (properties.containsKey(OAuth2Properties.OAUTH2_SERVER_URI)) {
      String oauth2ServerUri = properties.get(OAuth2Properties.OAUTH2_SERVER_URI);
      return !oauth2ServerUri.startsWith("http") // relative path
          || oauth2ServerUri.startsWith(properties.get(CatalogProperties.URI)) // same host
      ;
    }
    return true;
  }

  private static AuthConfig createConfig(Map<String, String> properties) {
    String scope = properties.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    Map<String, String> optionalOAuthParams = OAuth2Util.buildOptionalParam(properties);
    String oauth2ServerUri =
        properties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());
    boolean keepRefreshed =
        PropertyUtil.propertyAsBoolean(
            properties,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);
    return AuthConfig.builder()
        .credential(properties.get(OAuth2Properties.CREDENTIAL))
        .token(properties.get(OAuth2Properties.TOKEN))
        .scope(scope)
        .oauth2ServerUri(oauth2ServerUri)
        .optionalOAuthParams(optionalOAuthParams)
        .keepRefreshed(keepRefreshed)
        .expiresAtMillis(expiresAtMillis(properties))
        .build();
  }

  private static Duration sessionTimeout(Map<String, String> props) {
    long expirationIntervalMs =
        PropertyUtil.propertyAsLong(
            props,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT);
    return Duration.ofMillis(expirationIntervalMs);
  }

  private static Long expiresAtMillis(Map<String, String> props) {
    Long expiresInMillis = null;
    if (props.containsKey(OAuth2Properties.TOKEN)) {
      expiresInMillis = OAuth2Util.expiresAtMillis(props.get(OAuth2Properties.TOKEN));
    }
    if (expiresInMillis == null) {
      if (props.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
        long millis =
            PropertyUtil.propertyAsLong(
                props,
                OAuth2Properties.TOKEN_EXPIRES_IN_MS,
                OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
        expiresInMillis = System.currentTimeMillis() + millis;
      }
    }
    return expiresInMillis;
  }
}
