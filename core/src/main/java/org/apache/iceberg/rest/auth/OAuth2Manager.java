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

  private final String name;

  private volatile RESTClient refreshClient;
  private long startTimeMillis;
  private OAuthTokenResponse authResponse;
  private volatile AuthSessionCache sessionCache;

  public OAuth2Manager(String managerName) {
    super(managerName + "-token-refresh");
    this.name = managerName;
  }

  @Override
  public OAuth2Util.AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    warnIfOAuthServerUriNotSet(properties);
    AuthConfig config =
        ImmutableAuthConfig.builder()
            .from(AuthConfig.fromProperties(properties))
            .keepRefreshed(false) // no token refresh during init
            .build();
    Map<String, String> headers = OAuth2Util.authHeaders(config.token());
    OAuth2Util.AuthSession session = new OAuth2Util.AuthSession(headers, config);
    if (config.credential() != null && !config.credential().isEmpty()) {
      // Do not enable token refresh here since this is a short-lived session,
      // but keep track of the start time, so that token refresh can be
      // enabled later on when catalogSession is called.
      this.startTimeMillis = System.currentTimeMillis();
      this.authResponse =
          OAuth2Util.fetchToken(
              initClient.withAuthSession(session),
              Map.of(),
              config.credential(),
              config.scope(),
              config.oauth2ServerUri(),
              config.optionalOAuthParams());
      return OAuth2Util.AuthSession.fromTokenResponse(
          initClient, null, authResponse, startTimeMillis, session);
    } else if (config.token() != null) {
      return OAuth2Util.AuthSession.fromAccessToken(
          initClient, null, config.token(), null, session);
    }
    return session;
  }

  @Override
  public OAuth2Util.AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> properties) {
    // This client will be used for token refreshes; it should not have an auth session.
    this.refreshClient = sharedClient.withAuthSession(AuthSession.EMPTY);
    this.sessionCache = newSessionCache(name, properties);
    AuthConfig config = AuthConfig.fromProperties(properties);
    Map<String, String> headers = OAuth2Util.authHeaders(config.token());
    OAuth2Util.AuthSession session = new OAuth2Util.AuthSession(headers, config);
    keepRefreshed(config.keepRefreshed());
    // authResponse comes from the init phase: this means we already fetched a token
    // so reuse it now and turn token refresh on.
    if (authResponse != null) {
      return OAuth2Util.AuthSession.fromTokenResponse(
          refreshClient, refreshExecutor(), authResponse, startTimeMillis, session);
    } else if (config.token() != null) {
      // If both a token and a credential are provided, prefer the token.
      return OAuth2Util.AuthSession.fromAccessToken(
          refreshClient, refreshExecutor(), config.token(), config.expiresAtMillis(), session);
    } else if (config.credential() != null && !config.credential().isEmpty()) {
      OAuthTokenResponse response =
          OAuth2Util.fetchToken(
              sharedClient.withAuthSession(session),
              Map.of(),
              config.credential(),
              config.scope(),
              config.oauth2ServerUri(),
              config.optionalOAuthParams());
      return OAuth2Util.AuthSession.fromTokenResponse(
          refreshClient, refreshExecutor(), response, System.currentTimeMillis(), session);
    }
    return session;
  }

  @Override
  public OAuth2Util.AuthSession contextualSession(
      SessionCatalog.SessionContext context, AuthSession parent) {
    return maybeCreateChildSession(
        context.credentials(),
        context.properties(),
        ignored -> context.sessionId(),
        (OAuth2Util.AuthSession) parent);
  }

  @Override
  public OAuth2Util.AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    return maybeCreateChildSession(
        Maps.filterKeys(properties, TABLE_SESSION_ALLOW_LIST::contains),
        properties,
        properties::get,
        (OAuth2Util.AuthSession) parent);
  }

  @Override
  public AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    AuthConfig config = AuthConfig.fromProperties(properties);
    Map<String, String> headers = OAuth2Util.authHeaders(config.token());
    OAuth2Util.AuthSession parent = new OAuth2Util.AuthSession(headers, config);

    keepRefreshed(config.keepRefreshed());

    // Important: this method is invoked from standalone components; we must not assume that
    // the refresh client and session cache have been initialized, because catalogSession()
    // won't be called.
    // We also assume that this method may be called from multiple threads, so we must
    // synchronize access to the refresh client and session cache.

    if (refreshClient == null) {
      synchronized (this) {
        if (refreshClient == null) {
          this.refreshClient = sharedClient.withAuthSession(parent);
        }
      }
    }

    if (sessionCache == null) {
      synchronized (this) {
        if (sessionCache == null) {
          this.sessionCache = newSessionCache(name, properties);
        }
      }
    }

    String oauth2ServerUri =
        properties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());

    if (config.token() != null) {
      String cacheKey = oauth2ServerUri + ":" + config.token();
      return sessionCache.cachedSession(
          cacheKey, k -> newSessionFromAccessToken(config.token(), properties, parent));
    }

    if (config.credential() != null && !config.credential().isEmpty()) {
      String cacheKey = oauth2ServerUri + ":" + config.credential();
      return sessionCache.cachedSession(cacheKey, k -> newSessionFromTokenResponse(config, parent));
    }

    return parent;
  }

  @Override
  public void close() {
    try {
      super.close();
    } finally {
      AuthSessionCache cache = sessionCache;
      this.sessionCache = null;
      if (cache != null) {
        cache.close();
      }
    }
  }

  protected AuthSessionCache newSessionCache(String managerName, Map<String, String> properties) {
    return new AuthSessionCache(managerName, sessionTimeout(properties));
  }

  protected OAuth2Util.AuthSession maybeCreateChildSession(
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
            k -> newSessionFromAccessToken(token, properties, parent));
      }

      if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
        // fetch a token using the client credentials flow
        String credential = credentials.get(OAuth2Properties.CREDENTIAL);
        return sessionCache.cachedSession(
            cacheKeyFunc.apply(OAuth2Properties.CREDENTIAL),
            k -> newSessionFromCredential(credential, parent));
      }

      for (String tokenType : TOKEN_PREFERENCE_ORDER) {
        if (credentials.containsKey(tokenType)) {
          // exchange the token for an access token using the token exchange flow
          String token = credentials.get(tokenType);
          return sessionCache.cachedSession(
              cacheKeyFunc.apply(tokenType),
              k -> newSessionFromTokenExchange(token, tokenType, parent));
        }
      }
    }

    return parent;
  }

  protected OAuth2Util.AuthSession newSessionFromAccessToken(
      String token, Map<String, String> properties, OAuth2Util.AuthSession parent) {
    Long expiresAtMillis = AuthConfig.fromProperties(properties).expiresAtMillis();
    return OAuth2Util.AuthSession.fromAccessToken(
        refreshClient, refreshExecutor(), token, expiresAtMillis, parent);
  }

  protected OAuth2Util.AuthSession newSessionFromCredential(
      String credential, OAuth2Util.AuthSession parent) {
    return OAuth2Util.AuthSession.fromCredential(
        refreshClient, refreshExecutor(), credential, parent);
  }

  protected OAuth2Util.AuthSession newSessionFromTokenExchange(
      String token, String tokenType, OAuth2Util.AuthSession parent) {
    return OAuth2Util.AuthSession.fromTokenExchange(
        refreshClient, refreshExecutor(), token, tokenType, parent);
  }

  protected OAuth2Util.AuthSession newSessionFromTokenResponse(
      AuthConfig config, OAuth2Util.AuthSession parent) {
    OAuthTokenResponse response =
        OAuth2Util.fetchToken(
            refreshClient,
            Map.of(),
            config.credential(),
            config.scope(),
            config.oauth2ServerUri(),
            config.optionalOAuthParams());
    return OAuth2Util.AuthSession.fromTokenResponse(
        refreshClient, refreshExecutor(), response, System.currentTimeMillis(), parent);
  }

  private static void warnIfOAuthServerUriNotSet(Map<String, String> properties) {
    if (!properties.containsKey(OAuth2Properties.OAUTH2_SERVER_URI)) {
      String credential = properties.get(OAuth2Properties.CREDENTIAL);
      String initToken = properties.get(OAuth2Properties.TOKEN);
      boolean hasCredential = credential != null && !credential.isEmpty();
      boolean hasInitToken = initToken != null;
      if (hasInitToken || hasCredential) {
        LOG.warn(
            "Iceberg REST client is missing the OAuth2 server URI configuration and defaults to {}/{}. "
                + "This automatic fallback will be removed in a future Iceberg release. "
                + "It is recommended to configure the OAuth2 endpoint using the '{}' property to be prepared. "
                + "This warning will disappear if the OAuth2 endpoint is explicitly configured. "
                + "See https://github.com/apache/iceberg/issues/10537",
            RESTUtil.stripTrailingSlash(properties.get(CatalogProperties.URI)),
            ResourcePaths.tokens(),
            OAuth2Properties.OAUTH2_SERVER_URI);
      }
    }
  }

  private static Duration sessionTimeout(Map<String, String> props) {
    return Duration.ofMillis(
        PropertyUtil.propertyAsLong(
            props,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT));
  }
}
