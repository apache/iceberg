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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
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

public class OAuth2Manager extends RefreshingAuthManager {

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
  private Map<String, String> properties;
  private AuthConfig config;
  private long startTimeMillis;
  private OAuthTokenResponse authResponse;

  @Override
  public void initialize(String owner, RESTClient restClient, Map<String, String> props) {
    super.initialize(owner, restClient, props);
    this.client = restClient;
    this.properties = props;
    this.config = createConfig(props);
    setExecutorNamePrefix(owner + "-token-refresh");
    setKeepRefreshed(config.keepRefreshed());
    // keep track of the start time for token refresh
    this.startTimeMillis = System.currentTimeMillis();
  }

  private static AuthConfig createConfig(Map<String, String> props) {
    String scope = props.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    Map<String, String> optionalOAuthParams = OAuth2Util.buildOptionalParam(props);
    String oauth2ServerUri =
        props.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());
    boolean keepRefreshed =
        PropertyUtil.propertyAsBoolean(
            props,
            OAuth2Properties.TOKEN_REFRESH_ENABLED,
            OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT);
    return AuthConfig.builder()
        .credential(props.get(OAuth2Properties.CREDENTIAL))
        .token(props.get(OAuth2Properties.TOKEN))
        .scope(scope)
        .oauth2ServerUri(oauth2ServerUri)
        .optionalOAuthParams(optionalOAuthParams)
        .keepRefreshed(keepRefreshed)
        .expiresAtMillis(expiresAtMillis(props))
        .build();
  }

  @Override
  public AuthSession catalogSession() {
    OAuth2Util.AuthSession session = sessionFromConfig();
    if (config.credential() != null && authResponse == null) {
      this.authResponse =
          OAuth2Util.fetchToken(
              client,
              session,
              config.credential(),
              config.scope(),
              config.oauth2ServerUri(),
              config.optionalOAuthParams());
    }
    if (authResponse != null) {
      return OAuth2Util.AuthSession.fromTokenResponse(
          client, refreshExecutor(), authResponse, startTimeMillis, session);
    } else if (config.token() != null) {
      return OAuth2Util.AuthSession.fromAccessToken(
          client, refreshExecutor(), config.token(), config.expiresAtMillis(), session);
    }
    return session;
  }

  private OAuth2Util.AuthSession sessionFromConfig() {
    Map<String, String> headers =
        RESTUtil.merge(configHeaders(properties), OAuth2Util.authHeaders(config.token()));
    return new OAuth2Util.AuthSession(headers, config);
  }

  @Override
  protected Optional<CacheableAuthSession> cacheableContextSession(
      SessionCatalog.SessionContext context, AuthSession parent) {
    CacheableAuthSession session =
        newSession(
            context.credentials(),
            context.properties(),
            prop -> context.sessionId(),
            (OAuth2Util.AuthSession) parent);
    return Optional.ofNullable(session);
  }

  @Override
  protected Optional<CacheableAuthSession> cacheableTableSession(
      TableIdentifier table, Map<String, String> props, AuthSession parent) {
    CacheableAuthSession session =
        newSession(
            Maps.filterKeys(props, TABLE_SESSION_ALLOW_LIST::contains),
            props,
            props::get,
            (OAuth2Util.AuthSession) parent);
    return Optional.ofNullable(session);
  }

  private CacheableAuthSession newSession(
      Map<String, String> credentials,
      Map<String, String> props,
      Function<String, String> keyFunc,
      OAuth2Util.AuthSession parent) {
    if (credentials != null) {
      // use the bearer token without exchanging
      if (credentials.containsKey(OAuth2Properties.TOKEN)) {
        String token = credentials.get(OAuth2Properties.TOKEN);
        return ImmutableCacheableAuthSession.of(
            keyFunc.apply(OAuth2Properties.TOKEN),
            () ->
                OAuth2Util.AuthSession.fromAccessToken(
                    client, refreshExecutor(), token, expiresAtMillis(props), parent));
      }

      if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
        // fetch a token using the client credentials flow
        String credential = credentials.get(OAuth2Properties.CREDENTIAL);
        return ImmutableCacheableAuthSession.of(
            keyFunc.apply(OAuth2Properties.CREDENTIAL),
            () ->
                OAuth2Util.AuthSession.fromCredential(
                    client, refreshExecutor(), credential, parent));
      }

      for (String tokenType : TOKEN_PREFERENCE_ORDER) {
        if (credentials.containsKey(tokenType)) {
          // exchange the token for an access token using the token exchange flow
          String token = credentials.get(tokenType);
          return ImmutableCacheableAuthSession.of(
              keyFunc.apply(tokenType),
              () ->
                  OAuth2Util.AuthSession.fromTokenExchange(
                      client, refreshExecutor(), token, tokenType, parent));
        }
      }
    }
    return null;
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

  private static Map<String, String> configHeaders(Map<String, String> props) {
    return RESTUtil.extractPrefixMap(props, "header.");
  }
}
