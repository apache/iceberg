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
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuth2Manager implements AuthManager {

  private static final Logger LOG = LoggerFactory.getLogger(OAuth2Manager.class);

  private static final List<String> TOKEN_PREFERENCE_ORDER =
      ImmutableList.of(
          OAuth2Properties.ID_TOKEN_TYPE,
          OAuth2Properties.ACCESS_TOKEN_TYPE,
          OAuth2Properties.JWT_TOKEN_TYPE,
          OAuth2Properties.SAML2_TOKEN_TYPE,
          OAuth2Properties.SAML1_TOKEN_TYPE);

  private String name;
  private long startTimeMillis;
  private Map<String, String> authHeaders;
  private String credential;
  private String scope;
  private Map<String, String> optionalOAuthParams;
  private String oauth2ServerUri;
  private boolean keepTokenRefreshed = true;
  private OAuthTokenResponse authResponse;

  // a lazy thread pool for token refresh
  private volatile ScheduledExecutorService refreshExecutor = null;

  private RESTClient client;

  public OAuth2Manager() {}

  public OAuth2Manager(String name, Map<String, String> properties) {
    initialize(name, properties);
  }

  @Override
  public Map<String, String> mergeAuthHeadersForGetConfig(
      RESTClient initialAuthClient, Map<String, String> configuredHeaders) {
    Map<String, String> initHeaders = RESTUtil.merge(configuredHeaders, authHeaders);

    if (credential != null && !credential.isEmpty()) {
      this.authResponse =
          OAuth2Util.fetchToken(
              initialAuthClient,
              initHeaders,
              credential,
              scope,
              oauth2ServerUri,
              optionalOAuthParams);
      return RESTUtil.merge(initHeaders, OAuth2Util.authHeaders(authResponse.token()));
    } else {
      this.authResponse = null;
      return initHeaders;
    }
  }

  @Override
  public AuthSession newSession(
      RESTClient authClient, Map<String, String> mergedProps, Map<String, String> baseHeaders) {
    this.client = authClient;
    String token = mergedProps.get(OAuth2Properties.TOKEN);
    OAuth2Util.AuthSession catalogAuth =
        new OAuth2Util.AuthSession(
            baseHeaders,
            AuthConfig.builder()
                .credential(credential)
                .scope(scope)
                .oauth2ServerUri(oauth2ServerUri)
                .optionalOAuthParams(optionalOAuthParams)
                .build());
    if (authResponse != null) {
      catalogAuth =
          OAuth2Util.AuthSession.fromTokenResponse(
              authClient, tokenRefreshExecutor(name), authResponse, startTimeMillis, catalogAuth);
    } else if (token != null) {
      catalogAuth =
          OAuth2Util.AuthSession.fromAccessToken(
              authClient,
              tokenRefreshExecutor(name),
              token,
              expiresAtMillis(mergedProps),
              catalogAuth);
    }
    return catalogAuth;
  }

  @Override
  public Pair<String, Supplier<AuthSession>> newSessionSupplier(
      Map<String, String> credentials, Map<String, String> properties, AuthSession parent) {
    OAuth2Util.AuthSession oauth2Parent = (OAuth2Util.AuthSession) parent;
    if (credentials != null) {
      // use the bearer token without exchanging
      if (credentials.containsKey(OAuth2Properties.TOKEN)) {
        return Pair.of(
            credentials.get(OAuth2Properties.TOKEN),
            () ->
                OAuth2Util.AuthSession.fromAccessToken(
                    client,
                    tokenRefreshExecutor(name),
                    credentials.get(OAuth2Properties.TOKEN),
                    expiresAtMillis(properties),
                    oauth2Parent));
      }

      if (credentials.containsKey(OAuth2Properties.CREDENTIAL)) {
        // fetch a token using the client credentials flow
        return Pair.of(
            credentials.get(OAuth2Properties.CREDENTIAL),
            () ->
                OAuth2Util.AuthSession.fromCredential(
                    client,
                    tokenRefreshExecutor(name),
                    credentials.get(OAuth2Properties.CREDENTIAL),
                    oauth2Parent));
      }

      for (String tokenType : TOKEN_PREFERENCE_ORDER) {
        if (credentials.containsKey(tokenType)) {
          // exchange the token for an access token using the token exchange flow
          return Pair.of(
              credentials.get(tokenType),
              () ->
                  OAuth2Util.AuthSession.fromTokenExchange(
                      client,
                      tokenRefreshExecutor(name),
                      credentials.get(tokenType),
                      tokenType,
                      oauth2Parent));
        }
      }
    }

    return null;
  }

  @Override
  public void close() {
    shutdownRefreshExecutor();
  }

  @Override
  public void initialize(String managerName, Map<String, String> properties) {
    this.name = managerName;
    this.startTimeMillis =
        System.currentTimeMillis(); // keep track of the init start time for token refresh
    this.authHeaders = OAuth2Util.authHeaders(properties.get(OAuth2Properties.TOKEN));
    this.credential = properties.get(OAuth2Properties.CREDENTIAL);
    this.scope = properties.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE);
    this.optionalOAuthParams = OAuth2Util.buildOptionalParam(properties);
    this.oauth2ServerUri =
        properties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens());
  }

  private ScheduledExecutorService tokenRefreshExecutor(String catalogName) {
    if (!keepTokenRefreshed) {
      return null;
    }

    if (refreshExecutor == null) {
      synchronized (this) {
        if (refreshExecutor == null) {
          this.refreshExecutor = ThreadPools.newScheduledPool(catalogName + "-token-refresh", 1);
        }
      }
    }

    return refreshExecutor;
  }

  private Long expiresAtMillis(Map<String, String> properties) {
    if (properties.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
      long expiresInMillis =
          PropertyUtil.propertyAsLong(
              properties,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
      return System.currentTimeMillis() + expiresInMillis;
    } else {
      return null;
    }
  }

  private void shutdownRefreshExecutor() {
    if (refreshExecutor != null) {
      ScheduledExecutorService service = refreshExecutor;
      this.refreshExecutor = null;

      List<Runnable> tasks = service.shutdownNow();
      tasks.forEach(
          task -> {
            if (task instanceof Future) {
              ((Future<?>) task).cancel(true);
            }
          });

      try {
        if (!service.awaitTermination(1, TimeUnit.MINUTES)) {
          LOG.warn("Timed out waiting for refresh executor to terminate");
        }
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while waiting for refresh executor to terminate", e);
        Thread.currentThread().interrupt();
      }
    }
  }
}
