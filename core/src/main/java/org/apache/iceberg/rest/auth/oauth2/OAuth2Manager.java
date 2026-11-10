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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.ThreadPools;

/**
 * An {@link AuthManager} that authenticates with an OAuth2 authorization server using the client
 * credentials grant, and renews access tokens before they expire.
 *
 * <p>Callers must stop using this manager before closing it.
 */
public class OAuth2Manager implements AuthManager {

  private final LegacyConfigAdapter legacyConfigAdapter = new LegacyConfigAdapter();

  // The client used by the init, catalog and contextual sessions. The init client is closed once
  // the catalog is initialized, so sessions must always resolve the current client lazily.
  private volatile RESTClient restClient;
  private volatile String catalogUri;

  private OAuth2Session initSession;
  private OAuth2Session catalogSession;

  private volatile Cache<String, OAuth2Session> cacheById;
  private volatile Cache<OAuth2Config, OAuth2Session> cacheByConfig;

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> initProperties) {
    this.restClient = initClient;
    this.catalogUri = initProperties.get(CatalogProperties.URI);
    OAuth2Config initConfig = createConfig(initProperties, catalogUri);
    this.initSession = new OAuth2Session(initConfig, () -> restClient);
    return initSession;
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> catalogProps) {
    this.restClient = sharedClient;
    this.catalogUri = catalogProps.get(CatalogProperties.URI);
    this.cacheById = newCache(sessionTimeout(catalogProps));
    OAuth2Config catalogConfig = createConfig(catalogProps, catalogUri);
    this.catalogSession = new OAuth2Session(catalogConfig, () -> restClient);
    return catalogSession;
  }

  @Override
  public AuthSession contextualSession(SessionContext context, AuthSession parent) {
    Map<String, String> contextProperties =
        RESTUtil.merge(
            context.properties() != null ? context.properties() : Map.of(),
            context.credentials() != null ? context.credentials() : Map.of());
    Map<String, String> migrated = legacyConfigAdapter.migrate(contextProperties);
    if (migrated.isEmpty()) {
      List<String> unsupported = legacyConfigAdapter.unsupportedProperties(contextProperties);
      Preconditions.checkArgument(
          unsupported.isEmpty(),
          "Session context contains unsupported legacy OAuth2 properties %s",
          unsupported);
      return parent;
    }

    // the contextual configuration is self-contained: nothing is inherited from the parent config
    OAuth2Config contextConfig =
        OAuth2Config.fromProperties(legacyConfigAdapter.resolveTokenEndpoint(migrated, catalogUri));
    if (contextConfig.equals(((OAuth2Session) parent).config())) {
      return parent;
    }

    return cacheById.get(
        context.sessionId(), id -> new OAuth2Session(contextConfig, () -> restClient));
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    // vended tokens are not supported
    return parent;
  }

  @Override
  public AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    OAuth2Config config = createConfig(properties, properties.get(CatalogProperties.URI));
    if (cacheByConfig == null) {
      synchronized (this) {
        if (cacheByConfig == null) {
          cacheByConfig = newCache(sessionTimeout(properties));
        }
      }
    }

    Cache<OAuth2Config, OAuth2Session> cache = cacheByConfig;
    return cache.get(config, cfg -> new OAuth2Session(cfg, () -> sharedClient));
  }

  @Override
  public void close() {
    OAuth2Session init = initSession;
    OAuth2Session catalog = catalogSession;
    try (catalog;
        init) {
      invalidateCache(cacheById);
      invalidateCache(cacheByConfig);
    } finally {
      this.cacheById = null;
      this.cacheByConfig = null;
      this.initSession = null;
      this.catalogSession = null;
      this.restClient = null;
      this.catalogUri = null;
    }
  }

  @VisibleForTesting
  Cache<String, OAuth2Session> cacheById() {
    return cacheById;
  }

  @VisibleForTesting
  Cache<OAuth2Config, OAuth2Session> cacheByConfig() {
    return cacheByConfig;
  }

  private OAuth2Config createConfig(Map<String, String> properties, String uri) {
    Map<String, String> migrated = legacyConfigAdapter.migrate(properties);
    return OAuth2Config.fromProperties(legacyConfigAdapter.resolveTokenEndpoint(migrated, uri));
  }

  private static Duration sessionTimeout(Map<String, String> properties) {
    return Duration.ofMillis(
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT));
  }

  private static <K> Cache<K, OAuth2Session> newCache(Duration timeout) {
    return Caffeine.newBuilder()
        .executor(ThreadPools.authRefreshPool())
        .expireAfterAccess(timeout)
        .<K, OAuth2Session>removalListener(
            (key, session, cause) -> {
              if (session != null) {
                session.close();
              }
            })
        .build();
  }

  private static void invalidateCache(Cache<?, OAuth2Session> cache) {
    if (cache != null) {
      // close sessions synchronously; removal listeners run asynchronously and closing is
      // idempotent
      cache.asMap().values().forEach(OAuth2Session::close);
      cache.invalidateAll();
      cache.cleanUp();
    }
  }
}
