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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.oauth2.config.ConfigMigrator;
import org.apache.iceberg.util.ThreadPools;

public class OAuth2Manager implements AuthManager {

  // For legacy properties migration & sanitization
  private final ConfigMigrator configMigrator = new ConfigMigrator();
  private String catalogUri;

  // Main OAuth2 sessions
  private OAuth2Session initSession;
  private OAuth2Session catalogSession;

  // Runtime dependencies for OAuth2 sessions
  private RESTClient restClient;

  // OAuth2 session cache management
  private final AtomicReference<Cache<String, OAuth2Session>> cacheById = new AtomicReference<>();
  private final AtomicReference<Cache<OAuth2Config, OAuth2Session>> cacheByConfig =
      new AtomicReference<>();

  public OAuth2Manager(String ignoredManagerName) {}

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> initProperties) {
    restClient = initClient;
    catalogUri = initProperties.get(CatalogProperties.URI);
    OAuth2Config initConfig = configMigrator.migrateCatalogConfig(initProperties, catalogUri);
    initSession = new OAuth2Session(initConfig, () -> restClient, ThreadPools.authRefreshPool());
    return initSession;
  }

  @Override
  public AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> catalogProperties) {
    restClient = sharedClient;
    catalogUri = catalogProperties.get(CatalogProperties.URI);
    OAuth2Config catalogConfig = configMigrator.migrateCatalogConfig(catalogProperties, catalogUri);
    // Copy the existing session if the config is the same as the init session
    // to avoid requiring from users to log in again, for human-based flows.
    catalogSession =
        initSession != null && catalogConfig.equals(initSession.config())
            ? initSession.copy()
            : new OAuth2Session(catalogConfig, () -> restClient, ThreadPools.authRefreshPool());
    return catalogSession;
  }

  @Override
  public AuthSession contextualSession(SessionContext context, AuthSession parent) {
    Map<String, String> contextProperties =
        RESTUtil.merge(
            Optional.ofNullable(context.properties()).orElseGet(Map::of),
            Optional.ofNullable(context.credentials()).orElseGet(Map::of));
    if (contextProperties.isEmpty()) {
      return parent;
    }

    OAuth2Config parentConfig = ((OAuth2Session) parent).config();
    OAuth2Config childConfig =
        configMigrator.migrateContextualConfig(parentConfig, contextProperties, catalogUri);

    if (childConfig.equals(parentConfig)) {
      return parent;
    }

    return getOrCreateCacheById(parentConfig.basicConfig().sessionCacheTimeout())
        .get(
            context.sessionId(),
            id ->
                new OAuth2Session(
                    (OAuth2Session) parent,
                    childConfig,
                    () -> restClient,
                    ThreadPools.authRefreshPool()));
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    if (properties.isEmpty()) {
      return parent;
    }

    OAuth2Config parentConfig = ((OAuth2Session) parent).config();
    OAuth2Config childConfig = configMigrator.migrateTableConfig(parentConfig, properties);

    if (childConfig.equals(parentConfig)) {
      return parent;
    }

    return getOrCreateCacheByConfig(parentConfig.basicConfig().sessionCacheTimeout())
        .get(
            childConfig,
            cfg ->
                new OAuth2Session(
                    (OAuth2Session) parent, cfg, () -> restClient, ThreadPools.authRefreshPool()));
  }

  @Override
  public AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    OAuth2Config config =
        configMigrator.migrateCatalogConfig(properties, properties.get(CatalogProperties.URI));
    return getOrCreateCacheByConfig(config.basicConfig().sessionCacheTimeout())
        .get(
            config,
            cfg -> new OAuth2Session(cfg, () -> sharedClient, ThreadPools.authRefreshPool()));
  }

  @Override
  public void close() {
    OAuth2Session init = initSession;
    OAuth2Session catalog = catalogSession;
    try (catalog;
        init) {
      invalidateCache(cacheById.getAndSet(null));
      invalidateCache(cacheByConfig.getAndSet(null));
    } finally {
      initSession = null;
      catalogSession = null;
      restClient = null;
      catalogUri = null;
    }
  }

  @VisibleForTesting
  Cache<String, OAuth2Session> cacheById() {
    return cacheById.get();
  }

  @VisibleForTesting
  Cache<OAuth2Config, OAuth2Session> cacheByConfig() {
    return cacheByConfig.get();
  }

  private Cache<String, OAuth2Session> getOrCreateCacheById(Duration timeout) {
    return getOrCreateCache(cacheById, timeout);
  }

  private Cache<OAuth2Config, OAuth2Session> getOrCreateCacheByConfig(Duration timeout) {
    return getOrCreateCache(cacheByConfig, timeout);
  }

  private static <K> Cache<K, OAuth2Session> getOrCreateCache(
      AtomicReference<Cache<K, OAuth2Session>> ref, Duration timeout) {
    Cache<K, OAuth2Session> cache = ref.get();
    if (cache == null) {
      ref.compareAndSet(null, newCache(timeout));
      cache = ref.get();
    }
    return cache;
  }

  private static <K> Cache<K, OAuth2Session> newCache(Duration timeout) {
    return Caffeine.newBuilder()
        .executor(ThreadPools.authRefreshPool())
        .expireAfterAccess(timeout)
        .<K, OAuth2Session>removalListener(
            (id, auth, cause) -> {
              if (auth != null) {
                auth.close();
              }
            })
        .build();
  }

  private static void invalidateCache(Cache<?, OAuth2Session> cache) {
    if (cache != null) {
      cache.invalidateAll();
      cache.cleanUp();
    }
  }
}
