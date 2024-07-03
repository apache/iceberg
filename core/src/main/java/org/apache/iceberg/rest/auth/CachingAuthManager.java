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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;

/**
 * Manager for authentication sessions that caches sessions for contexts and tables. This class
 * provides a caching layer on top of another AuthManager.
 */
public abstract class CachingAuthManager implements AuthManager {

  @Value.Immutable
  public interface CacheableAuthSession {

    /** The cache key for the session. */
    @Value.Parameter(order = 0)
    String key();

    /**
     * Supplier for the session to cache. Will only be invoked if a session with the same key is not
     * already cached.
     */
    @Value.Parameter(order = 1)
    Supplier<AuthSession> session();
  }

  private Map<String, String> properties;
  private volatile Cache<String, AuthSession> sessionCache;

  @Override
  public void initialize(String owner, RESTClient client, Map<String, String> props) {
    this.properties = props;
  }

  @Override
  public final AuthSession contextualSession(
      SessionCatalog.SessionContext context, AuthSession parent) {
    Optional<CacheableAuthSession> cacheable = cacheableContextSession(context, parent);
    return cacheable.map(this::sessionFromCache).orElse(parent);
  }

  @Override
  public final AuthSession tableSession(
      TableIdentifier table, Map<String, String> props, AuthSession parent) {
    Optional<CacheableAuthSession> cacheable = cacheableTableSession(table, props, parent);
    return cacheable.map(this::sessionFromCache).orElse(parent);
  }

  /**
   * Returns a new session for the context. If the context does not require caching a new session,
   * this method should return empty.
   */
  protected abstract Optional<CacheableAuthSession> cacheableContextSession(
      SessionCatalog.SessionContext context, AuthSession parent);

  /**
   * Returns a new session for the table. If the table does not require caching a new session, this
   * method should return empty.
   */
  protected abstract Optional<CacheableAuthSession> cacheableTableSession(
      TableIdentifier table, Map<String, String> props, AuthSession parent);

  @Override
  public void close() {
    Cache<String, AuthSession> cache = sessionCache;
    try {
      if (cache != null) {
        cache.invalidateAll();
        cache.cleanUp();
      }
    } finally {
      this.sessionCache = null;
    }
  }

  private AuthSession sessionFromCache(CacheableAuthSession cacheable) {
    return sessionCache().get(cacheable.key(), id -> cacheable.session().get());
  }

  private Duration sessionTimeout() {
    long expirationIntervalMs =
        PropertyUtil.propertyAsLong(
            properties,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS,
            CatalogProperties.AUTH_SESSION_TIMEOUT_MS_DEFAULT);
    return Duration.ofMillis(expirationIntervalMs);
  }

  private Cache<String, AuthSession> sessionCache() {
    if (sessionCache == null) {
      synchronized (this) {
        if (sessionCache == null) {
          this.sessionCache = newSessionCache(sessionTimeout());
        }
      }
    }
    return sessionCache;
  }

  private static Cache<String, AuthSession> newSessionCache(Duration sessionTimeout) {
    return Caffeine.newBuilder()
        .expireAfterAccess(sessionTimeout)
        .removalListener(
            (RemovalListener<String, AuthSession>)
                (id, auth, cause) -> {
                  if (auth != null) {
                    auth.close();
                  }
                })
        .build();
  }
}
