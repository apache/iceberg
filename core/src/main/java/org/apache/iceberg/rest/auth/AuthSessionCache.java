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
import java.util.function.Supplier;

/** A cache for {@link AuthSession} instances. */
public class AuthSessionCache implements AutoCloseable {

  private final Duration sessionTimeout;
  private volatile Cache<String, AuthSession> sessionCache;

  public AuthSessionCache(Duration sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
  }

  @SuppressWarnings("unchecked")
  public <T extends AuthSession> T cachedSession(String key, Supplier<T> loader) {
    return (T) sessionCache().get(key, k -> loader.get());
  }

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

  private Cache<String, AuthSession> sessionCache() {
    if (sessionCache == null) {
      synchronized (this) {
        if (sessionCache == null) {
          this.sessionCache = newSessionCache(sessionTimeout);
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
