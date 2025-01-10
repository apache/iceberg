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
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;

/** A cache for {@link AuthSession} instances. */
public class AuthSessionCache implements AutoCloseable {

  private final Duration sessionTimeout;
  private final Executor executor;
  private final LongSupplier nanoTimeSupplier;

  private volatile Cache<String, AuthSession> sessionCache;

  /**
   * Creates a new cache with the given session timeout, and with default executor and nano time
   * supplier for eviction tasks.
   *
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   */
  public AuthSessionCache(Duration sessionTimeout) {
    this(sessionTimeout, null, null);
  }

  /**
   * Creates a new cache with the given session timeout, executor, and nano time supplier. This
   * method is useful for testing mostly.
   *
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   * @param executor the executor to use for eviction tasks; if null, the cache will use the
   *     {@linkplain ForkJoinPool#commonPool() common pool}. The executor will not be closed when
   *     this cache is closed.
   * @param nanoTimeSupplier the supplier for nano time; if null, the cache will use {@link
   *     System#nanoTime()}.
   */
  AuthSessionCache(
      Duration sessionTimeout,
      @Nullable Executor executor,
      @Nullable LongSupplier nanoTimeSupplier) {
    this.sessionTimeout = sessionTimeout;
    this.executor = executor;
    this.nanoTimeSupplier = nanoTimeSupplier;
  }

  /**
   * Returns a cached session for the given key, loading it with the given loader if it is not
   * already cached.
   *
   * @param key the key to use for the session.
   * @param loader the loader to use to load the session if it is not already cached.
   * @param <T> the type of the session.
   * @return the cached session.
   */
  @SuppressWarnings("unchecked")
  public <T extends AuthSession> T cachedSession(String key, Function<String, T> loader) {
    return (T) sessionCache().get(key, loader);
  }

  @Override
  public void close() {
    Cache<String, AuthSession> cache = sessionCache;
    this.sessionCache = null;
    if (cache != null) {
      cache.invalidateAll();
      cache.cleanUp();
    }
  }

  @VisibleForTesting
  Cache<String, AuthSession> sessionCache() {
    if (sessionCache == null) {
      synchronized (this) {
        if (sessionCache == null) {
          this.sessionCache = newSessionCache(sessionTimeout, executor, nanoTimeSupplier);
        }
      }
    }

    return sessionCache;
  }

  private static Cache<String, AuthSession> newSessionCache(
      Duration sessionTimeout, Executor executor, LongSupplier nanoTimeSupplier) {
    Caffeine<String, AuthSession> builder =
        Caffeine.newBuilder()
            .expireAfterAccess(sessionTimeout)
            .removalListener(
                (id, auth, cause) -> {
                  if (auth != null) {
                    auth.close();
                  }
                });
    if (executor != null) {
      builder.executor(executor);
    }

    if (nanoTimeSupplier != null) {
      builder.ticker(nanoTimeSupplier::getAsLong);
    }

    return builder.build();
  }
}
