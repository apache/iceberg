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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.LongSupplier;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A cache for {@link AuthSession} instances. */
public class AuthSessionCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AuthSessionCache.class);

  private final Duration sessionTimeout;
  private final Executor executor;
  private final LongSupplier nanoTimeSupplier;

  private volatile Cache<String, AuthSession> sessionCache;

  /**
   * Creates a new cache with the given session timeout, and with default executor and nano time
   * supplier for eviction tasks.
   *
   * @param name a distinctive name for the cache.
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   */
  public AuthSessionCache(String name, Duration sessionTimeout) {
    this(name, sessionTimeout, null, null);
  }

  /**
   * Creates a new cache with the given session timeout, executor, and nano time supplier. This
   * method is useful for testing mostly.
   *
   * @param name a distinctive name for the cache.
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   * @param executor the executor to use for eviction tasks; if null, the cache will create a
   *     default executor. The executor will be closed when this cache is closed.
   * @param nanoTimeSupplier the supplier for nano time; if null, the cache will use {@link
   *     System#nanoTime()}.
   */
  AuthSessionCache(
      String name,
      Duration sessionTimeout,
      @Nullable Executor executor,
      @Nullable LongSupplier nanoTimeSupplier) {
    this.sessionTimeout = sessionTimeout;
    this.executor = executor == null ? createExecutor(name) : executor;
    this.nanoTimeSupplier = nanoTimeSupplier;
  }

  private static Executor createExecutor(String name) {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder()
            .setNameFormat(name + "-auth-session-evict-%d")
            .setDaemon(true)
            .build();
    return Executors.newCachedThreadPool(threadFactory);
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
    try {
      Cache<String, AuthSession> cache = sessionCache;
      this.sessionCache = null;
      if (cache != null) {
        cache.invalidateAll();
        cache.cleanUp();
      }
    } finally {
      if (executor instanceof ExecutorService) {
        ExecutorService service = (ExecutorService) executor;
        service.shutdown();
        if (!Uninterruptibles.awaitTerminationUninterruptibly(service, 10, TimeUnit.SECONDS)) {
          LOG.warn("Timed out waiting for eviction executor to terminate");
        }
        service.shutdownNow();
      }
    }
  }

  @VisibleForTesting
  Cache<String, AuthSession> sessionCache() {
    if (sessionCache == null) {
      synchronized (this) {
        if (sessionCache == null) {
          this.sessionCache = newSessionCache();
        }
      }
    }

    return sessionCache;
  }

  private Cache<String, AuthSession> newSessionCache() {
    Caffeine<String, AuthSession> builder =
        Caffeine.newBuilder()
            .executor(executor)
            .expireAfterAccess(sessionTimeout)
            .removalListener(
                (id, auth, cause) -> {
                  if (auth != null) {
                    auth.close();
                  }
                });

    if (nanoTimeSupplier != null) {
      builder.ticker(nanoTimeSupplier::getAsLong);
    }

    return builder.build();
  }
}
