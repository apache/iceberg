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
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A cache for {@link AuthSession} instances. */
public class AuthSessionCache implements AutoCloseable {

  private static final Logger LOG = LoggerFactory.getLogger(AuthSessionCache.class);

  /**
   * A single daemon thread, shared by all cache instances for eviction tasks. A shared executor
   * avoids registering one JVM shutdown hook per cache instance: each hook thread is constructed
   * on the creating thread and would otherwise copy that thread's inheritable thread-locals,
   * pinning state such as closed Spark sessions for the life of the JVM. The worker thread is
   * created with inheritable thread-local variables disabled so it cannot pin the creating
   * thread's state either. The executor is deliberately exposed only as a plain {@link Executor}
   * (never an {@link ExecutorService}) so that {@link #close()} does not shut it down.
   */
  private static final Executor SHARED_EVICTION_EXECUTOR = newSharedEvictionExecutor();

  private final Duration sessionTimeout;
  private final Executor executor;
  private final Ticker ticker;

  private volatile Cache<String, AuthSession> sessionCache;

  /**
   * Creates a new cache with the given session timeout, and with default executor and default
   * ticker for eviction tasks.
   *
   * @param name a distinctive name for the cache. The name is retained for API compatibility but
   *     no longer backs a dedicated thread pool: all instances share a single daemon eviction
   *     thread.
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   */
  public AuthSessionCache(String name, Duration sessionTimeout) {
    this(sessionTimeout, SHARED_EVICTION_EXECUTOR, Ticker.systemTicker());
  }

  /**
   * Creates a new cache with the given session timeout, executor, and ticker. This method is useful
   * for testing mostly.
   *
   * @param sessionTimeout the session timeout. Sessions will become eligible for eviction after
   *     this duration of inactivity.
   * @param executor the executor to use for eviction tasks; if null, the cache will create a
   *     default executor. An {@link ExecutorService} executor is shut down when this cache is
   *     closed; a plain {@link Executor} is left running.
   * @param ticker the ticker to use for the cache.
   */
  AuthSessionCache(Duration sessionTimeout, Executor executor, Ticker ticker) {
    this.sessionTimeout = sessionTimeout;
    this.executor = executor;
    this.ticker = ticker;
  }

  @VisibleForTesting
  static Executor newSharedEvictionExecutor() {
    ThreadFactory threadFactory =
        runnable -> {
          // inheritThreadLocals=false so the worker thread cannot pin the creating thread's
          // inheritable thread-locals (e.g. a Spark session bound to an InheritableThreadLocal)
          Thread thread = new Thread(null, runnable, "iceberg-auth-session-evict", 0, false);
          thread.setDaemon(true);
          return thread;
        };
    ExecutorService service = Executors.newSingleThreadExecutor(threadFactory);
    // Expose as a plain Executor (not the service itself) so that close() leaves the shared
    // executor running.
    return service::execute;
  }

  @VisibleForTesting
  Executor executor() {
    return executor;
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
            .ticker(ticker)
            .removalListener(
                (id, auth, cause) -> {
                  if (auth != null) {
                    auth.close();
                  }
                });

    return builder.build();
  }
}
