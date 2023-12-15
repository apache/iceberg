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
package org.apache.iceberg.util;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockManagers {

  private static final LockManager LOCK_MANAGER_DEFAULT =
      new InMemoryLockManager(Maps.newHashMap());

  private LockManagers() {}

  public static LockManager defaultLockManager() {
    return LOCK_MANAGER_DEFAULT;
  }

  public static LockManager from(Map<String, String> properties) {
    if (properties.containsKey(CatalogProperties.LOCK_IMPL)) {
      return loadLockManager(properties.get(CatalogProperties.LOCK_IMPL), properties);
    } else {
      return defaultLockManager();
    }
  }

  private static LockManager loadLockManager(String impl, Map<String, String> properties) {
    DynConstructors.Ctor<LockManager> ctor;
    try {
      ctor = DynConstructors.builder(LockManager.class).hiddenImpl(impl).buildChecked();
    } catch (NoSuchMethodException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize LockManager, missing no-arg constructor: %s", impl), e);
    }

    LockManager lockManager;
    try {
      lockManager = ctor.newInstance();
    } catch (ClassCastException e) {
      throw new IllegalArgumentException(
          String.format("Cannot initialize LockManager, %s does not implement LockManager.", impl),
          e);
    }

    lockManager.initialize(properties);
    return lockManager;
  }

  public abstract static class BaseLockManager implements LockManager {

    private static volatile ScheduledExecutorService scheduler;

    private long acquireTimeoutMs;
    private long acquireIntervalMs;
    private long heartbeatIntervalMs;
    private long heartbeatTimeoutMs;
    private int heartbeatThreads;

    public long heartbeatTimeoutMs() {
      return heartbeatTimeoutMs;
    }

    public long heartbeatIntervalMs() {
      return heartbeatIntervalMs;
    }

    public long acquireIntervalMs() {
      return acquireIntervalMs;
    }

    public long acquireTimeoutMs() {
      return acquireTimeoutMs;
    }

    public int heartbeatThreads() {
      return heartbeatThreads;
    }

    public ScheduledExecutorService scheduler() {
      if (scheduler == null) {
        synchronized (BaseLockManager.class) {
          if (scheduler == null) {
            scheduler =
                MoreExecutors.getExitingScheduledExecutorService(
                    (ScheduledThreadPoolExecutor)
                        Executors.newScheduledThreadPool(
                            heartbeatThreads(),
                            new ThreadFactoryBuilder()
                                .setDaemon(true)
                                .setNameFormat("iceberg-lock-manager-%d")
                                .build()));
          }
        }
      }

      return scheduler;
    }

    @Override
    public void initialize(Map<String, String> properties) {
      this.acquireTimeoutMs =
          PropertyUtil.propertyAsLong(
              properties,
              CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS,
              CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS_DEFAULT);
      this.acquireIntervalMs =
          PropertyUtil.propertyAsLong(
              properties,
              CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS,
              CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS_DEFAULT);
      this.heartbeatIntervalMs =
          PropertyUtil.propertyAsLong(
              properties,
              CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS,
              CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT);
      this.heartbeatTimeoutMs =
          PropertyUtil.propertyAsLong(
              properties,
              CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS,
              CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS_DEFAULT);
      this.heartbeatThreads =
          PropertyUtil.propertyAsInt(
              properties,
              CatalogProperties.LOCK_HEARTBEAT_THREADS,
              CatalogProperties.LOCK_HEARTBEAT_THREADS_DEFAULT);
    }

    @Override
    public void close() throws Exception {
      if (scheduler != null) {
        scheduler.shutdownNow();
        scheduler = null;
      }
    }
  }

  /**
   * Implementation of {@link LockManager} that uses an in-memory concurrent map for locking. This
   * implementation should only be used for testing, or if the caller only needs locking within the
   * same JVM during table commits.
   */
  static class InMemoryLockManager extends BaseLockManager {

    private static final Logger LOG = LoggerFactory.getLogger(InMemoryLockManager.class);

    private static final Map<String, InMemoryLockContent> LOCKS = Maps.newConcurrentMap();
    private static final Map<String, ScheduledFuture<?>> HEARTBEATS = Maps.newHashMap();

    InMemoryLockManager(Map<String, String> properties) {
      initialize(properties);
    }

    @VisibleForTesting
    void acquireOnce(String entityId, String ownerId) {
      InMemoryLockContent content = LOCKS.get(entityId);
      if (content != null && content.expireMs() > System.currentTimeMillis()) {
        throw new IllegalStateException(
            String.format(
                "Lock for %s currently held by %s, expiration: %s",
                entityId, content.ownerId(), content.expireMs()));
      }

      long expiration = System.currentTimeMillis() + heartbeatTimeoutMs();
      boolean succeed;
      if (content == null) {
        InMemoryLockContent previous =
            LOCKS.putIfAbsent(entityId, new InMemoryLockContent(ownerId, expiration));
        succeed = previous == null;
      } else {
        succeed = LOCKS.replace(entityId, content, new InMemoryLockContent(ownerId, expiration));
      }

      if (succeed) {
        // cleanup old heartbeat
        if (HEARTBEATS.containsKey(entityId)) {
          HEARTBEATS.remove(entityId).cancel(false);
        }

        HEARTBEATS.put(
            entityId,
            scheduler()
                .scheduleAtFixedRate(
                    () -> {
                      InMemoryLockContent lastContent = LOCKS.get(entityId);
                      try {
                        long newExpiration = System.currentTimeMillis() + heartbeatTimeoutMs();
                        LOCKS.replace(
                            entityId, lastContent, new InMemoryLockContent(ownerId, newExpiration));
                      } catch (NullPointerException e) {
                        throw new RuntimeException(
                            "Cannot heartbeat to a deleted lock " + entityId, e);
                      }
                    },
                    0,
                    heartbeatIntervalMs(),
                    TimeUnit.MILLISECONDS));

      } else {
        throw new IllegalStateException("Unable to acquire lock " + entityId);
      }
    }

    @Override
    public boolean acquire(String entityId, String ownerId) {
      try {
        Tasks.foreach(entityId)
            .retry(Integer.MAX_VALUE - 1)
            .onlyRetryOn(IllegalStateException.class)
            .throwFailureWhenFinished()
            .exponentialBackoff(acquireIntervalMs(), acquireIntervalMs(), acquireTimeoutMs(), 1)
            .run(id -> acquireOnce(id, ownerId));
        return true;
      } catch (IllegalStateException e) {
        return false;
      }
    }

    @Override
    public boolean release(String entityId, String ownerId) {
      InMemoryLockContent currentContent = LOCKS.get(entityId);
      if (currentContent == null) {
        LOG.error("Cannot find lock for entity {}", entityId);
        return false;
      }

      if (!currentContent.ownerId().equals(ownerId)) {
        LOG.error(
            "Cannot unlock {} by {}, current owner: {}",
            entityId,
            ownerId,
            currentContent.ownerId());
        return false;
      }

      HEARTBEATS.remove(entityId).cancel(false);
      LOCKS.remove(entityId);
      return true;
    }

    @Override
    public void close() {
      HEARTBEATS.values().forEach(future -> future.cancel(false));
      HEARTBEATS.clear();
      LOCKS.clear();
      super.close();
    }
  }

  private static class InMemoryLockContent {
    private final String ownerId;
    private final long expireMs;

    InMemoryLockContent(String ownerId, long expireMs) {
      this.ownerId = ownerId;
      this.expireMs = expireMs;
    }

    public long expireMs() {
      return expireMs;
    }

    public String ownerId() {
      return ownerId;
    }
  }
}
