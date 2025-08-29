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
package org.apache.iceberg.flink.maintenance.api;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.shared.VersionedValue;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Zookeeper backed implementation of the {@link TriggerLockFactory}. */
public class ZkLockFactory implements TriggerLockFactory {
  private static final Logger LOG = LoggerFactory.getLogger(ZkLockFactory.class);

  private static final String LOCK_BASE_PATH = "/iceberg/flink/maintenance/locks/";

  private final String connectString;
  private final String lockId;
  private final int sessionTimeoutMs;
  private final int connectionTimeoutMs;
  private final int baseSleepTimeMs;
  private final int maxRetries;
  private transient CuratorFramework client;
  private transient SharedCount taskSharedCount;
  private transient SharedCount recoverySharedCount;
  private volatile boolean isOpen;

  /**
   * Create Zookeeper lock factory
   *
   * @param connectString Zookeeper connection string
   * @param lockId which should identify the job and the table
   * @param sessionTimeoutMs Session timeout in milliseconds
   * @param connectionTimeoutMs Connection timeout in milliseconds
   * @param baseSleepTimeMs Base sleep time in milliseconds
   * @param maxRetries Maximum number of retries
   */
  public ZkLockFactory(
      String connectString,
      String lockId,
      int sessionTimeoutMs,
      int connectionTimeoutMs,
      int baseSleepTimeMs,
      int maxRetries) {
    Preconditions.checkNotNull(connectString, "Zookeeper connection string cannot be null");
    Preconditions.checkNotNull(lockId, "Lock ID cannot be null");
    Preconditions.checkArgument(
        sessionTimeoutMs >= 0, "Session timeout must be positive, got: %s", sessionTimeoutMs);
    Preconditions.checkArgument(
        connectionTimeoutMs >= 0,
        "Connection timeout must be positive, got: %s",
        connectionTimeoutMs);
    Preconditions.checkArgument(
        baseSleepTimeMs >= 0, "Base sleep time must be positive, got: %s", baseSleepTimeMs);
    Preconditions.checkArgument(
        maxRetries >= 0, "Max retries must be non-negative, got: %s", maxRetries);
    this.connectString = connectString;
    this.lockId = lockId;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.baseSleepTimeMs = baseSleepTimeMs;
    this.maxRetries = maxRetries;
  }

  @Override
  public void open() {
    if (isOpen) {
      LOG.debug("ZkLockFactory already opened for lockId: {}.", lockId);
      return;
    }

    this.client =
        CuratorFrameworkFactory.builder()
            .connectString(connectString)
            .sessionTimeoutMs(sessionTimeoutMs)
            .connectionTimeoutMs(connectionTimeoutMs)
            .retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries))
            .build();
    client.start();

    try {
      if (!client.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
        throw new IllegalStateException("Connection to Zookeeper timed out");
      }

      this.taskSharedCount = new SharedCount(client, getTaskSharePath(), 0);
      this.recoverySharedCount = new SharedCount(client, getRecoverySharedPath(), 0);
      taskSharedCount.start();
      recoverySharedCount.start();
      isOpen = true;
      LOG.info("ZkLockFactory initialized for lockId: {}.", lockId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while connecting to Zookeeper", e);
    } catch (Exception e) {
      closeQuietly();
      throw new RuntimeException("Failed to initialize SharedCount", e);
    }
  }

  private String getTaskSharePath() {
    return LOCK_BASE_PATH + lockId + "/task";
  }

  private String getRecoverySharedPath() {
    return LOCK_BASE_PATH + lockId + "/recovery";
  }

  private void closeQuietly() {
    try {
      close();
    } catch (Exception e) {
      LOG.warn("Failed to close ZkLockFactory for lockId: {}", lockId, e);
    }
  }

  @Override
  public Lock createLock() {
    return new ZkLock(getTaskSharePath(), taskSharedCount);
  }

  @Override
  public Lock createRecoveryLock() {
    return new ZkLock(getRecoverySharedPath(), recoverySharedCount);
  }

  @Override
  public void close() throws IOException {
    try {
      if (taskSharedCount != null) {
        taskSharedCount.close();
      }

      if (recoverySharedCount != null) {
        recoverySharedCount.close();
      }
    } finally {
      if (client != null) {
        client.close();
      }

      isOpen = false;
    }
  }

  /** Zookeeper lock implementation */
  private static class ZkLock implements Lock {
    private final SharedCount sharedCount;
    private final String lockPath;

    private static final int LOCKED = 1;
    private static final int UNLOCKED = 0;

    private ZkLock(String lockPath, SharedCount sharedCount) {
      this.lockPath = lockPath;
      this.sharedCount = sharedCount;
    }

    @Override
    public boolean tryLock() {
      VersionedValue<Integer> versionedValue = sharedCount.getVersionedValue();
      if (isHeld(versionedValue)) {
        LOG.debug("Lock is already held for path: {}", lockPath);
        return false;
      }

      try {
        boolean acquired = sharedCount.trySetCount(versionedValue, LOCKED);
        if (!acquired) {
          LOG.debug("Failed to acquire lock for path: {}", lockPath);
        }

        return acquired;
      } catch (Exception e) {
        LOG.warn("Failed to acquire Zookeeper lock", e);
        return false;
      }
    }

    @Override
    public boolean isHeld() {
      return isHeld(sharedCount.getVersionedValue());
    }

    private static boolean isHeld(VersionedValue<Integer> versionedValue) {
      try {
        return versionedValue.getValue() == LOCKED;
      } catch (Exception e) {
        throw new RuntimeException("Failed to check Zookeeper lock status", e);
      }
    }

    @Override
    public void unlock() {
      try {
        sharedCount.setCount(UNLOCKED);
        LOG.debug("Released lock for path: {}", lockPath);
      } catch (Exception e) {
        LOG.warn("Failed to release lock for path: {}", lockPath, e);
        throw new RuntimeException("Failed to release lock", e);
      }
    }
  }
}
