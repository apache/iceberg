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
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.iceberg.jdbc.UncheckedSQLException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.zookeeper.KeeperException;
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
    this.connectString = connectString;
    this.lockId = lockId;
    this.sessionTimeoutMs = sessionTimeoutMs;
    this.connectionTimeoutMs = connectionTimeoutMs;
    this.baseSleepTimeMs = baseSleepTimeMs;
    this.maxRetries = maxRetries;
  }

  @Override
  public void open() {
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
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while connecting to Zookeeper", e);
    }
  }

  @Override
  public Lock createLock() {
    return new ZkLock(client, LOCK_BASE_PATH + lockId + "/task");
  }

  @Override
  public Lock createRecoveryLock() {
    return new ZkLock(client, LOCK_BASE_PATH + lockId + "/recovery");
  }

  @Override
  public void close() throws IOException {
    if (client != null) {
      client.close();
    }
  }

  /** Zookeeper lock implementation */
  private static class ZkLock implements Lock {
    private final String lockPath;
    private final CuratorFramework client;

    private ZkLock(CuratorFramework client, String lockPath) {
      this.client = client;
      this.lockPath = lockPath;
    }

    @Override
    public boolean tryLock() {
      if (isHeld()) {
        LOG.info("Lock is already held for {}", this);
        return false;
      }

      String newInstanceId = UUID.randomUUID().toString();
      try {
        client
            .create()
            .creatingParentsIfNeeded()
            .forPath(lockPath, newInstanceId.getBytes(StandardCharsets.UTF_8));
        return true;
      } catch (KeeperException.NodeExistsException e) {
        // Check if the lock creation was successful behind the scenes.
        if (newInstanceId.equals(instanceId())) {
          return true;
        } else {
          throw new UncheckedSQLException(e, "Failed to create %s lock", this);
        }
      } catch (Exception e) {
        LOG.info("Failed to acquire Zookeeper lock: {}", lockPath, e);
        return false;
      }
    }

    @Override
    public boolean isHeld() {
      try {
        return client.checkExists().forPath(lockPath) != null;
      } catch (Exception e) {
        LOG.info("Failed to check Zookeeper lock status: {}", lockPath, e);
        return false;
      }
    }

    @Override
    public void unlock() {
      // If the path is not exists, delete will fail.
      if (!isHeld()) {
        return;
      }
      try {
        client.delete().forPath(lockPath);
      } catch (Exception e) {
        LOG.info("Failed to release Zookeeper lock: {}", lockPath, e);
        throw new RuntimeException("Failed to release lock", e);
      }
    }

    public String instanceId() {
      try {
        byte[] data = client.getData().forPath(lockPath);
        return new String(data, StandardCharsets.UTF_8);
      } catch (Exception e) {
        LOG.info("Failed to get lock value: {}", lockPath, e);
        return null;
      }
    }
  }
}
