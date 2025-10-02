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

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.lock.LockResponse;
import io.etcd.jetcd.lock.UnlockResponse;
import io.etcd.jetcd.options.GetOption;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Etcd backed implementation of the {@link EtcdLockFactory}. */
public class EtcdLockFactory implements TriggerLockFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EtcdLockFactory.class);
  private static final String LOCK_BASE_PATH = "/iceberg/flink/maintenance/locks/";

  private final String etcdEndpoint;
  private final String lockId;
  private final Duration connectionTimeoutMs;
  private final Duration keepAliveMs;
  private final Duration keepAliveTimeoutMs;
  private final int maxRetries;
  private transient Client client;
  private volatile boolean isOpen;

  /**
   * Constructs a new EtcdLockFactory instance.
   *
   * @param etcdEndpoint The Etcd server endpoint (e.g., "http://127.0.0.1:2379") — required, no
   *     default
   * @param lockId A unique identifier for the lock — required, no default
   * @param connectionTimeoutMs Connection timeout in milliseconds (must be >= 0). Default (if not
   *     set elsewhere): 5000 ms
   * @param keepAliveMs Interval in milliseconds for gRPC keepalive pings. Default: 30000 ms
   * @param keepAliveTimeoutMs Timeout in milliseconds for gRPC keepalive before connection is
   *     considered dead. Default: 10000 ms
   * @param maxRetries Maximum number of retries in case of connection or request failures. Default:
   *     2
   * @throws NullPointerException if {@code etcdEndpoint} or {@code lockId} is null
   * @throws IllegalArgumentException if {@code connectionTimeoutMs} is negative
   */
  public EtcdLockFactory(
      String etcdEndpoint,
      String lockId,
      int connectionTimeoutMs,
      int keepAliveMs,
      int keepAliveTimeoutMs,
      int maxRetries) {
    Preconditions.checkNotNull(etcdEndpoint, "Etcd endpoint cannot be null");
    Preconditions.checkNotNull(lockId, "Lock ID cannot be null");
    // connectionTimeout must be strictly positive ( > 0 )
    Preconditions.checkArgument(
        connectionTimeoutMs > 0,
        "Connection timeout must be greater than 0, got: %s",
        connectionTimeoutMs);

    // keepAlive must be strictly positive
    Preconditions.checkArgument(
        keepAliveMs > 0, "KeepAlive interval must be greater than 0, got: %s", keepAliveMs);

    // keepAliveTimeout must be strictly positive
    Preconditions.checkArgument(
        keepAliveTimeoutMs > 0,
        "KeepAlive timeout must be greater than 0, got: %s",
        keepAliveTimeoutMs);

    // keepAliveTimeout should not exceed keepAlive interval
    Preconditions.checkArgument(
        keepAliveTimeoutMs < keepAliveMs,
        "KeepAlive timeout (%s ms) must be less than keepAlive interval (%s ms)",
        keepAliveTimeoutMs,
        keepAliveMs);

    // maxRetries must be non-negative
    Preconditions.checkArgument(maxRetries >= 0, "Max retries must be >= 0, got: %s", maxRetries);

    this.etcdEndpoint = etcdEndpoint;
    this.lockId = lockId;
    this.connectionTimeoutMs = Duration.ofMillis(connectionTimeoutMs);
    this.keepAliveMs = Duration.ofMillis(keepAliveMs);
    this.keepAliveTimeoutMs = Duration.ofMillis(keepAliveTimeoutMs);
    this.maxRetries = maxRetries;
  }

  @Override
  public void open() {
    if (isOpen) {
      LOG.debug("EtcdLockFactory already opened for lockId: {}.", lockId);
      return;
    }

    try {
      this.client =
          Client.builder()
              .endpoints(etcdEndpoint)
              .connectTimeout(connectionTimeoutMs)
              .retryMaxAttempts(maxRetries)
              .keepaliveTime(keepAliveMs)
              .keepaliveTimeout(keepAliveTimeoutMs)
              .build();
      isOpen = true;
      LOG.info("EtcdLockFactory initialized for lockId: {}.", lockId);
    } catch (Exception e) {
      closeQuietly();
      throw new RuntimeException("Failed to initialize EtcdLockFactory", e);
    }
  }

  @Override
  public Lock createLock() {
    return new EtcdLockFactory.EtcdLock(getTaskSharePath(), client);
  }

  @Override
  public Lock createRecoveryLock() {
    return new EtcdLockFactory.EtcdLock(getRecoverySharedPath(), client);
  }

  @Override
  public void close() throws IOException {
    try {
      if (client != null) {
        client.close();
      }
    } finally {
      isOpen = false;
    }
  }

  private void closeQuietly() {
    try {
      close();
    } catch (Exception e) {
      LOG.warn("Failed to close EtcdLockFactory for lockId: {}", lockId, e);
    }
  }

  private String getTaskSharePath() {
    return LOCK_BASE_PATH + lockId + "/task";
  }

  private String getRecoverySharedPath() {
    return LOCK_BASE_PATH + lockId + "/recovery";
  }

  /** Etcd lock implementation */
  private static class EtcdLock implements Lock {
    private final io.etcd.jetcd.Lock lockClient;
    private final KV kvClient;
    private final ByteSequence lockKey;
    private final String lockPath;
    private final AtomicReference<ByteSequence> lockKeyRef;

    private EtcdLock(String lockPath, Client client) {
      this.lockClient = client.getLockClient();
      this.kvClient = client.getKVClient();
      this.lockPath = lockPath;
      this.lockKey = ByteSequence.from(lockPath, StandardCharsets.UTF_8);
      this.lockKeyRef = new AtomicReference<>(null);
    }

    @Override
    public boolean tryLock() {
      try {
        if (isHeld()) {
          LOG.warn("Lock is already held for path: {}", lockPath);
          return false;
        }

        CompletableFuture<LockResponse> lockFuture = lockClient.lock(lockKey, 0L);
        LockResponse response = lockFuture.get();
        if (response != null) {
          lockKeyRef.set(response.getKey());
          return true;
        }
        return false;
      } catch (Exception e) {
        LOG.warn("Failed to acquire Etcd lock", e);
        return false;
      }
    }

    @Override
    public boolean isHeld() {
      try {
        GetOption option = GetOption.newBuilder().withPrefix(lockKey).build();
        CompletableFuture<GetResponse> future = kvClient.get(lockKey, option);
        GetResponse response = future.get();
        return response.getKvs().size() > 0;
      } catch (Exception e) {
        throw new RuntimeException("Failed to check Etcd lock status", e);
      }
    }

    @Override
    public void unlock() {
      try {
        ByteSequence key = lockKeyRef.get();
        if (key != null) {
          CompletableFuture<UnlockResponse> future = lockClient.unlock(key);
          future.get();
          lockKeyRef.set(null);
          LOG.debug("Released lock for path: {}", lockPath);
        }
      } catch (Exception e) {
        LOG.warn("Failed to release lock for path: {}", lockPath, e);
        throw new RuntimeException("Failed to release lock", e);
      }
    }
  }
}
