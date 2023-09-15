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
package org.apache.iceberg.hive;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.util.Tasks;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MetastoreLock implements HiveLock {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreLock.class);
  private static final String HIVE_ACQUIRE_LOCK_TIMEOUT_MS = "iceberg.hive.lock-timeout-ms";
  private static final String HIVE_LOCK_CHECK_MIN_WAIT_MS = "iceberg.hive.lock-check-min-wait-ms";
  private static final String HIVE_LOCK_CHECK_MAX_WAIT_MS = "iceberg.hive.lock-check-max-wait-ms";
  private static final String HIVE_LOCK_CREATION_TIMEOUT_MS =
      "iceberg.hive.lock-creation-timeout-ms";
  private static final String HIVE_LOCK_CREATION_MIN_WAIT_MS =
      "iceberg.hive.lock-creation-min-wait-ms";
  private static final String HIVE_LOCK_CREATION_MAX_WAIT_MS =
      "iceberg.hive.lock-creation-max-wait-ms";
  private static final String HIVE_LOCK_HEARTBEAT_INTERVAL_MS =
      "iceberg.hive.lock-heartbeat-interval-ms";
  private static final String HIVE_TABLE_LEVEL_LOCK_EVICT_MS =
      "iceberg.hive.table-level-lock-evict-ms";

  private static final long HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
  private static final long HIVE_LOCK_CREATION_TIMEOUT_MS_DEFAULT = 3 * 60 * 1000; // 3 minutes
  private static final long HIVE_LOCK_CREATION_MIN_WAIT_MS_DEFAULT = 50; // 50 milliseconds
  private static final long HIVE_LOCK_CREATION_MAX_WAIT_MS_DEFAULT = 5 * 1000; // 5 seconds
  private static final long HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT = 4 * 60 * 1000; // 4 minutes
  private static final long HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT = TimeUnit.MINUTES.toMillis(10);
  private static volatile Cache<String, ReentrantLock> commitLockCache;

  private final ClientPool<IMetaStoreClient, TException> metaClients;
  private final String databaseName;
  private final String tableName;
  private final String fullName;
  private final long lockAcquireTimeout;
  private final long lockCheckMinWaitTime;
  private final long lockCheckMaxWaitTime;
  private final long lockCreationTimeout;
  private final long lockCreationMinWaitTime;
  private final long lockCreationMaxWaitTime;
  private final long lockHeartbeatIntervalTime;
  private final ScheduledExecutorService exitingScheduledExecutorService;
  private final String agentInfo;

  private Optional<Long> hmsLockId = Optional.empty();
  private ReentrantLock jvmLock = null;
  private Heartbeat heartbeat = null;

  MetastoreLock(
      Configuration conf,
      ClientPool<IMetaStoreClient, TException> metaClients,
      String catalogName,
      String databaseName,
      String tableName) {
    this.metaClients = metaClients;
    this.fullName = catalogName + "." + databaseName + "." + tableName;
    this.databaseName = databaseName;
    this.tableName = tableName;

    this.lockAcquireTimeout =
        conf.getLong(HIVE_ACQUIRE_LOCK_TIMEOUT_MS, HIVE_ACQUIRE_LOCK_TIMEOUT_MS_DEFAULT);
    this.lockCheckMinWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MIN_WAIT_MS, HIVE_LOCK_CHECK_MIN_WAIT_MS_DEFAULT);
    this.lockCheckMaxWaitTime =
        conf.getLong(HIVE_LOCK_CHECK_MAX_WAIT_MS, HIVE_LOCK_CHECK_MAX_WAIT_MS_DEFAULT);
    this.lockCreationTimeout =
        conf.getLong(HIVE_LOCK_CREATION_TIMEOUT_MS, HIVE_LOCK_CREATION_TIMEOUT_MS_DEFAULT);
    this.lockCreationMinWaitTime =
        conf.getLong(HIVE_LOCK_CREATION_MIN_WAIT_MS, HIVE_LOCK_CREATION_MIN_WAIT_MS_DEFAULT);
    this.lockCreationMaxWaitTime =
        conf.getLong(HIVE_LOCK_CREATION_MAX_WAIT_MS, HIVE_LOCK_CREATION_MAX_WAIT_MS_DEFAULT);
    this.lockHeartbeatIntervalTime =
        conf.getLong(HIVE_LOCK_HEARTBEAT_INTERVAL_MS, HIVE_LOCK_HEARTBEAT_INTERVAL_MS_DEFAULT);
    long tableLevelLockCacheEvictionTimeout =
        conf.getLong(HIVE_TABLE_LEVEL_LOCK_EVICT_MS, HIVE_TABLE_LEVEL_LOCK_EVICT_MS_DEFAULT);

    this.agentInfo = "Iceberg-" + UUID.randomUUID();

    this.exitingScheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat("iceberg-hive-lock-heartbeat-" + fullName + "-%d")
                .build());

    initTableLevelLockCache(tableLevelLockCacheEvictionTimeout);
  }

  @Override
  public void lock() throws LockException {
    // getting a process-level lock per table to avoid concurrent commit attempts to the same table
    // from the same JVM process, which would result in unnecessary HMS lock acquisition requests
    acquireJvmLock();

    // Getting HMS lock
    hmsLockId = Optional.of(acquireLock());

    // Starting heartbeat for the HMS lock
    heartbeat = new Heartbeat(metaClients, hmsLockId.get(), lockHeartbeatIntervalTime);
    heartbeat.schedule(exitingScheduledExecutorService);
  }

  @Override
  public void ensureActive() throws LockException {
    if (heartbeat == null) {
      throw new LockException("Lock is not active");
    }

    if (heartbeat.encounteredException != null) {
      throw new LockException(
          heartbeat.encounteredException,
          "Failed to heartbeat for hive lock. %s",
          heartbeat.encounteredException.getMessage());
    }
    if (!heartbeat.active()) {
      throw new LockException("Hive lock heartbeat thread not active");
    }
  }

  @Override
  public void unlock() {
    if (heartbeat != null) {
      heartbeat.cancel();
      exitingScheduledExecutorService.shutdown();
    }

    try {
      unlock(hmsLockId);
    } finally {
      releaseJvmLock();
    }
  }

  private long acquireLock() throws LockException {
    LockInfo lockInfo = createLock();

    final long start = System.currentTimeMillis();
    long duration = 0;
    boolean timeout = false;
    TException thriftError = null;

    try {
      if (lockInfo.lockState.equals(LockState.WAITING)) {
        // Retry count is the typical "upper bound of retries" for Tasks.run() function. In fact,
        // the maximum number of
        // attempts the Tasks.run() would try is `retries + 1`. Here, for checking locks, we use
        // timeout as the
        // upper bound of retries. So it is just reasonable to set a large retry count. However, if
        // we set
        // Integer.MAX_VALUE, the above logic of `retries + 1` would overflow into
        // Integer.MIN_VALUE. Hence,
        // the retry is set conservatively as `Integer.MAX_VALUE - 100` so it doesn't hit any
        // boundary issues.
        Tasks.foreach(lockInfo.lockId)
            .retry(Integer.MAX_VALUE - 100)
            .exponentialBackoff(lockCheckMinWaitTime, lockCheckMaxWaitTime, lockAcquireTimeout, 1.5)
            .throwFailureWhenFinished()
            .onlyRetryOn(WaitingForLockException.class)
            .run(
                id -> {
                  try {
                    LockResponse response = metaClients.run(client -> client.checkLock(id));
                    LockState newState = response.getState();
                    lockInfo.lockState = newState;
                    if (newState.equals(LockState.WAITING)) {
                      throw new WaitingForLockException(
                          String.format(
                              "Waiting for lock on table %s.%s", databaseName, tableName));
                    }
                  } catch (InterruptedException e) {
                    Thread.interrupted(); // Clear the interrupt status flag
                    LOG.warn(
                        "Interrupted while waiting for lock on table {}.{}",
                        databaseName,
                        tableName,
                        e);
                  }
                },
                TException.class);
      }
    } catch (WaitingForLockException e) {
      timeout = true;
      duration = System.currentTimeMillis() - start;
    } catch (TException e) {
      thriftError = e;
    } finally {
      if (!lockInfo.lockState.equals(LockState.ACQUIRED)) {
        unlock(Optional.of(lockInfo.lockId));
      }
    }

    if (!lockInfo.lockState.equals(LockState.ACQUIRED)) {
      if (timeout) {
        throw new LockException(
            "Timed out after %s ms waiting for lock on %s.%s", duration, databaseName, tableName);
      }

      if (thriftError != null) {
        throw new LockException(
            thriftError, "Metastore operation failed for %s.%s", databaseName, tableName);
      }

      // Just for safety. We should not get here.
      throw new LockException(
          "Could not acquire the lock on %s.%s, lock request ended in state %s",
          databaseName, tableName, lockInfo.lockState);
    } else {
      return lockInfo.lockId;
    }
  }

  /**
   * Creates a lock, retrying if possible on failure.
   *
   * @return The {@link LockInfo} object for the successfully created lock
   * @throws LockException When we are not able to fill the hostname for lock creation, or there is
   *     an error during lock creation
   */
  @SuppressWarnings("ReverseDnsLookup")
  private LockInfo createLock() throws LockException {
    LockInfo lockInfo = new LockInfo();

    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException uhe) {
      throw new LockException(uhe, "Error generating host name");
    }

    LockComponent lockComponent =
        new LockComponent(LockType.EXCLUSIVE, LockLevel.TABLE, databaseName);
    lockComponent.setTablename(tableName);
    LockRequest lockRequest =
        new LockRequest(Lists.newArrayList(lockComponent), HiveHadoopUtil.currentUser(), hostName);

    // Only works in Hive 2 or later.
    if (HiveVersion.min(HiveVersion.HIVE_2)) {
      lockRequest.setAgentInfo(agentInfo);
    }

    AtomicBoolean interrupted = new AtomicBoolean(false);
    Tasks.foreach(lockRequest)
        .retry(Integer.MAX_VALUE - 100)
        .exponentialBackoff(
            lockCreationMinWaitTime, lockCreationMaxWaitTime, lockCreationTimeout, 2.0)
        .shouldRetryTest(
            e ->
                !interrupted.get()
                    && e instanceof LockException
                    && HiveVersion.min(HiveVersion.HIVE_2))
        .throwFailureWhenFinished()
        .run(
            request -> {
              try {
                LockResponse lockResponse = metaClients.run(client -> client.lock(request));
                lockInfo.lockId = lockResponse.getLockid();
                lockInfo.lockState = lockResponse.getState();
              } catch (TException te) {
                LOG.warn("Failed to create lock {}", request, te);
                try {
                  // If we can not check for lock, or we do not find it, then rethrow the exception
                  // Otherwise we are happy as the findLock sets the lockId and the state correctly
                  if (HiveVersion.min(HiveVersion.HIVE_2)) {
                    LockInfo lockFound = findLock();
                    if (lockFound != null) {
                      lockInfo.lockId = lockFound.lockId;
                      lockInfo.lockState = lockFound.lockState;
                      LOG.info("Found lock {} by agentInfo {}", lockInfo, agentInfo);
                      return;
                    }
                  }

                  throw new LockException(
                      "Failed to find lock for table %s.%s", databaseName, tableName);
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  interrupted.set(true);
                  LOG.warn(
                      "Interrupted while trying to find lock for table {}.{}",
                      databaseName,
                      tableName,
                      e);
                  throw new LockException(
                      e,
                      "Interrupted while trying to find lock for table %s.%s",
                      databaseName,
                      tableName);
                }
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                interrupted.set(true);
                LOG.warn(
                    "Interrupted while creating lock on table {}.{}", databaseName, tableName, e);
                throw new LockException(
                    e, "Interrupted while creating lock on table %s.%s", databaseName, tableName);
              }
            },
            LockException.class);

    // This should be initialized always, or exception should be thrown.
    LOG.debug("Lock {} created for table {}.{}", lockInfo, databaseName, tableName);
    return lockInfo;
  }

  /**
   * Search for the locks using HMSClient.showLocks identified by the agentInfo. If the lock is
   * there, then a {@link LockInfo} object is returned. If the lock is not found <code>null</code>
   * is returned.
   *
   * @return The {@link LockInfo} for the found lock, or <code>null</code> if nothing found
   */
  private LockInfo findLock() throws LockException, InterruptedException {
    Preconditions.checkArgument(
        HiveVersion.min(HiveVersion.HIVE_2),
        "Minimally Hive 2 HMS client is needed to find the Lock using the showLocks API call");
    ShowLocksRequest showLocksRequest = new ShowLocksRequest();
    showLocksRequest.setDbname(databaseName);
    showLocksRequest.setTablename(tableName);
    ShowLocksResponse response;
    try {
      response = metaClients.run(client -> client.showLocks(showLocksRequest));
    } catch (TException e) {
      throw new LockException(e, "Failed to find lock for table %s.%s", databaseName, tableName);
    }
    for (ShowLocksResponseElement lock : response.getLocks()) {
      if (lock.getAgentInfo().equals(agentInfo)) {
        // We found our lock
        return new LockInfo(lock.getLockid(), lock.getState());
      }
    }

    // Not found anything
    return null;
  }

  private void unlock(Optional<Long> lockId) {
    Long id = null;
    try {
      if (!lockId.isPresent()) {
        // Try to find the lock based on agentInfo. Only works with Hive 2 or later.
        if (HiveVersion.min(HiveVersion.HIVE_2)) {
          LockInfo lockInfo = findLock();
          if (lockInfo == null) {
            // No lock found
            LOG.info("No lock found with {} agentInfo", agentInfo);
            return;
          }

          id = lockInfo.lockId;
        } else {
          LOG.warn("Could not find lock with HMSClient {}", HiveVersion.current());
          return;
        }
      } else {
        id = lockId.get();
      }

      doUnlock(id);
    } catch (InterruptedException ie) {
      if (id != null) {
        // Interrupted unlock. We try to unlock one more time if we have a lockId
        try {
          Thread.interrupted(); // Clear the interrupt status flag for now, so we can retry unlock
          LOG.warn("Interrupted unlock we try one more time {}.{}", databaseName, tableName, ie);
          doUnlock(id);
        } catch (Exception e) {
          LOG.warn("Failed to unlock even on 2nd attempt {}.{}", databaseName, tableName, e);
        } finally {
          Thread.currentThread().interrupt(); // Set back the interrupt status
        }
      } else {
        Thread.currentThread().interrupt(); // Set back the interrupt status
        LOG.warn("Interrupted finding locks to unlock {}.{}", databaseName, tableName, ie);
      }
    } catch (Exception e) {
      LOG.warn("Failed to unlock {}.{}", databaseName, tableName, e);
    }
  }

  private void doUnlock(long lockId) throws TException, InterruptedException {
    metaClients.run(
        client -> {
          client.unlock(lockId);
          return null;
        });
  }

  private void acquireJvmLock() {
    if (jvmLock != null) {
      throw new IllegalStateException(
          String.format("Cannot call acquireLock twice for %s", fullName));
    }

    jvmLock = commitLockCache.get(fullName, t -> new ReentrantLock(true));
    jvmLock.lock();
  }

  private void releaseJvmLock() {
    if (jvmLock != null) {
      jvmLock.unlock();
      jvmLock = null;
    }
  }

  private static void initTableLevelLockCache(long evictionTimeout) {
    if (commitLockCache == null) {
      synchronized (MetastoreLock.class) {
        if (commitLockCache == null) {
          commitLockCache =
              Caffeine.newBuilder()
                  .expireAfterAccess(evictionTimeout, TimeUnit.MILLISECONDS)
                  .build();
        }
      }
    }
  }

  private static class Heartbeat implements Runnable {
    private final ClientPool<IMetaStoreClient, TException> hmsClients;
    private final long lockId;
    private final long intervalMs;
    private ScheduledFuture<?> future;
    private volatile Exception encounteredException = null;

    Heartbeat(ClientPool<IMetaStoreClient, TException> hmsClients, long lockId, long intervalMs) {
      this.hmsClients = hmsClients;
      this.lockId = lockId;
      this.intervalMs = intervalMs;
      this.future = null;
    }

    @Override
    public void run() {
      try {
        hmsClients.run(
            client -> {
              client.heartbeat(0, lockId);
              return null;
            });
      } catch (TException | InterruptedException e) {
        this.encounteredException = e;
        throw new CommitFailedException(e, "Failed to heartbeat for lock: %d", lockId);
      }
    }

    public void schedule(ScheduledExecutorService scheduler) {
      future =
          scheduler.scheduleAtFixedRate(this, intervalMs / 2, intervalMs, TimeUnit.MILLISECONDS);
    }

    boolean active() {
      return future != null && !future.isCancelled();
    }

    public void cancel() {
      if (future != null) {
        future.cancel(false);
      }
    }
  }

  private static class LockInfo {
    private long lockId;
    private LockState lockState;

    private LockInfo() {
      this.lockId = -1;
      this.lockState = null;
    }

    private LockInfo(long lockId, LockState lockState) {
      this.lockId = lockId;
      this.lockState = lockState;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("lockId", lockId)
          .add("lockState", lockState)
          .toString();
    }
  }

  private static class WaitingForLockException extends RuntimeException {
    WaitingForLockException(String message) {
      super(message);
    }
  }
}
