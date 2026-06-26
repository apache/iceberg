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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 5)
public class TestInMemoryLockManager {

  private LockManagers.InMemoryLockManager lockManager;
  private String lockEntityId;
  private String ownerId;

  @BeforeEach
  public void before() {
    lockEntityId = UUID.randomUUID().toString();
    ownerId = UUID.randomUUID().toString();
    lockManager = new LockManagers.InMemoryLockManager(Maps.newHashMap());
  }

  @AfterEach
  public void after() throws Exception {
    lockManager.close();
  }

  @Test
  public void testAcquireOnceSingleProcess() {
    lockManager.acquireOnce(lockEntityId, ownerId);
    assertThatThrownBy(() -> lockManager.acquireOnce(lockEntityId, ownerId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Lock for")
        .hasMessageContaining("currently held by")
        .hasMessageContaining("expiration");
  }

  @Test
  public void testAcquireOnceMultiProcesses() {
    List<Boolean> results =
        IntStream.range(0, 10)
            .parallel()
            .mapToObj(
                i -> {
                  try {
                    lockManager.acquireOnce(lockEntityId, ownerId);
                    return true;
                  } catch (IllegalStateException e) {
                    return false;
                  }
                })
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("only 1 thread should have acquired the lock")
        .isOne();
  }

  @Test
  public void testReleaseAndAcquire() {
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.release(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.acquire(lockEntityId, ownerId))
        .as("acquire after release should succeed")
        .isTrue();
  }

  @Test
  public void testReleaseWithWrongOwner() {
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(lockManager.release(lockEntityId, UUID.randomUUID().toString()))
        .as("should return false if ownerId is wrong")
        .isFalse();
  }

  @Test
  public void testAcquireSingleProcess() throws Exception {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "2000"));
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    String oldOwner = ownerId;

    CompletableFuture.supplyAsync(
        () -> {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          assertThat(lockManager.release(lockEntityId, oldOwner)).isTrue();
          return null;
        });

    ownerId = UUID.randomUUID().toString();
    long start = System.currentTimeMillis();
    assertThat(lockManager.acquire(lockEntityId, ownerId)).isTrue();
    assertThat(System.currentTimeMillis() - start)
        .as("should succeed after 200ms")
        .isGreaterThanOrEqualTo(200);
  }

  @Test
  public void testAcquireMultiProcessAllSucceed() {
    lockManager.initialize(ImmutableMap.of(CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500"));
    long start = System.currentTimeMillis();
    List<Boolean> results =
        IntStream.range(0, 3)
            .parallel()
            .mapToObj(
                i -> {
                  String owner = UUID.randomUUID().toString();
                  boolean succeeded = lockManager.acquire(lockEntityId, owner);
                  if (succeeded) {
                    try {
                      Thread.sleep(1000);
                    } catch (InterruptedException e) {
                      throw new RuntimeException(e);
                    }
                    assertThat(lockManager.release(lockEntityId, owner)).isTrue();
                  }
                  return succeeded;
                })
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("all lock acquire should succeed sequentially")
        .isEqualTo(3);
    assertThat(System.currentTimeMillis() - start)
        .as("must take more than 3 seconds")
        .isGreaterThanOrEqualTo(3000);
  }

  @Test
  public void testAcquireMultiProcessOnlyOneSucceed() {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS, "100",
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "2000"));

    List<Boolean> results =
        IntStream.range(0, 3)
            .parallel()
            .mapToObj(i -> lockManager.acquire(lockEntityId, ownerId))
            .collect(Collectors.toList());
    assertThat(results.stream().filter(s -> s).count())
        .as("only 1 thread should have acquired the lock")
        .isOne();
  }

  @Test
  @Timeout(value = 30)
  public void testConcurrentAcquireReleaseDistinctEntities()
      throws InterruptedException, ExecutionException {
    // Each thread locks its own entity, so every acquire succeeds and the threads contend only on
    // the shared, static heartbeat map. A CyclicBarrier starts them together and the storm repeats
    // over many rounds to widen the race window, so a non-thread-safe map that loses a put/remove
    // (or spins in a resize loop) is caught by the per-round heartbeatCount() assertion below.
    int threadCount = 16;
    int iterationsPerThread = 50;
    int rounds = 50;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      for (int round = 0; round < rounds; round++) {
        CyclicBarrier startGate = new CyclicBarrier(threadCount);
        List<Future<Boolean>> futures =
            IntStream.range(0, threadCount)
                .mapToObj(
                    i ->
                        executor.submit(
                            () -> {
                              String entityId = UUID.randomUUID().toString();
                              String owner = UUID.randomUUID().toString();
                              startGate.await();
                              for (int j = 0; j < iterationsPerThread; j++) {
                                if (!lockManager.acquire(entityId, owner)
                                    || !lockManager.release(entityId, owner)) {
                                  return false;
                                }
                              }
                              return true;
                            }))
                .collect(Collectors.toList());

        List<Boolean> results = Lists.newArrayList();
        for (Future<Boolean> future : futures) {
          results.add(future.get());
        }

        assertThat(results)
            .as("every thread should acquire and release its own lock without interference")
            .hasSize(threadCount)
            .containsOnly(true);

        // Every entity ends its loop with a release, which atomically removes its heartbeat. The
        // thread-safe map is therefore empty after each round; a non-thread-safe map could leave a
        // stale or lost entry that surfaces here as a non-empty map.
        assertThat(LockManagers.InMemoryLockManager.heartbeatCount())
            .as("all heartbeats should be removed after round %d", round)
            .isZero();
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(value = 30)
  public void testConcurrentAcquireReleaseSharedEntity()
      throws InterruptedException, ExecutionException {
    // All threads contend on the SAME entity, so the lock serializes acquires: only the winner
    // runs the acquireOnce cleanup (HEARTBEATS.remove + put) while the others retry. This stresses
    // the heartbeat bookkeeping over serialized acquire/release cycles, not concurrently on a key.
    // Since the map is keyed by entityId it holds at most one entry, so the in-flight check below
    // guards against a leaked heartbeat rather than testing atomicity.
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "10",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "25000",
            CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS, "100"));
    int threadCount = 16;
    int iterationsPerThread = 50;
    String entityId = UUID.randomUUID().toString();
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      CyclicBarrier startGate = new CyclicBarrier(threadCount);
      List<Future<Integer>> futures =
          IntStream.range(0, threadCount)
              .mapToObj(
                  i ->
                      executor.submit(
                          () -> {
                            String owner = UUID.randomUUID().toString();
                            startGate.await();
                            int successes = 0;
                            for (int j = 0; j < iterationsPerThread; j++) {
                              assertThat(lockManager.acquire(entityId, owner))
                                  .as("acquire should succeed under contention")
                                  .isTrue();
                              // a single shared entity is backed by at most one heartbeat
                              assertThat(LockManagers.InMemoryLockManager.heartbeatCount())
                                  .as("a held lock has at most one heartbeat")
                                  .isLessThanOrEqualTo(1);
                              assertThat(lockManager.release(entityId, owner)).isTrue();
                              successes++;
                            }
                            return successes;
                          }))
              .collect(Collectors.toList());

      for (Future<Integer> future : futures) {
        assertThat(future.get())
            .as("every thread should run all acquire/release cycles")
            .isEqualTo(iterationsPerThread);
      }

      assertThat(LockManagers.InMemoryLockManager.heartbeatCount())
          .as("no heartbeat should remain after the shared entity is fully released")
          .isZero();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(value = 30)
  public void testAcquireCancelsOrphanedHeartbeatOnTakeover() {
    // A holder that never releases leaves its heartbeat tracked in HEARTBEATS. Once the lock
    // expires, another owner can take it over, and acquireOnce must cancel the stale heartbeat and
    // replace it. This is the only path that reaches the non-null branch of the cleanup, since
    // release() always removes the heartbeat first.
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_HEARTBEAT_TIMEOUT_MS, "10",
            CatalogProperties.LOCK_HEARTBEAT_INTERVAL_MS, "60000",
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "10",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "10000"));

    // Acquire without releasing so the heartbeat is left behind; the long heartbeat interval means
    // the lock is renewed once and then expires for good.
    lockManager.acquireOnce(lockEntityId, ownerId);
    ScheduledFuture<?> orphaned = LockManagers.InMemoryLockManager.heartbeat(lockEntityId);
    assertThat((Object) orphaned).as("the first acquire should track a heartbeat").isNotNull();

    // acquire() retries until the lock expires, then a new owner takes it over.
    String newOwner = UUID.randomUUID().toString();
    assertThat(lockManager.acquire(lockEntityId, newOwner))
        .as("takeover of an expired lock should succeed")
        .isTrue();

    assertThat(orphaned.isCancelled())
        .as("the orphaned heartbeat should be cancelled on takeover")
        .isTrue();
    assertThat((Object) LockManagers.InMemoryLockManager.heartbeat(lockEntityId))
        .as("a fresh heartbeat should replace the orphaned one")
        .isNotNull()
        .isNotSameAs(orphaned);
    assertThat(LockManagers.InMemoryLockManager.heartbeatCount())
        .as("exactly one heartbeat should remain after takeover")
        .isOne();
  }
}
