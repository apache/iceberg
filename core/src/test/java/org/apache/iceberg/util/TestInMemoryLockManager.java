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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
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
  public void after() {
    lockManager.close();
  }

  @Test
  public void testAcquireOnceSingleProcess() {
    lockManager.acquireOnce(lockEntityId, ownerId);
    Assertions.assertThatThrownBy(() -> lockManager.acquireOnce(lockEntityId, ownerId))
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
}
