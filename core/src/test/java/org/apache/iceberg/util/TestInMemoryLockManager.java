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

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

public class TestInMemoryLockManager {

  private LockManagers.InMemoryLockManager lockManager;
  private String lockEntityId;
  private String ownerId;

  @Rule public Timeout timeout = new Timeout(5, TimeUnit.SECONDS);

  @Before
  public void before() {
    lockEntityId = UUID.randomUUID().toString();
    ownerId = UUID.randomUUID().toString();
    lockManager = new LockManagers.InMemoryLockManager(Maps.newHashMap());
  }

  @After
  public void after() throws Exception {
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
    Assert.assertEquals(
        "only 1 thread should have acquired the lock", 1, results.stream().filter(s -> s).count());
  }

  @Test
  public void testReleaseAndAcquire() {
    Assert.assertTrue(lockManager.acquire(lockEntityId, ownerId));
    Assert.assertTrue(lockManager.release(lockEntityId, ownerId));
    Assert.assertTrue(
        "acquire after release should succeed", lockManager.acquire(lockEntityId, ownerId));
  }

  @Test
  public void testReleaseWithWrongOwner() {
    Assert.assertTrue(lockManager.acquire(lockEntityId, ownerId));
    Assert.assertFalse(
        "should return false if ownerId is wrong",
        lockManager.release(lockEntityId, UUID.randomUUID().toString()));
  }

  @Test
  public void testAcquireSingleProcess() throws Exception {
    lockManager.initialize(
        ImmutableMap.of(
            CatalogProperties.LOCK_ACQUIRE_INTERVAL_MS, "500",
            CatalogProperties.LOCK_ACQUIRE_TIMEOUT_MS, "2000"));
    Assert.assertTrue(lockManager.acquire(lockEntityId, ownerId));
    String oldOwner = ownerId;

    CompletableFuture.supplyAsync(
        () -> {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
          Assert.assertTrue(lockManager.release(lockEntityId, oldOwner));
          return null;
        });

    ownerId = UUID.randomUUID().toString();
    long start = System.currentTimeMillis();
    Assert.assertTrue(lockManager.acquire(lockEntityId, ownerId));
    Assert.assertTrue("should succeed after 200ms", System.currentTimeMillis() - start >= 200);
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
                    Assert.assertTrue(lockManager.release(lockEntityId, owner));
                  }
                  return succeeded;
                })
            .collect(Collectors.toList());
    Assert.assertEquals(
        "all lock acquire should succeed sequentially", 3, results.stream().filter(s -> s).count());
    Assert.assertTrue("must take more than 3 seconds", System.currentTimeMillis() - start >= 3000);
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
    Assert.assertEquals(
        "only 1 thread should have acquired the lock", 1, results.stream().filter(s -> s).count());
  }
}
