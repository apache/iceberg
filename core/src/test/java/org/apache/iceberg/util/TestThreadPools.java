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

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class TestThreadPools {

  @AfterEach
  void restoreShutdownHook() {
    ThreadPools.initShutdownHook();
  }

  @Test
  void testRemoveShutdownHook() {
    assertThat(ThreadPools.isShutdownHookRegistered()).isTrue();
    ThreadPools.removeShutdownHook();
    assertThat(ThreadPools.isShutdownHookRegistered()).isFalse();
  }

  @Test
  void testNewExitingWorkerPoolShutdown() {
    ExecutorService pool = ThreadPools.newExitingWorkerPool("test-exiting-pool", 2);

    ThreadPools.shutdownThreadPools();

    assertThat(pool.isShutdown()).isTrue();
  }

  @Test
  void testMultipleExitingPoolsShutdown() {
    ExecutorService pool1 = ThreadPools.newExitingWorkerPool("test-pool-1", 1);
    ExecutorService pool2 = ThreadPools.newExitingWorkerPool("test-pool-2", 1);

    ThreadPools.shutdownThreadPools();

    assertThat(pool1.isShutdown()).isTrue();
    assertThat(pool2.isShutdown()).isTrue();
  }

  @Test
  void testExitingScheduledPoolShutdown() {
    ExecutorService scheduled =
        ThreadPools.newExitingScheduledPool("test-scheduled", 1, Duration.ofSeconds(5));

    ThreadPools.shutdownThreadPools();

    assertThat(scheduled.isShutdown()).isTrue();
  }

  @Test
  void testShutdownThreadPoolsInterruptsRunningTasks() throws Exception {
    final AtomicBoolean interrupted = new AtomicBoolean(false);
    // Use a short termination timeout so shutdownThreadPools will call shutdownNow
    ExecutorService slowPool =
        ThreadPools.newExitingScheduledPool("test-slow-pool", 1, Duration.ofMillis(50));

    CountDownLatch threadStarted = new CountDownLatch(1);
    CountDownLatch threadInterrupted = new CountDownLatch(1);

    slowPool.submit(
        () -> {
          try {
            threadStarted.countDown();
            // NOTE: the test is NOT actually waiting for 60s, as it will be interrupted
            // after 50ms due to the `shutdownThreadPools` call below
            Thread.sleep(60_000);
          } catch (InterruptedException e) {
            interrupted.set(true);
            threadInterrupted.countDown();
            Thread.currentThread().interrupt();
          }
        });

    threadStarted.await();

    long start = System.nanoTime();
    ThreadPools.shutdownThreadPools();
    threadInterrupted.await();
    long end = System.nanoTime();

    assertThat(slowPool.isShutdown()).isTrue();
    assertThat(interrupted.get()).isTrue();
    assertThat(end - start).isGreaterThanOrEqualTo(50_000_000);
  }
}
