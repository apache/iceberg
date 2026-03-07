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
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

public class TestThreadPools {

  @Test
  public void testRemoveShutdownHook() {
    try {
      assertThat(ThreadPools.isShutdownHookRegistered()).isTrue();
      ThreadPools.removeShutdownHook();
      assertThat(ThreadPools.isShutdownHookRegistered()).isFalse();
    } finally {
      ThreadPools.initShutdownHook();
    }
  }

  @Test
  public void testThreadPoolManagerAddAndShutdown() throws Exception {
    ThreadPools.ThreadPoolManager manager = new ThreadPools.ThreadPoolManager();

    ExecutorService testExecutor = Executors.newFixedThreadPool(2);

    Duration timeout = Duration.ofSeconds(5);
    manager.addThreadPool(testExecutor, timeout);

    manager.shutdownAll();

    assertThat(testExecutor.isShutdown()).isTrue();
  }

  @Test
  public void testThreadPoolManagerMultipleShutdowns() throws Exception {
    ThreadPools.ThreadPoolManager manager = new ThreadPools.ThreadPoolManager();

    ExecutorService executor1 = Executors.newFixedThreadPool(1);
    ExecutorService executor2 = Executors.newFixedThreadPool(1);

    Duration timeout = Duration.ofSeconds(5);
    manager.addThreadPool(executor1, timeout);
    manager.addThreadPool(executor2, timeout);

    manager.shutdownAll();

    assertThat(executor1.isShutdown()).isTrue();
    assertThat(executor2.isShutdown()).isTrue();
  }

  @Test
  public void testThreadPoolManagerShutdownNowCalled() throws Exception {
    ThreadPools.ThreadPoolManager manager = new ThreadPools.ThreadPoolManager();

    final AtomicBoolean interrupted = new AtomicBoolean(false);
    ExecutorService slowExecutor = Executors.newFixedThreadPool(1);

    manager.addThreadPool(slowExecutor, Duration.ofMillis(50));

    CountDownLatch threadStarted = new CountDownLatch(1);
    CountDownLatch threadInterrupted = new CountDownLatch(1);

    slowExecutor.submit(
        () -> {
          try {
            threadStarted.countDown();
            // NOTE: the test is NOT actually waiting for 60s, as it will be interrupted
            // after 50ms due to the `shutdownAll` call below
            Thread.sleep(60_000);
          } catch (InterruptedException e) {
            interrupted.set(true);
            threadInterrupted.countDown();
            Thread.currentThread().interrupt();
          }
        });

    threadStarted.await();

    long start = System.nanoTime();
    manager.shutdownAll();
    threadInterrupted.await();
    long end = System.nanoTime();

    assertThat(slowExecutor.isShutdown()).isTrue();
    assertThat(interrupted.get()).isTrue();
    assertThat(end - start).isGreaterThanOrEqualTo(50_000_000);
  }
}
