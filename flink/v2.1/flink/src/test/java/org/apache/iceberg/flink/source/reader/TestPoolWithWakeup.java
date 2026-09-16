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
package org.apache.iceberg.flink.source.reader;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/** Tests for {@link PoolWithWakeup}, the wakeable object pool that fixes issue #16426. */
class TestPoolWithWakeup {

  @Test
  void testPollReturnsAvailableEntry() throws Exception {
    PoolWithWakeup<Object> pool = new PoolWithWakeup<>(2);
    Object entry = new Object();
    pool.add(entry);

    assertThat(pool.pollEntry()).isSameAs(entry);
  }

  @Test
  @Timeout(10)
  void testWakeUpUnblocksEmptyPoll() throws Exception {
    PoolWithWakeup<Object> pool = new PoolWithWakeup<>(2);

    CountDownLatch started = new CountDownLatch(1);
    AtomicBoolean returned = new AtomicBoolean(false);
    AtomicReference<Object> result = new AtomicReference<>();

    Thread blocked =
        new Thread(
            () -> {
              started.countDown();
              try {
                result.set(pool.pollEntry());
                returned.set(true);
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    blocked.start();

    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);
    assertThat(blocked.isAlive()).as("Thread should block on empty pool").isTrue();

    pool.wakeUp();

    blocked.join(2000);
    assertThat(blocked.isAlive()).isFalse();
    assertThat(returned.get()).isTrue();
    assertThat(result.get()).as("pollEntry returns null when woken up").isNull();
  }

  @Test
  @Timeout(10)
  void testRecycleUnblocksPoll() throws Exception {
    PoolWithWakeup<Object> pool = new PoolWithWakeup<>(2);
    Object entry = new Object();

    CountDownLatch started = new CountDownLatch(1);
    AtomicReference<Object> result = new AtomicReference<>();

    Thread blocked =
        new Thread(
            () -> {
              started.countDown();
              try {
                result.set(pool.pollEntry());
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    blocked.start();

    assertThat(started.await(2, TimeUnit.SECONDS)).isTrue();
    Thread.sleep(200);
    assertThat(blocked.isAlive()).as("Thread should block on empty pool").isTrue();

    // Recycling an entry must wake the blocked poll and hand it the entry.
    pool.recycler().recycle(entry);

    blocked.join(2000);
    assertThat(blocked.isAlive()).isFalse();
    assertThat(result.get()).isSameAs(entry);
  }

  @Test
  void testWakeUpWithoutBlockedThreadReturnsNullOnce() throws Exception {
    PoolWithWakeup<Object> pool = new PoolWithWakeup<>(2);

    // No thread is blocked; the signal is latched and consumed by the next poll on an empty pool.
    pool.wakeUp();
    assertThat(pool.pollEntry()).isNull();

    // The signal is only consumed once: an available entry is still returned afterwards.
    Object entry = new Object();
    pool.add(entry);
    assertThat(pool.pollEntry()).isSameAs(entry);
  }
}
