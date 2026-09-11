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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestTasks {

  @Test
  public void attemptCounterIsIncreasedOnRetries() {
    Counter counter = new DefaultMetricsContext().counter("counter");

    final int retries = 10;

    Tasks.foreach(IntStream.range(0, 10))
        .countAttempts(counter)
        .totalTimeoutMs(5000)
        .backoffStrategy(attempt -> 0L)
        .retry(retries)
        .onlyRetryOn(RuntimeException.class)
        .run(
            x -> {
              // don't throw on the last retry
              if (counter.value() <= retries) {
                throw new RuntimeException();
              }
            });

    assertThat(counter.value()).isEqualTo(retries + 1);
  }

  @Test
  public void attemptCounterIsIncreasedWithoutRetries() {
    Counter counter = new DefaultMetricsContext().counter("counter");

    Tasks.foreach(IntStream.range(0, 10)).countAttempts(counter).run(x -> {});

    assertThat(counter.value()).isOne();
  }

  @Test
  public void customBackoffStrategyIsUsed() {
    List<Integer> attempts = Collections.synchronizedList(Lists.newArrayList());
    BackoffStrategy recording =
        attempt -> {
          attempts.add(attempt);
          return 0L;
        };

    int retries = 3;
    Counter counter = new DefaultMetricsContext().counter("counter");

    Tasks.foreach(IntStream.range(0, 1))
        .countAttempts(counter)
        .retry(retries)
        .backoffStrategy(recording)
        .onlyRetryOn(RuntimeException.class)
        .run(
            x -> {
              if (counter.value() <= retries) {
                throw new RuntimeException();
              }
            });

    // computeBackoff is called once before each of the 3 retries, with 1-based attempt numbers
    assertThat(attempts).containsExactly(1, 2, 3);
  }

  @Test
  public void nullBackoffStrategyIsNoOp() {
    int retries = 3;
    Counter counter = new DefaultMetricsContext().counter("counter");

    Tasks.foreach(IntStream.range(0, 1))
        .countAttempts(counter)
        .totalTimeoutMs(5000)
        .retry(retries)
        .backoffStrategy(attempt -> 0L) // set a non-null strategy first
        .backoffStrategy(null) // mirrors call sites passing BackoffStrategies.from(...) == null
        .onlyRetryOn(RuntimeException.class)
        .run(
            x -> {
              if (counter.value() <= retries) {
                throw new RuntimeException();
              }
            });

    assertThat(counter.value()).isEqualTo(retries + 1);
  }

  @Test
  public void parallelTasksShareStrategyAcrossWorkers() throws InterruptedException {
    int items = 20;
    Set<Thread> threadsSeen = ConcurrentHashMap.newKeySet();
    AtomicInteger calls = new AtomicInteger();
    BackoffStrategy recording =
        attempt -> {
          threadsSeen.add(Thread.currentThread());
          calls.incrementAndGet();
          return 0L;
        };
    ConcurrentMap<Integer, AtomicInteger> perItemAttempts = Maps.newConcurrentMap();
    ExecutorService svc = Executors.newFixedThreadPool(4);
    try {
      Tasks.range(items)
          .executeWith(svc)
          .retry(2)
          .backoffStrategy(recording)
          .onlyRetryOn(RuntimeException.class)
          .run(
              i -> {
                int attemptCount =
                    perItemAttempts.computeIfAbsent(i, k -> new AtomicInteger()).incrementAndGet();
                if (attemptCount == 1) {
                  throw new RuntimeException("first attempt fails");
                }
              });
    } finally {
      svc.shutdownNow();
      svc.awaitTermination(5, TimeUnit.SECONDS);
    }

    assertThat(calls.get()).isEqualTo(items);
    assertThat(threadsSeen).hasSizeGreaterThan(1);
  }

  @Test
  public void backoffStrategyReturnValueDrivesSleepDuration() {
    long perRetryMs = 150L;
    int retries = 3;
    Counter counter = new DefaultMetricsContext().counter("counter");

    long startMs = System.currentTimeMillis();
    Tasks.foreach(IntStream.range(0, 1))
        .countAttempts(counter)
        .retry(retries)
        .backoffStrategy(attempt -> perRetryMs)
        .onlyRetryOn(RuntimeException.class)
        .run(
            x -> {
              if (counter.value() <= retries) {
                throw new RuntimeException();
              }
            });
    long elapsedMs = System.currentTimeMillis() - startMs;

    assertThat(elapsedMs).isGreaterThanOrEqualTo((long) (retries * perRetryMs * 0.9));
  }
}
