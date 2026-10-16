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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.HashMultiset;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMultiset;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Multiset;
import org.apache.iceberg.util.ParallelIterable.ParallelIterator;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class TestParallelIterable {
  @Test
  public void closeParallelIteratorWithoutCompleteIteration() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      Iterable<CloseableIterable<Integer>> transform =
          Iterables.transform(
              Lists.newArrayList(1, 2, 3, 4, 5),
              item ->
                  new CloseableIterable<Integer>() {
                    @Override
                    public void close() {}

                    @Override
                    public CloseableIterator<Integer> iterator() {
                      return CloseableIterator.withClose(
                          Collections.singletonList(item).iterator());
                    }
                  });

      ParallelIterable<Integer> parallelIterable = new ParallelIterable<>(transform, executor);
      ParallelIterator<Integer> iterator = (ParallelIterator<Integer>) parallelIterable.iterator();

      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isNotNull();
      Awaitility.await("Queue is populated")
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> queueHasElements(iterator));
      iterator.close();
      Awaitility.await("Queue is cleared")
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> assertThat(iterator.queueSize()).isEqualTo(0));
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void closeMoreDataParallelIteratorWithoutCompleteIteration() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      Iterator<Integer> integerIterator =
          new Iterator<Integer>() {
            private int number = 1;

            @Override
            public boolean hasNext() {
              if (number > 1000) {
                return false;
              }

              number++;
              return true;
            }

            @Override
            public Integer next() {
              try {
                // sleep to control number generate rate
                Thread.sleep(10);
              } catch (InterruptedException e) {
                // Sleep interrupted, we ignore it!
              }
              return number;
            }
          };
      Iterable<CloseableIterable<Integer>> transform =
          Iterables.transform(
              Lists.newArrayList(1),
              item ->
                  new CloseableIterable<Integer>() {
                    @Override
                    public void close() {}

                    @Override
                    public CloseableIterator<Integer> iterator() {
                      return CloseableIterator.withClose(integerIterator);
                    }
                  });

      ParallelIterable<Integer> parallelIterable = new ParallelIterable<>(transform, executor);
      ParallelIterator<Integer> iterator = (ParallelIterator<Integer>) parallelIterable.iterator();

      assertThat(iterator.hasNext()).isTrue();
      assertThat(iterator.next()).isNotNull();
      Awaitility.await("Queue is populated")
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(() -> queueHasElements(iterator));
      iterator.close();
      Awaitility.await("Queue is cleared")
          .atMost(10, TimeUnit.SECONDS)
          .untilAsserted(
              () ->
                  assertThat(iterator.queueSize())
                      .as("Queue is not empty after cleaning")
                      .isEqualTo(0));
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void limitQueueSize() {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      List<Iterable<Integer>> iterables =
          ImmutableList.of(
              () -> IntStream.range(0, 100).iterator(),
              () -> IntStream.range(0, 100).iterator(),
              () -> IntStream.range(0, 100).iterator());

      Multiset<Integer> expectedValues =
          IntStream.range(0, 100)
              .boxed()
              .flatMap(i -> Stream.of(i, i, i))
              .collect(ImmutableMultiset.toImmutableMultiset());

      int maxQueueSize = 20;
      ParallelIterable<Integer> parallelIterable =
          new ParallelIterable<>(iterables, executor, maxQueueSize);
      ParallelIterator<Integer> iterator = (ParallelIterator<Integer>) parallelIterable.iterator();

      Multiset<Integer> actualValues = HashMultiset.create();

      while (iterator.hasNext()) {
        assertThat(iterator.queueSize())
            .as("iterator internal queue size")
            .isLessThanOrEqualTo(100);
        actualValues.add(iterator.next());
      }

      assertThat(actualValues)
          .as("multiset of values returned by the iterator")
          .isEqualTo(expectedValues);

      iterator.close();
    } finally {
      executor.shutdown();
    }
  }

  @Test
  @Timeout(10)
  public void noDeadlock() {
    // This test simulates a scenario where iterators use a constrained resource
    // (e.g. an S3 connection pool that has a limit on the number of connections).
    // In this case, the constrained resource shouldn't cause a deadlock when queue
    // is full and the iterator is waiting for the queue to be drained.
    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      Semaphore semaphore = new Semaphore(1);

      List<Iterable<Integer>> iterablesA =
          ImmutableList.of(
              testIterable(
                  semaphore::acquire, semaphore::release, IntStream.range(0, 100).iterator()));
      List<Iterable<Integer>> iterablesB =
          ImmutableList.of(
              testIterable(
                  semaphore::acquire, semaphore::release, IntStream.range(200, 300).iterator()));

      ParallelIterable<Integer> parallelIterableA = new ParallelIterable<>(iterablesA, executor, 1);
      ParallelIterable<Integer> parallelIterableB = new ParallelIterable<>(iterablesB, executor, 1);

      parallelIterableA.iterator().next();
      parallelIterableB.iterator().next();
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  @Timeout(60)
  void wakesOnDataBeforeTaskCompletes() throws Exception {
    Semaphore producerGate = new Semaphore(0);
    Semaphore completionGate = new Semaphore(0);
    Iterable<Integer> input =
        () ->
            new Iterator<>() {
              private boolean produced;

              @Override
              public boolean hasNext() {
                if (produced) {
                  completionGate.acquireUninterruptibly();
                  return false;
                }
                return true;
              }

              @Override
              public Integer next() {
                producerGate.acquireUninterruptibly();
                produced = true;
                return 1;
              }
            };

    int result =
        consumeAfterWakeUp(
            input,
            CloseableIterator::next,
            ignored -> producerGate.release(),
            producerGate,
            completionGate);
    assertThat(result).isEqualTo(1);
  }

  @Test
  @Timeout(60)
  void wakesWhenEmptyTaskCompletes() throws Exception {
    Semaphore producerGate = new Semaphore(0);
    Iterable<Integer> input =
        () -> {
          producerGate.acquireUninterruptibly();
          return Collections.emptyIterator();
        };

    boolean result =
        consumeAfterWakeUp(
            input, CloseableIterator::hasNext, ignored -> producerGate.release(), producerGate);
    assertThat(result).isFalse();
  }

  @Test
  @Timeout(60)
  void wakesWhenTaskFails() {
    Semaphore producerGate = new Semaphore(0);
    IllegalArgumentException failure = new IllegalArgumentException("Cannot read manifest");
    Iterable<Integer> input =
        () -> {
          producerGate.acquireUninterruptibly();
          throw failure;
        };

    assertThatThrownBy(
            () ->
                consumeAfterWakeUp(
                    input,
                    CloseableIterator::hasNext,
                    ignored -> producerGate.release(),
                    producerGate))
        .isInstanceOf(ExecutionException.class)
        .hasMessage(failure.toString())
        .hasCause(failure);
  }

  @Test
  @Timeout(60)
  void wakesWhenClosed() {
    Semaphore producerGate = new Semaphore(0);
    Iterable<Integer> input =
        () -> {
          producerGate.acquireUninterruptibly();
          return Collections.emptyIterator();
        };

    assertThatThrownBy(
            () ->
                consumeAfterWakeUp(
                    input, CloseableIterator::hasNext, CloseableIterator::close, producerGate))
        .isInstanceOf(ExecutionException.class)
        .hasMessageContaining("Already closed")
        .hasCauseInstanceOf(IllegalStateException.class);
  }

  private <T> T consumeAfterWakeUp(
      Iterable<Integer> input,
      Function<CloseableIterator<Integer>, T> consume,
      IteratorAction wakeUp,
      Semaphore... gates)
      throws Exception {
    ExecutorService workers = Executors.newSingleThreadExecutor();
    AtomicReference<Thread> consumerThread = new AtomicReference<>();
    ExecutorService consumer =
        Executors.newSingleThreadExecutor(
            runnable -> {
              Thread thread = new Thread(runnable);
              consumerThread.set(thread);
              return thread;
            });

    try (ParallelIterable<Integer> iterable = new ParallelIterable<>(List.of(input), workers)) {
      CloseableIterator<Integer> iterator = iterable.iterator();
      Future<T> result = consumer.submit(() -> consume.apply(iterator));
      awaitWaitingConsumer(consumerThread);
      assertThat(result.isDone()).isFalse();
      wakeUp.run(iterator);
      return result.get(10, TimeUnit.SECONDS);
    } finally {
      for (Semaphore gate : gates) {
        gate.release();
      }

      consumer.shutdownNow();
      workers.shutdownNow();
      assertThat(consumer.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
      assertThat(workers.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
    }
  }

  private interface IteratorAction {
    void run(CloseableIterator<Integer> iterator) throws Exception;
  }

  private void awaitWaitingConsumer(AtomicReference<Thread> consumerThread) {
    Awaitility.await()
        .atMost(10, TimeUnit.SECONDS)
        .until(
            () ->
                consumerThread.get() != null
                    && consumerThread.get().getState() == Thread.State.WAITING);
  }

  private <T> CloseableIterable<T> testIterable(
      RunnableWithException open, RunnableWithException close, Iterator<T> iterator) {
    return new CloseableIterable<T>() {
      @Override
      public void close() {
        try {
          close.run();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public CloseableIterator<T> iterator() {
        try {
          open.run();
          return CloseableIterator.withClose(iterator);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private interface RunnableWithException {
    void run() throws Exception;
  }

  private void queueHasElements(ParallelIterator<Integer> iterator) {
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();
    assertThat(iterator.queueSize()).as("queue size").isGreaterThan(0);
  }
}
