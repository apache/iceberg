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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
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
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> queueHasElements(iterator));
      iterator.close();
      Awaitility.await("Queue is cleared")
          .atMost(5, TimeUnit.SECONDS)
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
          .atMost(5, TimeUnit.SECONDS)
          .untilAsserted(() -> queueHasElements(iterator));
      iterator.close();
      Awaitility.await("Queue is cleared")
          .atMost(5, TimeUnit.SECONDS)
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

  @Test
  public void testDynamicIterableAddition() {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // Create ParallelIterable with empty initial list
      ParallelIterable<Integer> parallelIterable =
          new ParallelIterable<>(Lists.newArrayList(), executor);

      // Add iterables dynamically
      parallelIterable.addIterable(Lists.newArrayList(1, 2, 3));

      // Signal that no more iterables will be added
      parallelIterable.finishAdding();

      // Collect all values
      Multiset<Integer> actualValues = HashMultiset.create();
      try (CloseableIterator<Integer> iterator = parallelIterable.iterator()) {
        int count = 0;
        while (iterator.hasNext() && count < 10) { // Prevent infinite loops in testing
          actualValues.add(iterator.next());
          count++;
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to close iterator", e);
      }

      Multiset<Integer> expectedValues = ImmutableMultiset.of(1, 2, 3);

      assertThat(actualValues)
          .as("All dynamically added iterables should be processed")
          .isEqualTo(expectedValues);
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testAddIterableAfterFinishAddingThrowsException() {
    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      ParallelIterable<Integer> parallelIterable =
          new ParallelIterable<>(Lists.newArrayList(), executor);

      parallelIterable.finishAdding();

      // This should throw an exception
      try {
        parallelIterable.addIterable(Lists.newArrayList(1, 2, 3));
        assertThat(false).as("Expected IllegalStateException").isTrue();
      } catch (IllegalStateException e) {
        assertThat(e.getMessage())
            .contains("No more iterables can be added after finishAdding() is called");
      }
    } finally {
      executor.shutdown();
    }
  }

  @Test
  public void testMixedStaticAndDynamicIterables() {
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      // Create with some initial iterables
      List<Iterable<Integer>> initialIterables =
          Lists.newArrayList(Lists.newArrayList(1, 2), Lists.newArrayList(3, 4));

      ParallelIterable<Integer> parallelIterable =
          new ParallelIterable<>(initialIterables, executor);

      // Add more iterables dynamically
      parallelIterable.addIterable(Lists.newArrayList(5, 6));
      parallelIterable.addIterable(Lists.newArrayList(7, 8));

      parallelIterable.finishAdding();

      // Collect all values
      Multiset<Integer> actualValues = HashMultiset.create();
      try (CloseableIterator<Integer> iterator = parallelIterable.iterator()) {
        while (iterator.hasNext()) {
          actualValues.add(iterator.next());
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to close iterator", e);
      }

      Multiset<Integer> expectedValues = ImmutableMultiset.of(1, 2, 3, 4, 5, 6, 7, 8);

      assertThat(actualValues)
          .as("Both initial and dynamically added iterables should be processed")
          .isEqualTo(expectedValues);
    } finally {
      executor.shutdown();
    }
  }

  private void queueHasElements(ParallelIterator<Integer> iterator) {
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();
    assertThat(iterator.queueSize()).as("queue size").isGreaterThan(0);
  }
}
