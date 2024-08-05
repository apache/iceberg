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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

public class TestParallelIterable {
  @Test
  public void closeParallelIteratorWithoutCompleteIteration()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    ExecutorService executor = Executors.newFixedThreadPool(1);

    Iterable<CloseableIterable<Integer>> transform =
        Iterables.transform(
            Lists.newArrayList(1, 2, 3, 4, 5),
            item ->
                new CloseableIterable<Integer>() {
                  @Override
                  public void close() {}

                  @Override
                  public CloseableIterator<Integer> iterator() {
                    return CloseableIterator.withClose(Collections.singletonList(item).iterator());
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
  }

  @Test
  public void closeMoreDataParallelIteratorWithoutCompleteIteration()
      throws IOException, IllegalAccessException, NoSuchFieldException {
    ExecutorService executor = Executors.newFixedThreadPool(1);
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
  }

  @Test
  public void limitQueueSize() throws IOException, IllegalAccessException, NoSuchFieldException {

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
    ExecutorService executor = Executors.newCachedThreadPool();
    ParallelIterable<Integer> parallelIterable =
        new ParallelIterable<>(iterables, executor, maxQueueSize);
    ParallelIterator<Integer> iterator = (ParallelIterator<Integer>) parallelIterable.iterator();

    Multiset<Integer> actualValues = HashMultiset.create();

    while (iterator.hasNext()) {
      assertThat(iterator.queueSize())
          .as("iterator internal queue size")
          .isLessThanOrEqualTo(maxQueueSize + iterables.size());
      actualValues.add(iterator.next());
    }

    assertThat(actualValues)
        .as("multiset of values returned by the iterator")
        .isEqualTo(expectedValues);

    iterator.close();
    executor.shutdownNow();
  }

  private void queueHasElements(ParallelIterator<Integer> iterator) {
    assertThat(iterator.hasNext()).isTrue();
    assertThat(iterator.next()).isNotNull();
    assertThat(iterator.queueSize()).as("queue size").isGreaterThan(0);
  }
}
