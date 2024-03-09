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
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.iceberg.io.TestableCloseableIterable.TestableCloseableIterator;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestCloseableIterable {

  @Test
  public void testFilterManuallyClosable() throws IOException {
    TestableCloseableIterable iterable = new TestableCloseableIterable();
    TestableCloseableIterator iterator = (TestableCloseableIterator) iterable.iterator();

    CloseableIterable<Integer> filtered = CloseableIterable.filter(iterable, x -> x > 5);

    assertThat(iterable.closed()).isFalse();
    assertThat(iterator.closed()).isFalse();

    filtered.iterator().close();
    assertThat(iterable.closed()).isFalse();
    assertThat(iterator.closed()).isTrue();

    filtered.close();
    assertThat(iterable.closed()).isTrue();
    assertThat(iterator.closed()).isTrue();
  }

  @Test
  public void testFilterAutomaticallyClosable() throws IOException {
    TestableCloseableIterable iterable = new TestableCloseableIterable();
    assertThat(iterable.closed()).isFalse();
    try (CloseableIterable<Integer> filtered = CloseableIterable.filter(iterable, x -> x > 5)) {
      assertThat(iterable.closed()).isFalse();
    }
    assertThat(iterable.closed()).isTrue();
  }

  @Test
  public void testConcatWithEmptyIterables() {
    CloseableIterable<Integer> iter =
        CloseableIterable.combine(Lists.newArrayList(1, 2, 3), () -> {});
    CloseableIterable<Integer> empty = CloseableIterable.empty();

    CloseableIterable<Integer> concat1 =
        CloseableIterable.concat(Lists.newArrayList(iter, empty, empty));
    assertThat(Iterables.getLast(concat1).intValue()).isEqualTo(3);

    CloseableIterable<Integer> concat2 =
        CloseableIterable.concat(Lists.newArrayList(empty, empty, iter));
    assertThat(Iterables.getLast(concat2).intValue()).isEqualTo(3);

    CloseableIterable<Integer> concat3 =
        CloseableIterable.concat(Lists.newArrayList(empty, iter, empty));
    assertThat(Iterables.getLast(concat3).intValue()).isEqualTo(3);

    CloseableIterable<Integer> concat4 =
        CloseableIterable.concat(Lists.newArrayList(empty, iter, empty, empty, iter));
    assertThat(Iterables.getLast(concat4).intValue()).isEqualTo(3);

    // This will throw a NoSuchElementException
    CloseableIterable<Integer> concat5 =
        CloseableIterable.concat(Lists.newArrayList(empty, empty, empty));

    Assertions.assertThatThrownBy(() -> Iterables.getLast(concat5))
        .isInstanceOf(NoSuchElementException.class);
  }

  @Test
  public void testWithCompletionRunnable() throws IOException {
    AtomicInteger completionCounter = new AtomicInteger(0);
    List<Integer> items = Lists.newArrayList(1, 2, 3, 4, 5);
    Assertions.assertThatThrownBy(
            () -> CloseableIterable.whenComplete(CloseableIterable.combine(items, () -> {}), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid runnable: null");

    try (CloseableIterable<Integer> iter =
        CloseableIterable.whenComplete(
            CloseableIterable.combine(items, () -> {}), completionCounter::incrementAndGet)) {
      iter.forEach(val -> assertThat(completionCounter.get()).isZero());
    }
    assertThat(completionCounter.get()).isOne();
  }

  @Test
  public void testWithCompletionRunnableAndEmptyIterable() throws IOException {
    AtomicInteger completionCounter = new AtomicInteger(0);
    CloseableIterable<Integer> empty = CloseableIterable.empty();
    try (CloseableIterable<Integer> iter =
        CloseableIterable.whenComplete(
            CloseableIterable.combine(empty, () -> {}), completionCounter::incrementAndGet)) {
      iter.forEach(val -> Assertions.assertThat(completionCounter.get()).isZero());
    }
    Assertions.assertThat(completionCounter.get()).isOne();
  }

  @Test
  public void testWithCompletionRunnableAndUnclosedIterable() {
    AtomicInteger completionCounter = new AtomicInteger(0);
    List<Integer> items = Lists.newArrayList(1, 2, 3, 4, 5);
    CloseableIterable<Integer> iter =
        CloseableIterable.whenComplete(
            CloseableIterable.combine(items, () -> {}), completionCounter::incrementAndGet);
    iter.forEach(val -> assertThat(completionCounter.get()).isZero());
    // given that we never close iter, the completionRunnable is never called
    assertThat(completionCounter.get()).isZero();
  }

  @Test
  public void testWithCompletionRunnableWhenIterableThrows() {
    AtomicInteger completionCounter = new AtomicInteger(0);
    List<Integer> items = Lists.newArrayList(1, 2, 3, 4, 5);

    Assertions.assertThatThrownBy(
            () -> {
              try (CloseableIterable<Integer> iter =
                  CloseableIterable.whenComplete(
                      CloseableIterable.combine(
                          items,
                          () -> {
                            throw new RuntimeException("expected");
                          }),
                      completionCounter::incrementAndGet)) {
                iter.forEach(val -> assertThat(completionCounter.get()).isZero());
              }
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessage("expected");

    Assertions.assertThat(completionCounter.get()).isOne();
  }

  @Test
  public void testConcatWithEmpty() {
    AtomicInteger counter = new AtomicInteger(0);
    CloseableIterable.concat(Collections.emptyList()).forEach(c -> counter.incrementAndGet());
    assertThat(counter.get()).isZero();
  }

  @Test
  public void concatShouldOnlyEvaluateItemsOnce() throws IOException {
    AtomicInteger counter = new AtomicInteger(0);
    List<Integer> items = Lists.newArrayList(1, 2, 3, 4, 5);
    Iterable<Integer> iterable =
        Iterables.filter(
            items,
            item -> {
              counter.incrementAndGet();
              return true;
            });

    Iterable<CloseableIterable<Integer>> transform =
        Iterables.transform(
            iterable,
            item ->
                new CloseableIterable<Integer>() {
                  @Override
                  public void close() {}

                  @Override
                  public CloseableIterator<Integer> iterator() {
                    return CloseableIterator.withClose(Collections.singletonList(item).iterator());
                  }
                });

    AtomicInteger consumedCounter = new AtomicInteger(0);
    try (CloseableIterable<Integer> concat = CloseableIterable.concat(transform)) {
      concat.forEach(count -> consumedCounter.getAndIncrement());
    }
    assertThat(counter.get()).isEqualTo(items.size()).isEqualTo(consumedCounter.get());
  }

  @Test
  public void concatIterablesWithIterator() throws IOException {
    CloseableIterable<Object> closeableIterable = CloseableIterable.concat(Collections.emptyList());
    closeableIterable.iterator();
    // shouldn't throw a NPE
    closeableIterable.close();
  }

  @Test
  public void count() {
    Counter counter = new DefaultMetricsContext().counter("x");
    CloseableIterable<Integer> items =
        CloseableIterable.count(
            counter, CloseableIterable.withNoopClose(Arrays.asList(1, 2, 3, 4, 5)));
    assertThat(counter.value()).isZero();
    items.forEach(item -> {});
    Assertions.assertThat(counter.value()).isEqualTo(5);
  }

  @Test
  public void countSkipped() {
    Counter counter = new DefaultMetricsContext().counter("x");
    CloseableIterable<Integer> items =
        CloseableIterable.filter(
            counter,
            CloseableIterable.withNoopClose(Arrays.asList(1, 2, 3, 4, 5)),
            x -> x % 2 == 0);
    assertThat(counter.value()).isZero();
    items.forEach(item -> {});
    Assertions.assertThat(counter.value()).isEqualTo(3);
  }

  @Test
  public void countNullCheck() {
    Assertions.assertThatThrownBy(() -> CloseableIterable.count(null, CloseableIterable.empty()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid counter: null");

    Counter counter = new DefaultMetricsContext().counter("x");
    Assertions.assertThatThrownBy(() -> CloseableIterable.count(counter, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid iterable: null");
  }

  @Test
  public void countSkippedNullCheck() {
    Assertions.assertThatThrownBy(
            () ->
                CloseableIterable.filter(null, CloseableIterable.empty(), Predicate.isEqual(true)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid counter: null");

    Counter counter = new DefaultMetricsContext().counter("x");
    Assertions.assertThatThrownBy(
            () -> CloseableIterable.filter(counter, null, Predicate.isEqual(true)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid iterable: null");

    Assertions.assertThatThrownBy(
            () -> CloseableIterable.filter(counter, CloseableIterable.empty(), null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid predicate: null");
  }

  @Test
  public void transformNullCheck() {
    Assertions.assertThatThrownBy(
            () -> CloseableIterable.transform(CloseableIterable.empty(), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("Invalid transform: null");
  }
}
