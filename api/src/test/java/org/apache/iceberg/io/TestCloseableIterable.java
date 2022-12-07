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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.io.TestableCloseableIterable.TestableCloseableIterator;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.metrics.DefaultMetricsContext;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestCloseableIterable {

  @Test
  public void testFilterManuallyClosable() throws IOException {
    TestableCloseableIterable iterable = new TestableCloseableIterable();
    TestableCloseableIterator iterator = (TestableCloseableIterator) iterable.iterator();

    CloseableIterable<Integer> filtered = CloseableIterable.filter(iterable, x -> x > 5);

    Assert.assertFalse("Iterable should not be closed", iterable.closed());
    Assert.assertFalse("Iterator should not be closed", iterator.closed());

    filtered.iterator().close();
    Assert.assertFalse("Iterable should not be closed", iterable.closed());
    Assert.assertTrue("Iterator should be closed", iterator.closed());

    filtered.close();
    Assert.assertTrue("Iterable should be closed", iterable.closed());
    Assert.assertTrue("Iterator should be closed", iterator.closed());
  }

  @Test
  public void testFilterAutomaticallyClosable() throws IOException {
    TestableCloseableIterable iterable = new TestableCloseableIterable();
    Assert.assertFalse("Iterable should not be closed", iterable.closed());
    try (CloseableIterable<Integer> filtered = CloseableIterable.filter(iterable, x -> x > 5)) {
      Assert.assertFalse("Iterable should not be closed", iterable.closed());
    }
    Assert.assertTrue("Iterable should be closed", iterable.closed());
  }

  @Test
  public void testConcatWithEmptyIterables() {
    CloseableIterable<Integer> iter =
        CloseableIterable.combine(Lists.newArrayList(1, 2, 3), () -> {});
    CloseableIterable<Integer> empty = CloseableIterable.empty();

    CloseableIterable<Integer> concat1 =
        CloseableIterable.concat(Lists.newArrayList(iter, empty, empty));
    Assert.assertEquals(Iterables.getLast(concat1).intValue(), 3);

    CloseableIterable<Integer> concat2 =
        CloseableIterable.concat(Lists.newArrayList(empty, empty, iter));
    Assert.assertEquals(Iterables.getLast(concat2).intValue(), 3);

    CloseableIterable<Integer> concat3 =
        CloseableIterable.concat(Lists.newArrayList(empty, iter, empty));
    Assert.assertEquals(Iterables.getLast(concat3).intValue(), 3);

    CloseableIterable<Integer> concat4 =
        CloseableIterable.concat(Lists.newArrayList(empty, iter, empty, empty, iter));
    Assert.assertEquals(Iterables.getLast(concat4).intValue(), 3);

    // This will throw a NoSuchElementException
    CloseableIterable<Integer> concat5 =
        CloseableIterable.concat(Lists.newArrayList(empty, empty, empty));
    AssertHelpers.assertThrows(
        "should throw NoSuchElementException",
        NoSuchElementException.class,
        () -> Iterables.getLast(concat5));
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
      iter.forEach(val -> Assertions.assertThat(completionCounter.get()).isEqualTo(0));
    }
    Assertions.assertThat(completionCounter.get()).isEqualTo(1);
  }

  @Test
  public void testWithCompletionRunnableAndEmptyIterable() throws IOException {
    AtomicInteger completionCounter = new AtomicInteger(0);
    CloseableIterable<Integer> empty = CloseableIterable.empty();
    try (CloseableIterable<Integer> iter =
        CloseableIterable.whenComplete(
            CloseableIterable.combine(empty, () -> {}), completionCounter::incrementAndGet)) {
      iter.forEach(val -> Assertions.assertThat(completionCounter.get()).isEqualTo(0));
    }
    Assertions.assertThat(completionCounter.get()).isEqualTo(1);
  }

  @Test
  public void testWithCompletionRunnableAndUnclosedIterable() {
    AtomicInteger completionCounter = new AtomicInteger(0);
    List<Integer> items = Lists.newArrayList(1, 2, 3, 4, 5);
    CloseableIterable<Integer> iter =
        CloseableIterable.whenComplete(
            CloseableIterable.combine(items, () -> {}), completionCounter::incrementAndGet);
    iter.forEach(val -> Assertions.assertThat(completionCounter.get()).isEqualTo(0));
    // given that we never close iter, the completionRunnable is never called
    Assertions.assertThat(completionCounter.get()).isEqualTo(0);
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
                iter.forEach(val -> Assertions.assertThat(completionCounter.get()).isEqualTo(0));
              }
            })
        .isInstanceOf(RuntimeException.class)
        .hasMessage("expected");

    Assertions.assertThat(completionCounter.get()).isEqualTo(1);
  }

  @Test
  public void testConcatWithEmpty() {
    AtomicInteger counter = new AtomicInteger(0);
    CloseableIterable.concat(Collections.emptyList()).forEach(c -> counter.incrementAndGet());
    Assertions.assertThat(counter.get()).isEqualTo(0);
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

    try (CloseableIterable<Integer> concat = CloseableIterable.concat(transform)) {
      concat.forEach(c -> c++);
    }
    Assertions.assertThat(counter.get()).isEqualTo(items.size());
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
    Assertions.assertThat(counter.value()).isEqualTo(0);
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
    Assertions.assertThat(counter.value()).isEqualTo(0);
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
