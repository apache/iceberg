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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.stream.Collectors;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;

/**
 * An Iterable that merges the items from other Iterables in order.
 *
 * <p>This assumes that the Iterables passed in produce items in sorted order.
 *
 * @param <T> the type of objects produced by this Iterable
 */
public class SortedMerge<T> extends CloseableGroup implements CloseableIterable<T> {
  private final Comparator<T> comparator;
  private final List<CloseableIterable<T>> iterables;

  public SortedMerge(Comparator<T> comparator, List<CloseableIterable<T>> iterables) {
    this.comparator = comparator;
    this.iterables = iterables;
  }

  @Override
  public CloseableIterator<T> iterator() {
    List<CloseableIterator<T>> iterators =
        iterables.stream()
            .map(CloseableIterable::iterator)
            .filter(Iterator::hasNext)
            .collect(Collectors.toList());

    if (iterators.size() == 1) {
      addCloseable(iterators.get(0));
      return iterators.get(0);
    } else {
      CloseableIterator<T> merge = new MergeIterator(iterators);
      addCloseable(merge);
      return merge;
    }
  }

  /**
   * An Iterator that merges the items from other Iterators in order.
   *
   * <p>This assumes that the Iterators passed in produce items in sorted order.
   */
  private class MergeIterator implements CloseableIterator<T> {
    private final PriorityQueue<Pair<T, Iterator<T>>> heap;

    private MergeIterator(Iterable<CloseableIterator<T>> iterators) {
      this.heap = new PriorityQueue<>(Comparator.comparing(Pair::first, comparator));
      iterators.forEach(this::addNext);
    }

    @Override
    public boolean hasNext() {
      return !heap.isEmpty();
    }

    @Override
    public T next() {
      if (heap.isEmpty()) {
        throw new NoSuchElementException();
      }

      Pair<T, Iterator<T>> pair = heap.poll();

      addNext(pair.second());

      return pair.first();
    }

    private void addNext(Iterator<T> iter) {
      if (iter.hasNext()) {
        heap.add(Pair.of(iter.next(), iter));
      } else {
        close(iter);
      }
    }

    @Override
    public void close() throws IOException {
      while (!heap.isEmpty()) {
        Pair<T, Iterator<T>> pair = heap.poll();
        close(pair.second());
      }
    }

    private void close(Iterator<?> iter) {
      if (iter instanceof Closeable) {
        try {
          ((Closeable) iter).close();
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }
}
