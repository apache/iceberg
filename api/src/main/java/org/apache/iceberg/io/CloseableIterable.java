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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {

  /**
   * Returns a closeable iterator over elements of type {@code T}.
   *
   * @return an {@link CloseableIterator}.
   */
  @Override
  CloseableIterator<T> iterator();

  static <E> CloseableIterable<E> withNoopClose(E entry) {
    return withNoopClose(ImmutableList.of(entry));
  }

  static <E> CloseableIterable<E> withNoopClose(Iterable<E> iterable) {
    return new CloseableIterable<E>() {
      @Override
      public void close() {}

      @Override
      public CloseableIterator<E> iterator() {
        return CloseableIterator.withClose(iterable.iterator());
      }
    };
  }

  static <E> CloseableIterable<E> empty() {
    return withNoopClose(Collections.emptyList());
  }

  static <E> CloseableIterable<E> combine(Iterable<E> iterable, Closeable closeable) {
    return new CloseableIterable<E>() {
      @Override
      public void close() throws IOException {
        closeable.close();
      }

      @Override
      public CloseableIterator<E> iterator() {
        return CloseableIterator.withClose(iterable.iterator());
      }
    };
  }

  /**
   * Will run the given runnable when {@link CloseableIterable#close()} has been called.
   *
   * @param iterable The underlying {@link CloseableIterable} to iterate over
   * @param onCompletionRunnable The runnable to run after the underlying iterable was closed
   * @param <E> The type of the underlying iterable
   * @return A new {@link CloseableIterable} where the runnable will be executed as the final step
   *     after {@link CloseableIterable#close()} has been called
   */
  static <E> CloseableIterable<E> whenComplete(
      CloseableIterable<E> iterable, Runnable onCompletionRunnable) {
    Preconditions.checkNotNull(onCompletionRunnable, "Invalid runnable: null");
    return new CloseableIterable<E>() {
      @Override
      public void close() throws IOException {
        try {
          iterable.close();
        } finally {
          onCompletionRunnable.run();
        }
      }

      @Override
      public CloseableIterator<E> iterator() {
        return iterable.iterator();
      }
    };
  }

  static <E> CloseableIterable<E> filter(CloseableIterable<E> iterable, Predicate<E> pred) {
    return combine(
        () ->
            new FilterIterator<E>(iterable.iterator()) {
              @Override
              protected boolean shouldKeep(E item) {
                return pred.test(item);
              }
            },
        iterable);
  }

  /**
   * Filters the given {@link CloseableIterable} and counts the number of elements that do not match
   * the predicate by incrementing the {@link Counter}.
   *
   * @param skipCounter The {@link Counter} instance to increment on each skipped item during
   *     filtering.
   * @param iterable The underlying {@link CloseableIterable} to filter.
   * @param <E> The underlying type to be iterated.
   * @return A filtered {@link CloseableIterable} where the given skipCounter is incremented
   *     whenever the predicate does not match.
   */
  static <E> CloseableIterable<E> filter(
      Counter skipCounter, CloseableIterable<E> iterable, Predicate<E> pred) {
    Preconditions.checkArgument(null != skipCounter, "Invalid counter: null");
    Preconditions.checkArgument(null != iterable, "Invalid iterable: null");
    Preconditions.checkArgument(null != pred, "Invalid predicate: null");
    return combine(
        () ->
            new FilterIterator<E>(iterable.iterator()) {
              @Override
              protected boolean shouldKeep(E item) {
                boolean matches = pred.test(item);
                if (!matches) {
                  skipCounter.increment();
                }
                return matches;
              }
            },
        iterable);
  }

  /**
   * Counts the number of elements in the given {@link CloseableIterable} by incrementing the {@link
   * Counter} instance for each {@link Iterator#next()} call.
   *
   * @param counter The {@link Counter} instance to increment on each {@link Iterator#next()} call.
   * @param iterable The underlying {@link CloseableIterable} to count
   * @param <T> The underlying type to be iterated.
   * @return A {@link CloseableIterable} that increments the given counter on each {@link
   *     Iterator#next()} call.
   */
  static <T> CloseableIterable<T> count(Counter counter, CloseableIterable<T> iterable) {
    Preconditions.checkArgument(null != counter, "Invalid counter: null");
    Preconditions.checkArgument(null != iterable, "Invalid iterable: null");
    return new CloseableIterable<T>() {
      @Override
      public CloseableIterator<T> iterator() {
        return CloseableIterator.count(counter, iterable.iterator());
      }

      @Override
      public void close() throws IOException {
        iterable.close();
      }
    };
  }

  static <I, O> CloseableIterable<O> transform(
      CloseableIterable<I> iterable, Function<I, O> transform) {
    Preconditions.checkNotNull(transform, "Invalid transform: null");

    return new CloseableIterable<O>() {
      @Override
      public void close() throws IOException {
        iterable.close();
      }

      @Override
      public CloseableIterator<O> iterator() {
        return new CloseableIterator<O>() {
          private final CloseableIterator<I> inner = iterable.iterator();

          @Override
          public void close() throws IOException {
            inner.close();
          }

          @Override
          public boolean hasNext() {
            return inner.hasNext();
          }

          @Override
          public O next() {
            return transform.apply(inner.next());
          }
        };
      }
    };
  }

  static <E> CloseableIterable<E> concat(Iterable<CloseableIterable<E>> iterable) {
    return new ConcatCloseableIterable<>(iterable);
  }

  class ConcatCloseableIterable<E> extends CloseableGroup implements CloseableIterable<E> {
    private final Iterable<CloseableIterable<E>> inputs;

    ConcatCloseableIterable(Iterable<CloseableIterable<E>> inputs) {
      this.inputs = inputs;
    }

    @Override
    public CloseableIterator<E> iterator() {
      ConcatCloseableIterator<E> iter = new ConcatCloseableIterator<>(inputs);
      addCloseable(iter);
      return iter;
    }

    private static class ConcatCloseableIterator<E> implements CloseableIterator<E> {
      private final Iterator<CloseableIterable<E>> iterables;
      private CloseableIterable<E> currentIterable = null;
      private Iterator<E> currentIterator = null;
      private boolean closed = false;

      private ConcatCloseableIterator(Iterable<CloseableIterable<E>> inputs) {
        this.iterables = inputs.iterator();
      }

      @Override
      public boolean hasNext() {
        if (closed) {
          return false;
        }

        if (null != currentIterator && currentIterator.hasNext()) {
          return true;
        }

        while (iterables.hasNext()) {
          try {
            if (null != currentIterable) {
              currentIterable.close();
            }
          } catch (IOException e) {
            throw new RuntimeIOException(e, "Failed to close iterable");
          }

          this.currentIterable = iterables.next();
          this.currentIterator = currentIterable.iterator();

          if (currentIterator.hasNext()) {
            return true;
          }
        }

        try {
          if (null != currentIterable) {
            currentIterable.close();
          }
        } catch (IOException e) {
          throw new RuntimeIOException(e, "Failed to close iterable");
        }

        this.closed = true;
        this.currentIterator = null;
        this.currentIterable = null;

        return false;
      }

      @Override
      public void close() throws IOException {
        if (!closed) {
          if (null != currentIterable) {
            currentIterable.close();
          }
          this.closed = true;
          this.currentIterator = null;
          this.currentIterable = null;
        }
      }

      @Override
      public E next() {
        if (hasNext()) {
          return currentIterator.next();
        } else {
          throw new NoSuchElementException();
        }
      }
    }
  }
}
