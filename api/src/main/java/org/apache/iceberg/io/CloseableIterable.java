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
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {

  /**
   * Returns an closeable iterator over elements of type {@code T}.
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
      public void close() {
      }

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

  static <E> CloseableIterable<E> filter(CloseableIterable<E> iterable, Predicate<E> pred) {
    return combine(() -> new FilterIterator<E>(iterable.iterator()) {
      @Override
      protected boolean shouldKeep(E item) {
        return pred.test(item);
      }
    }, iterable);
  }

  static <I, O> CloseableIterable<O> transform(CloseableIterable<I> iterable, Function<I, O> transform) {
    Preconditions.checkNotNull(transform, "Cannot apply a null transform");

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
    Iterator<CloseableIterable<E>> iterables = iterable.iterator();
    if (!iterables.hasNext()) {
      return empty();
    } else {
      return new ConcatCloseableIterable<>(iterable);
    }
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
        this.currentIterable = iterables.next();
        this.currentIterator = currentIterable.iterator();
      }

      @Override
      public boolean hasNext() {
        if (closed) {
          return false;
        }

        if (currentIterator.hasNext()) {
          return true;
        }

        while (iterables.hasNext()) {
          try {
            currentIterable.close();
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
          currentIterable.close();
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
          currentIterable.close();
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
