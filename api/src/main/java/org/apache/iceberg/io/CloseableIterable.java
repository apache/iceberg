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

import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

public interface CloseableIterable<T> extends Iterable<T>, Closeable {
  static <E> CloseableIterable<E> withNoopClose(Iterable<E> iterable) {
    return new CloseableIterable<E>() {
      @Override
      public void close() {
      }

      @Override
      public Iterator<E> iterator() {
        return iterable.iterator();
      }
    };
  }

  static <E> CloseableIterable<E> empty() {
    return new CloseableIterable<E>() {
      @Override
      public void close() {
      }

      @Override
      public Iterator<E> iterator() {
        return Collections.emptyIterator();
      }
    };
  }

  static <E> CloseableIterable<E> combine(Iterable<E> iterable, Iterable<Closeable> closeables) {
    return new CloseableGroup.ClosingIterable<>(iterable, closeables);
  }

  static <I, O> CloseableIterable<O> wrap(CloseableIterable<I> iterable, Function<Iterable<I>, Iterable<O>> wrap) {
    Iterable<O> wrappedIterable = wrap.apply(iterable);

    return new CloseableIterable<O>() {
      @Override
      public void close() throws IOException {
        iterable.close();
      }

      @Override
      public Iterator<O> iterator() {
        return wrappedIterable.iterator();
      }
    };
  }

  static <I, O> CloseableIterable<O> transform(CloseableIterable<I> iterable, Function<I, O> transform) {
    Preconditions.checkNotNull(transform, "Cannot apply a null transform");

    return new CloseableIterable<O>() {
      @Override
      public void close() throws IOException {
        iterable.close();
      }

      @Override
      public Iterator<O> iterator() {
        return new Iterator<O>() {
          private final Iterator<I> inner = iterable.iterator();

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
}
