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
import java.util.function.Function;
import org.apache.iceberg.metrics.Counter;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public interface CloseableIterator<T> extends Iterator<T>, Closeable {

  static <E> CloseableIterator<E> empty() {
    return withClose(Collections.emptyIterator());
  }

  static <E> CloseableIterator<E> withClose(Iterator<E> iterator) {
    if (iterator instanceof CloseableIterator) {
      return (CloseableIterator<E>) iterator;
    }

    return new CloseableIterator<E>() {
      @Override
      public void close() throws IOException {
        if (iterator instanceof Closeable) {
          ((Closeable) iterator).close();
        }
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public E next() {
        return iterator.next();
      }
    };
  }

  static <I, O> CloseableIterator<O> transform(
      CloseableIterator<I> iterator, Function<I, O> transform) {
    Preconditions.checkNotNull(transform, "Invalid transform: null");

    return new CloseableIterator<O>() {
      @Override
      public void close() throws IOException {
        iterator.close();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public O next() {
        return transform.apply(iterator.next());
      }
    };
  }

  static <T> CloseableIterator<T> count(Counter counter, CloseableIterator<T> iterator) {
    return new CloseableIterator<T>() {
      @Override
      public void close() throws IOException {
        iterator.close();
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        T next = iterator.next();
        counter.increment();
        return next;
      }
    };
  }
}
