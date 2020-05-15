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
}
