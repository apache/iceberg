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
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

/**
 * Warning: This class is not thread safe
 * @param <T> object type of the iterable
 * @param <O> object type of the key for deduping
 */
public class DedupingIterator<T, O> implements CloseableIterator<T> {
  private final CloseableIterator<T> iterator;
  private final Function<T, O> getKey;
  private final Set<O> seen;
  private T buffer;
  private long start;
  public DedupingIterator(CloseableIterator<T> iterator, Function<T, O> getKey) {
    this.iterator = iterator;
    this.getKey = getKey;
    this.seen = new HashSet<>();
    buffer = null;
  }

  @Override
  public void close() throws IOException {
    iterator.close();
  }

  @Override
  public boolean hasNext() {
    return buffer != null || iterator.hasNext();
  }

  private void pump() {
    while (buffer == null && iterator.hasNext()) {
      T next = iterator.next();
      O key = getKey.apply(next);
      if (!seen.contains(key)) {
        seen.add(key);
        buffer = next;
      }
    }
  }

  @Override
  public T next() {
    T output;
    if (buffer == null) {
      T next = iterator.next();
      O key = getKey.apply(next);
      seen.add(key);
      output = next;
      start = System.currentTimeMillis();
    } else {
      output = buffer;
      buffer = null;
    }
    pump();
    return output;
  }
}
