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
import java.io.UncheckedIOException;
import java.util.Iterator;

public class ClosingIterator<T> implements Iterator<T> {
  private final CloseableIterator<T> iterator;
  private boolean shouldClose = false;

  public ClosingIterator(CloseableIterator<T> iterator) {
    this.iterator = iterator;

  }

  @Override
  public boolean hasNext() {
    boolean hasNext = iterator.hasNext();
    this.shouldClose = !hasNext;
    return hasNext;
  }

  @Override
  public T next() {
    T next = iterator.next();

    if (shouldClose) {
      // this will only be called once because iterator.next would throw NoSuchElementException
      try {
        iterator.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return next;
  }
}
