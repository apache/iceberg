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
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.iceberg.io.CloseableIterator;

/**
 * An Iterator that filters another Iterator.
 *
 * @param <T> the type of objects produced by this Iterator
 */
public abstract class FilterIterator<T> implements CloseableIterator<T> {
  private final Iterator<T> items;
  private boolean closed;
  private boolean hasNext;
  private T next;

  protected FilterIterator(Iterator<T> items) {
    this.items = items;
    this.closed = false;
    this.next = null;
    this.hasNext = false;
  }

  protected abstract boolean shouldKeep(T item);

  @Override
  public boolean hasNext() {
    return hasNext || advance();
  }

  @Override
  public T next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    T returnVal = next;

    advance();

    return returnVal;
  }

  private boolean advance() {
    while (!closed && items.hasNext()) {
      this.next = items.next();
      if (shouldKeep(next)) {
        this.hasNext = true;
        return true;
      }
    }

    close();

    this.hasNext = false;
    return false;
  }

  @Override
  public void close() {
    if (!closed) {
      try {
        ((Closeable) items).close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      this.closed = true;
    }
  }
}
