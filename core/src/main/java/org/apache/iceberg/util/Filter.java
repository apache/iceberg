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

import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FilterIterator;

/**
 * A Class for generic filters
 *
 * @param <T> the type of objects filtered by this Filter
 */
public abstract class Filter<T> {

  protected abstract boolean shouldKeep(T item);

  public Iterable<T> filter(Iterable<T> items) {
    return () -> new Iterator(items.iterator());
  }

  public CloseableIterable<T> filter(CloseableIterable<T> items) {
    return CloseableIterable.combine(filter((Iterable<T>) items), items);
  }

  private class Iterator extends FilterIterator<T> {
    protected Iterator(java.util.Iterator<T> items) {
      super(items);
    }

    @Override
    protected boolean shouldKeep(T item) {
      return Filter.this.shouldKeep(item);
    }
  }
}
