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

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import com.google.common.collect.Maps;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A thread-safe set implementation for Iceberg {@link org.apache.iceberg.ContentFile} objects.
 *
 * <p>This class provides a concurrent alternative to {@link DataFileSet} and {@link DeleteFileSet}
 * for scenarios where files must be collected from multiple worker threads. The standard {@link
 * WrapperSet} implementations use {@link java.util.LinkedHashSet} internally, which is
 * <strong>not</strong> thread-safe and can corrupt when mutated concurrently (see Iceberg issue
 * #16978).
 *
 * <p>Unlike {@link WrapperSet}, this implementation does <strong>not</strong> preserve insertion
 * order. If order is required, collect files in a thread-safe structure and copy to a {@link
 * DataFileSet} or {@link DeleteFileSet} on the main thread after parallel work completes.
 *
 * @param <F> the file type, typically {@link org.apache.iceberg.DataFile} or {@link
 *     org.apache.iceberg.DeleteFile}
 */
public class ThreadSafeFileSet<F> implements Set<F> {
  private final Set<F> delegate;

  @SuppressWarnings("unchecked")
  public static <F> ThreadSafeFileSet<F> create() {
    return new ThreadSafeFileSet<>();
  }

  @SuppressWarnings("unchecked")
  public static <F> ThreadSafeFileSet<F> create(Iterable<F> files) {
    ThreadSafeFileSet<F> set = new ThreadSafeFileSet<>();
    files.forEach(set::add);
    return set;
  }

  private ThreadSafeFileSet() {
    this.delegate = Collections.newSetFromMap(Maps.newConcurrentMap());
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public boolean isEmpty() {
    return delegate.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return delegate.contains(o);
  }

  @Override
  public Iterator<F> iterator() {
    return delegate.iterator();
  }

  @Override
  public Object[] toArray() {
    return delegate.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return delegate.toArray(a);
  }

  @Override
  public boolean add(F f) {
    return delegate.add(f);
  }

  @Override
  public boolean remove(Object o) {
    return delegate.remove(o);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return delegate.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends F> c) {
    return delegate.addAll(c);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    return delegate.retainAll(c);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    return delegate.removeAll(c);
  }

  @Override
  public void clear() {
    delegate.clear();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ThreadSafeFileSet<?> that = (ThreadSafeFileSet<?>) o;
    return delegate.equals(that.delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public String toString() {
    return delegate.toString();
  }
}
